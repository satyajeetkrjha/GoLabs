// cmd/main.go
package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"worker-pool-lab/internal/model"
	"worker-pool-lab/internal/worker"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	ctx := context.Background()

	startAll := time.Now()
	defer func() {
		fmt.Println("total_runtime:", time.Since(startAll))
	}()

	const (
		numJobs    = 500000
		queueSize  = 5000
		numWorkers = 1
	)

	// Phase 3C: bounded waiting budget for enqueue
	const enqueueWaitBudget = 2 * time.Millisecond

	// Phase 3B/3C + TTL stale drop in worker
	const staleTTL = 2 * time.Second

	// Option B knob:
	// - true  => on full queue, wait up to enqueueWaitBudget to enqueue
	// - false => on full queue, drop immediately (no waiting)
	const allowWaitOnFull = true

	jobs := make(chan model.Job, queueSize)
	results := make(chan model.Result, queueSize)

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	var enqueued uint64
	var droppedEnqueue uint64 // immediate drop (full + no wait)
	var droppedTimeout uint64 // waited up to budget, still couldn't enqueue
	var droppedStale uint64
	var processed uint64

	// Workers
	for w := 1; w <= numWorkers; w++ {
		workerID := w
		go func(id int) {
			defer wg.Done()

			for job := range jobs {
				age := time.Since(job.Created)
				if age > staleTTL {
					n := atomic.AddUint64(&droppedStale, 1)
					if n%500 == 0 {
						fmt.Printf("[worker] dropped_stale=%d last_age=%s queue_len=%d\n",
							n, age, len(jobs))
					}
					continue
				}

				res := worker.Process(ctx, id, job)
				atomic.AddUint64(&processed, 1)

				// This may block if consumer lags and results fills up.
				results <- res
			}
		}(workerID)
	}

	// Producer (Phase 3C + Option B immediate drop tier)
	go func() {
		for i := 1; i <= numJobs; i++ {
			job := model.Job{
				ID:      i,
				Payload: i * 10,
				Created: time.Now(),
			}

			// Cheap path: try immediate non-blocking enqueue
			select {
			case jobs <- job:
				atomic.AddUint64(&enqueued, 1)

			default:
				// Queue is full.
				if !allowWaitOnFull {
					// Immediate drop tier (true "droppedEnqueue")
					atomic.AddUint64(&droppedEnqueue, 1)
					continue
				}

				// Bounded-wait tier
				t := time.NewTimer(enqueueWaitBudget)
				select {
				case jobs <- job:
					// Cleanup timer correctly to avoid leaks/races
					if !t.Stop() {
						<-t.C
					}
					atomic.AddUint64(&enqueued, 1)

				case <-t.C:
					atomic.AddUint64(&droppedTimeout, 1)
				}
			}

			if i%1000 == 0 {
				fmt.Printf("[producer] job=%d queue_len=%d enqueued=%d dropped_enqueue=%d dropped_timeout=%d dropped_stale=%d processed=%d\n",
					i,
					len(jobs),
					atomic.LoadUint64(&enqueued),
					atomic.LoadUint64(&droppedEnqueue),
					atomic.LoadUint64(&droppedTimeout),
					atomic.LoadUint64(&droppedStale),
					atomic.LoadUint64(&processed),
				)
			}
		}

		close(jobs)
	}()

	// Close results after workers finish
	go func() {
		wg.Wait()
		close(results)
	}()

	// Consumer
	count := 0
	for r := range results {
		count++
		_ = r // keep it if you want to print e2e every N like earlier
	}

	fmt.Printf("done results=%d enqueued=%d dropped_enqueue=%d dropped_timeout=%d dropped_stale=%d processed=%d\n",
		count,
		atomic.LoadUint64(&enqueued),
		atomic.LoadUint64(&droppedEnqueue),
		atomic.LoadUint64(&droppedTimeout),
		atomic.LoadUint64(&droppedStale),
		atomic.LoadUint64(&processed),
	)
}
