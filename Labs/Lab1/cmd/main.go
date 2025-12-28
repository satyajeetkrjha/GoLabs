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
	defer func() { fmt.Println("total_runtime:", time.Since(startAll)) }()

	const (
		numJobs    = 500000
		queueSize  = 5000
		numWorkers = 1

		// Phase-3B: If a job waited longer than this, it's "too stale" to process.
		maxJobAge = 2 * time.Second
	)

	jobs := make(chan model.Job, queueSize)
	results := make(chan model.Result, queueSize)

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	var enqueued uint64
	var droppedAtEnqueue uint64
	var droppedStale uint64
	var processed uint64

	// Workers (drop stale jobs BEFORE processing)
	for w := 1; w <= numWorkers; w++ {
		workerID := w
		go func(id int) {
			defer wg.Done()
			for job := range jobs {
				age := time.Since(job.Created)
				if age > maxJobAge {
					ds := atomic.AddUint64(&droppedStale, 1)
					if ds%500 == 0 {
						fmt.Printf("[worker] dropped_stale=%d last_age=%s queue_len=%d\n", ds, age, len(jobs))
					}
					continue
				}

				res := worker.Process(ctx, id, job)
				results <- res
				atomic.AddUint64(&processed, 1)
			}
		}(workerID)
	}

	// Producer (still drop-on-full to avoid blocking)
	go func() {
		for i := 1; i <= numJobs; i++ {
			job := model.Job{
				ID:      i,
				Payload: i * 10,
				Created: time.Now(),
			}

			select {
			case jobs <- job:
				atomic.AddUint64(&enqueued, 1)
			default:
				atomic.AddUint64(&droppedAtEnqueue, 1)
			}

			if i%1000 == 0 {
				fmt.Printf(
					"[producer] job=%d queue_len=%d enqueued=%d dropped_enqueue=%d dropped_stale=%d processed=%d\n",
					i,
					len(jobs),
					atomic.LoadUint64(&enqueued),
					atomic.LoadUint64(&droppedAtEnqueue),
					atomic.LoadUint64(&droppedStale),
					atomic.LoadUint64(&processed),
				)
			}
		}
		close(jobs)
	}()

	// Close results
	go func() {
		wg.Wait()
		close(results)
	}()

	// Consumer (prints e2e for processed jobs)
	count := 0
	for r := range results {
		count++
		e2e := time.Since(r.JobCreated)

		if count%1000 == 0 {
			fmt.Printf("[result] job=%d worker=%d worker_latency=%s e2e=%s\n",
				r.JobID, r.WorkerID, r.Latency, e2e)
		}
	}

	fmt.Printf(
		"done results=%d enqueued=%d dropped_enqueue=%d dropped_stale=%d processed=%d\n",
		count,
		atomic.LoadUint64(&enqueued),
		atomic.LoadUint64(&droppedAtEnqueue),
		atomic.LoadUint64(&droppedStale),
		atomic.LoadUint64(&processed),
	)
}
