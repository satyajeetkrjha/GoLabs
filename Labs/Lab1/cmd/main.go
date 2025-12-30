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
		numJobs = 500000

		queueSizeIngest  = 5000
		queueSizeProcess = 100

		// Stage A: fast workers (like cheap processors)
		numStageAWorkers = 8

		// Stage B: slow workers (like exporters)
		numStageBWorkers = 8
	)

	// Producer enqueue policy (into ingestQ)
	const enqueueWaitBudget = 2 * time.Millisecond
	const allowWaitOnFull = true // if false => immediate drop when ingestQ full

	// Stale TTL check in Stage B (slow stage)
	const staleTTL = 2 * time.Second

	ingestQ := make(chan model.Job, queueSizeIngest)
	processQ := make(chan model.Job, queueSizeProcess)
	results := make(chan model.Result, queueSizeProcess)

	// Counters (separate by stage so you can SEE pressure move)
	var produced uint64

	// Producer -> ingestQ
	var enqIngest uint64
	var dropIngestFull uint64
	var dropIngestTimeout uint64

	// Stage A -> processQ
	var stageARead uint64
	var enqProcess uint64
	var dropProcessFull uint64 // Stage A drops when processQ is full (immediate)

	// Stage B
	var droppedStale uint64
	var processed uint64

	// ---- Stage B workers (slow) ----
	var wgB sync.WaitGroup
	wgB.Add(numStageBWorkers)

	for w := 1; w <= numStageBWorkers; w++ {
		workerID := w
		go func(id int) {
			defer wgB.Done()

			for job := range processQ {
				age := time.Since(job.Created)
				if age > staleTTL {
					n := atomic.AddUint64(&droppedStale, 1)
					if n%500 == 0 {
						fmt.Printf("[stageB] dropped_stale=%d last_age=%s processQ_len=%d\n",
							n, age, len(processQ))
					}
					continue
				}

				res := worker.Process(ctx, id, job)
				atomic.AddUint64(&processed, 1)
				results <- res
			}
		}(workerID)
	}

	// ---- Stage A workers (fast) ----
	var wgA sync.WaitGroup
	wgA.Add(numStageAWorkers)

	for w := 1; w <= numStageAWorkers; w++ {
		stageAID := w
		go func(id int) {
			defer wgA.Done()

			for job := range ingestQ {
				atomic.AddUint64(&stageARead, 1)

				// FAST work (simulate cheap processor cost)
				// Keep this tiny; otherwise you blur the lesson.
				time.Sleep(time.Duration(1+rand.Intn(3)) * time.Millisecond)

				// Forward into processQ with IMMEDIATE DROP if full.
				// This is intentional: shed load BEFORE expensive Stage B.
				select {
				case processQ <- job:
					atomic.AddUint64(&enqProcess, 1)
				default:
					atomic.AddUint64(&dropProcessFull, 1)
				}
			}
		}(stageAID)
	}

	// Close processQ after all Stage A workers finish
	go func() {
		wgA.Wait()
		close(processQ)
	}()

	// Close results after Stage B workers finish
	go func() {
		wgB.Wait()
		close(results)
	}()

	// ---- Producer -> ingestQ ----
	go func() {
		for i := 1; i <= numJobs; i++ {
			atomic.AddUint64(&produced, 1)

			job := model.Job{
				ID:      i,
				Payload: i * 10,
				Created: time.Now(),
			}

			// Cheap path: immediate non-blocking enqueue to ingestQ
			select {
			case ingestQ <- job:
				atomic.AddUint64(&enqIngest, 1)

			default:
				// ingestQ is full
				if !allowWaitOnFull {
					atomic.AddUint64(&dropIngestFull, 1)
					continue
				}

				t := time.NewTimer(enqueueWaitBudget)
				select {
				case ingestQ <- job:
					if !t.Stop() {
						<-t.C
					}
					atomic.AddUint64(&enqIngest, 1)
				case <-t.C:
					atomic.AddUint64(&dropIngestTimeout, 1)
				}
			}

			if i%1000 == 0 {
				fmt.Printf("[producer] job=%d ingest_len=%d process_len=%d produced=%d enq_ingest=%d drop_ingest_full=%d drop_ingest_timeout=%d stageA_read=%d enq_process=%d drop_process_full=%d dropped_stale=%d processed=%d\n",
					i,
					len(ingestQ),
					len(processQ),
					atomic.LoadUint64(&produced),
					atomic.LoadUint64(&enqIngest),
					atomic.LoadUint64(&dropIngestFull),
					atomic.LoadUint64(&dropIngestTimeout),
					atomic.LoadUint64(&stageARead),
					atomic.LoadUint64(&enqProcess),
					atomic.LoadUint64(&dropProcessFull),
					atomic.LoadUint64(&droppedStale),
					atomic.LoadUint64(&processed),
				)
			}
		}

		close(ingestQ)
	}()

	// ---- Consumer ----
	count := 0
	for r := range results {
		count++
		_ = r
	}

	fmt.Printf("done results=%d produced=%d enq_ingest=%d drop_ingest_full=%d drop_ingest_timeout=%d stageA_read=%d enq_process=%d drop_process_full=%d dropped_stale=%d processed=%d\n",
		count,
		atomic.LoadUint64(&produced),
		atomic.LoadUint64(&enqIngest),
		atomic.LoadUint64(&dropIngestFull),
		atomic.LoadUint64(&dropIngestTimeout),
		atomic.LoadUint64(&stageARead),
		atomic.LoadUint64(&enqProcess),
		atomic.LoadUint64(&dropProcessFull),
		atomic.LoadUint64(&droppedStale),
		atomic.LoadUint64(&processed),
	)
}
