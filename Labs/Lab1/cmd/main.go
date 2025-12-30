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

		// Stage B: exporter workers (slow) — now with retry/backoff
		numStageBWorkers = 8
	)

	// Producer enqueue policy (into ingestQ)
	const enqueueWaitBudget = 2 * time.Millisecond
	const allowWaitOnFull = true

	// Stale TTL: if job sits too long in the system, drop it
	const staleTTL = 2 * time.Second

	// Phase 6: Retry policy (exporter realism)
	const (
		retryMaxElapsed = 3 * time.Second // per-job retry budget (like exporter retry max elapsed)
		backoffBase     = 50 * time.Millisecond
		backoffMax      = 500 * time.Millisecond
	)

	ingestQ := make(chan model.Job, queueSizeIngest)
	processQ := make(chan model.Job, queueSizeProcess)
	results := make(chan model.Result, queueSizeProcess)

	// Counters
	var produced uint64

	// Producer -> ingestQ
	var enqIngest uint64
	var dropIngestFull uint64
	var dropIngestTimeout uint64

	// Stage A -> processQ
	var stageARead uint64
	var enqProcess uint64
	var dropProcessFull uint64

	// Stage B retry + outcome
	var droppedStale uint64
	var processed uint64

	var stageBAttempts uint64       // total attempts (including retries)
	var stageBFailures uint64       // failed attempts (transient errors)
	var stageBRetried uint64        // how many times we decided to retry
	var stageBGaveUp uint64         // dropped because retry budget exceeded
	var stageBBackoffSleepNs uint64 // time spent sleeping in backoff (sum)

	// ---- Stage B workers (exporter with retry/backoff) ----
	var wgB sync.WaitGroup
	wgB.Add(numStageBWorkers)

	for w := 1; w <= numStageBWorkers; w++ {
		workerID := w
		go func(id int) {
			defer wgB.Done()

			for job := range processQ {
				// Drop if already stale
				if time.Since(job.Created) > staleTTL {
					n := atomic.AddUint64(&droppedStale, 1)
					if n%500 == 0 {
						fmt.Printf("[stageB] dropped_stale=%d last_age=%s processQ_len=%d\n",
							n, time.Since(job.Created), len(processQ))
					}
					continue
				}

				// Per-job retry budget (like exporter retry max elapsed)
				deadline := time.Now().Add(retryMaxElapsed)
				backoff := backoffBase

				for {
					atomic.AddUint64(&stageBAttempts, 1)

					res := worker.Process(ctx, id, job)
					if res.Err == nil {
						atomic.AddUint64(&processed, 1)
						// NOTE: this can block if consumer lags; Phase-5 will address that.
						results <- res
						break
					}

					// Failed attempt
					atomic.AddUint64(&stageBFailures, 1)

					// If job becomes stale while retrying, drop.
					if time.Since(job.Created) > staleTTL {
						n := atomic.AddUint64(&droppedStale, 1)
						if n%500 == 0 {
							fmt.Printf("[stageB] dropped_stale=%d last_age=%s processQ_len=%d\n",
								n, time.Since(job.Created), len(processQ))
						}
						break
					}

					// Retry budget exceeded => give up (bounded retry)
					if time.Now().After(deadline) {
						atomic.AddUint64(&stageBGaveUp, 1)
						break
					}

					// We will retry
					atomic.AddUint64(&stageBRetried, 1)

					// Exponential backoff with jitter (0.5x..1.5x)
					jitterFactor := 0.5 + rand.Float64() // [0.5, 1.5)
					sleepFor := time.Duration(float64(backoff) * jitterFactor)

					// Cap sleep to not exceed remaining retry budget
					remaining := time.Until(deadline)
					if sleepFor > remaining {
						sleepFor = remaining
					}

					atomic.AddUint64(&stageBBackoffSleepNs, uint64(sleepFor.Nanoseconds()))
					time.Sleep(sleepFor)

					// Exponential growth with cap
					next := backoff * 2
					if next > backoffMax {
						next = backoffMax
					}
					backoff = next
				}
			}
		}(workerID)
	}

	// ---- Stage A workers (fast processors) ----
	var wgA sync.WaitGroup
	wgA.Add(numStageAWorkers)

	for w := 1; w <= numStageAWorkers; w++ {
		stageAID := w
		go func(id int) {
			defer wgA.Done()

			for job := range ingestQ {
				atomic.AddUint64(&stageARead, 1)

				// FAST work (cheap processor cost)
				time.Sleep(time.Duration(1+rand.Intn(3)) * time.Millisecond)

				// Forward into processQ with IMMEDIATE DROP if full
				select {
				case processQ <- job:
					atomic.AddUint64(&enqProcess, 1)
				default:
					atomic.AddUint64(&dropProcessFull, 1)
				}
			}
		}(stageAID)
	}

	// Close processQ after all Stage A workers finish (only senders to processQ are Stage A)
	go func() {
		wgA.Wait()
		close(processQ)
	}()

	// Close results after Stage B workers finish (only senders to results are Stage B)
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

			// Cheap path: immediate non-blocking enqueue
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
				sleepMs := float64(atomic.LoadUint64(&stageBBackoffSleepNs)) / 1e6
				fmt.Printf(
					"[producer] job=%d ingest_len=%d process_len=%d produced=%d enq_ingest=%d drop_ingest_full=%d drop_ingest_timeout=%d "+
						"stageA_read=%d enq_process=%d drop_process_full=%d dropped_stale=%d processed=%d "+
						"b_attempts=%d b_fail=%d b_retry=%d b_giveup=%d b_backoff_ms=%.1f\n",
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
					atomic.LoadUint64(&stageBAttempts),
					atomic.LoadUint64(&stageBFailures),
					atomic.LoadUint64(&stageBRetried),
					atomic.LoadUint64(&stageBGaveUp),
					sleepMs,
				)
			}
		}

		close(ingestQ) // only producer sends to ingestQ
	}()

	// ---- Consumer ----
	count := 0
	for r := range results {
		count++
		_ = r
	}

	sleepMs := float64(atomic.LoadUint64(&stageBBackoffSleepNs)) / 1e6
	fmt.Printf(
		"done results=%d produced=%d enq_ingest=%d drop_ingest_full=%d drop_ingest_timeout=%d "+
			"stageA_read=%d enq_process=%d drop_process_full=%d dropped_stale=%d processed=%d "+
			"b_attempts=%d b_fail=%d b_retry=%d b_giveup=%d b_backoff_ms=%.1f\n",
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
		atomic.LoadUint64(&stageBAttempts),
		atomic.LoadUint64(&stageBFailures),
		atomic.LoadUint64(&stageBRetried),
		atomic.LoadUint64(&stageBGaveUp),
		sleepMs,
	)
}
