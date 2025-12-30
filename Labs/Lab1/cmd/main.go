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

		// Results buffering: should usually be bigger than processQ because it’s after the slow stage.
		queueSizeResults = 5000

		// Stage A: fast workers
		numStageAWorkers = 8

		// Stage B: exporter workers (slow) — retry/backoff
		numStageBWorkers = 8
	)

	// Producer enqueue policy (into ingestQ)
	const enqueueWaitBudget = 2 * time.Millisecond
	const allowWaitOnFull = true

	// Stale TTL: if job sits too long in the system, drop it
	const staleTTL = 2 * time.Second

	// Phase 6: Retry policy (exporter realism)
	const (
		retryMaxElapsed = 3 * time.Second
		backoffBase     = 50 * time.Millisecond
		backoffMax      = 500 * time.Millisecond
	)

	// Results publish policy (StageB -> results)
	// In real collectors, exporters should not block forever on downstream consumers.
	const resultsWaitBudget = 2 * time.Millisecond // bounded wait before we drop result
	const dropResultsOnFull = true                 // if false, we will block (not recommended)

	ingestQ := make(chan model.Job, queueSizeIngest)
	processQ := make(chan model.Job, queueSizeProcess)

	// StageB writes here
	results := make(chan model.Result, queueSizeResults)

	// Consumer reads from here (drained by a single goroutine)
	consQ := make(chan model.Result, queueSizeResults)

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

	var stageBAttempts uint64
	var stageBFailures uint64
	var stageBRetried uint64
	var stageBGaveUp uint64
	var stageBBackoffSleepNs uint64

	// Results path counters
	var resultsEnq uint64
	var resultsDropFull uint64
	var resultsDropTimeout uint64

	// ---- Results drainer (decouples StageB from consumer) ----
	// This makes "consumer slowness" not directly stall StageB.
	// If consumer is slower than production, consQ will fill — then the drainer blocks,
	// and StageB only blocks/drops according to the results publish policy below.
	var drainWG sync.WaitGroup
	drainWG.Add(1)
	go func() {
		defer drainWG.Done()
		for r := range results {
			consQ <- r // blocking is fine here; this goroutine is the only place we allow it.
		}
		close(consQ)
	}()

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

				deadline := time.Now().Add(retryMaxElapsed)
				backoff := backoffBase

				for {
					atomic.AddUint64(&stageBAttempts, 1)

					res := worker.Process(ctx, id, job)
					if res.Err == nil {
						atomic.AddUint64(&processed, 1)

						// FIX: results send must not silently become the bottleneck.
						// We enforce bounded behavior here.
						if dropResultsOnFull {
							// fast path: try immediate send
							select {
							case results <- res:
								atomic.AddUint64(&resultsEnq, 1)
							default:
								// optional: bounded wait before dropping
								t := time.NewTimer(resultsWaitBudget)
								select {
								case results <- res:
									if !t.Stop() {
										<-t.C
									}
									atomic.AddUint64(&resultsEnq, 1)
								case <-t.C:
									atomic.AddUint64(&resultsDropTimeout, 1)
								}
							}
						} else {
							// Not recommended, but kept as an option: fully blocking send.
							results <- res
							atomic.AddUint64(&resultsEnq, 1)
						}

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

					// Retry budget exceeded => give up
					if time.Now().After(deadline) {
						atomic.AddUint64(&stageBGaveUp, 1)
						break
					}

					atomic.AddUint64(&stageBRetried, 1)

					// Exponential backoff with jitter (0.5x..1.5x)
					jitterFactor := 0.5 + rand.Float64()
					sleepFor := time.Duration(float64(backoff) * jitterFactor)

					remaining := time.Until(deadline)
					if sleepFor > remaining {
						sleepFor = remaining
					}

					atomic.AddUint64(&stageBBackoffSleepNs, uint64(sleepFor.Nanoseconds()))
					time.Sleep(sleepFor)

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

	// Close processQ after Stage A finishes
	go func() {
		wgA.Wait()
		close(processQ)
	}()

	// Close results after Stage B finishes
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

			select {
			case ingestQ <- job:
				atomic.AddUint64(&enqIngest, 1)
			default:
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
					"[producer] job=%d ingest_len=%d process_len=%d results_len=%d cons_len=%d "+
						"produced=%d enq_ingest=%d drop_ingest_full=%d drop_ingest_timeout=%d "+
						"stageA_read=%d enq_process=%d drop_process_full=%d dropped_stale=%d processed=%d "+
						"b_attempts=%d b_fail=%d b_retry=%d b_giveup=%d b_backoff_ms=%.1f "+
						"results_enq=%d results_drop_timeout=%d\n",
					i,
					len(ingestQ),
					len(processQ),
					len(results),
					len(consQ),
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
					atomic.LoadUint64(&resultsEnq),
					atomic.LoadUint64(&resultsDropTimeout),
				)
			}
		}

		close(ingestQ)
	}()

	// ---- Consumer (read from consQ, not results) ----
	count := 0
	for r := range consQ {
		count++
		_ = r
	}

	// Ensure drainer exited (it will, since consQ closes after results closes)
	drainWG.Wait()

	sleepMs := float64(atomic.LoadUint64(&stageBBackoffSleepNs)) / 1e6
	fmt.Printf(
		"done results=%d produced=%d enq_ingest=%d drop_ingest_full=%d drop_ingest_timeout=%d "+
			"stageA_read=%d enq_process=%d drop_process_full=%d dropped_stale=%d processed=%d "+
			"b_attempts=%d b_fail=%d b_retry=%d b_giveup=%d b_backoff_ms=%.1f "+
			"results_enq=%d results_drop_full=%d results_drop_timeout=%d\n",
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
		atomic.LoadUint64(&resultsEnq),
		atomic.LoadUint64(&resultsDropFull),
		atomic.LoadUint64(&resultsDropTimeout),
	)
}
