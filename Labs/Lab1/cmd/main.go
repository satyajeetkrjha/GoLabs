package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
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

	jobs := make(chan model.Job, queueSize)
	results := make(chan model.Result, queueSize)

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	// Workers
	for w := 1; w <= numWorkers; w++ {
		workerID := w
		go func(id int) {
			defer wg.Done()
			for job := range jobs {
				res := worker.Process(ctx, id, job)
				results <- res
			}
		}(workerID)
	}

	// Producer
	go func() {
		for i := 1; i <= numJobs; i++ {
			job := model.Job{
				ID:      i,
				Payload: i * 10,
				Created: time.Now(),
			}

			t0 := time.Now()
			jobs <- job
			waited := time.Since(t0)

			if i%1000 == 0 {
				fmt.Printf("[producer] job=%d waited=%v queue_len=%d\n",
					job.ID, waited, len(jobs))
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
		e2e := time.Since(r.JobCreated)

		if count%1000 == 0 {
			fmt.Printf(
				"[result] job=%d worker=%d worker_latency=%s e2e=%s\n",
				r.JobID,
				r.WorkerID,
				r.Latency,
				e2e,
			)
		}
	}

	fmt.Printf("all results processed count=%d\n", count)
}
