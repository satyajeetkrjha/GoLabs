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
	defer func() { fmt.Println("total_runtime:", time.Since(startAll)) }()

	const (
		numJobs    = 500000
		queueSize  = 5000
		numWorkers = 1
	)

	jobs := make(chan model.Job, queueSize)
	results := make(chan model.Result, queueSize)

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for w := 1; w <= numWorkers; w++ {
		workerID := w // capture
		go func(id int) {
			defer wg.Done()
			for job := range jobs {
				res := worker.Process(ctx, id, job)
				results <- res
			}
		}(workerID)
	}

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

			fmt.Printf("[producer] queue job=%d waited=%v queue_len=%d\n", job.ID, waited, len(jobs))

		}

		close(jobs)

	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	count := 0
	for r := range results {
		count++
		fmt.Printf("[result] job=%d worker=%d value=%d latency=%s err=%v\n",
			r.JobID, r.WorkerID, r.Value, r.Latency, r.Err)

	}

	fmt.Printf("all results processed count=%d\n", count)

}
