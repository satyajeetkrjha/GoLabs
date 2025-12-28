package worker

import (
	"context"
	"math/rand"
	"time"

	"worker-pool-lab/internal/model"
)

func Process(ctx context.Context, workerId int, job model.Job) model.Result {
	start := time.Now()

	select {
	case <-ctx.Done():
		return model.Result{
			JobID:      job.ID,
			WorkerID:   workerId,
			JobCreated: job.Created,
			Err:        ctx.Err(),
			Latency:    time.Since(start),
		}
	default:
	}

	work := time.Duration(80+rand.Intn(220)) * time.Millisecond
	time.Sleep(work)

	val := job.Payload * 2

	return model.Result{
		JobID:      job.ID,
		WorkerID:   workerId,
		Value:      val,
		JobCreated: job.Created,
		Err:        nil,
		Latency:    time.Since(start),
	}
}
