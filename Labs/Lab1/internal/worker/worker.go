package worker

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"worker-pool-lab/internal/model"
)

var ErrTransient = errors.New("transient exporter error")

// Process simulates exporter work.
// - It sleeps to model service time.
// - It randomly fails with a transient error to trigger retry logic in Stage B.
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

	// Simulate exporter/network/service time
	work := time.Duration(80+rand.Intn(220)) * time.Millisecond
	time.Sleep(work)

	// Simulate transient failure probability (tune this)
	const failProb = 0.12 // 12% failures
	if rand.Float64() < failProb {
		return model.Result{
			JobID:      job.ID,
			WorkerID:   workerId,
			JobCreated: job.Created,
			Err:        ErrTransient,
			Latency:    time.Since(start),
		}
	}

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
