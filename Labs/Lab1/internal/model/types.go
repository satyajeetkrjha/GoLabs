package model

import "time"

type Job struct {
	ID      int
	Payload int
	Created time.Time
}

type Result struct {
	JobID    int
	WorkerID int
	Value    int
	Latency  time.Duration
	Err      error
}
