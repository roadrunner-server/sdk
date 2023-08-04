package process

import (
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/sdk/v4/worker"
	"github.com/shirou/gopsutil/process"
)

// State provides information about specific worker.
type State struct {
	// Pid contains process id.
	Pid int64 `json:"pid"`
	// Status of the worker.
	Status int64 `json:"status"`
	// Number of worker executions.
	NumExecs uint64 `json:"numExecs"`
	// Created is unix nano timestamp of worker creation time.
	Created int64 `json:"created"`
	// MemoryUsage holds the information about worker memory usage in bytes.
	// Values might vary for different operating systems and based on RSS.
	MemoryUsage uint64 `json:"memoryUsage"`
	// CPU_Percent returns how many percent of the CPU time this process uses
	CPUPercent float64 `json:"CPUPercent"`
	// Command used in the service plugin and shows a command for the particular service
	Command string `json:"command"`
	// Status
	StatusStr string `json:"statusStr"`
}

// WorkerProcessState creates new worker state definition.
func WorkerProcessState(w *worker.Process) (*State, error) {
	const op = errors.Op("worker_process_state")
	p, _ := process.NewProcess(int32(w.Pid()))
	i, err := p.MemoryInfo()
	if err != nil {
		return nil, errors.E(op, err)
	}

	percent, err := p.CPUPercent()
	if err != nil {
		return nil, err
	}

	return &State{
		CPUPercent:  percent,
		Pid:         w.Pid(),
		Status:      w.State().CurrentState(),
		StatusStr:   w.State().String(),
		NumExecs:    w.State().NumExecs(),
		Created:     w.Created().UnixNano(),
		MemoryUsage: i.RSS,
	}, nil
}
