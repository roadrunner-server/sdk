package process

import (
	"github.com/roadrunner-server/api/v2/worker"
	"github.com/roadrunner-server/errors"
	"github.com/shirou/gopsutil/process"
)

// State provides information about specific worker.
type State struct {
	// Pid contains process id.
	Pid_ int64 `json:"pid"`

	// Status of the worker.
	Status_ int64 `json:"status"`

	// Number of worker executions.
	NumExecs_ uint64 `json:"numExecs"`

	// Created is unix nano timestamp of worker creation time.
	Created_ int64 `json:"created"`

	// MemoryUsage holds the information about worker memory usage in bytes.
	// Values might vary for different operating systems and based on RSS.
	MemoryUsage_ uint64 `json:"memoryUsage"`

	// CPU_Percent returns how many percent of the CPU time this process uses
	CPUPercent_ float64

	// Command used in the service plugin and shows a command for the particular service
	Command_ string

	// state
	status string
}

// WorkerProcessState creates new worker state definition.
func WorkerProcessState(w worker.BaseProcess) (*State, error) {
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
		CPUPercent_:  percent,
		Pid_:         w.Pid(),
		Status_:      w.State().CurrentState(),
		NumExecs_:    w.State().NumExecs(),
		Created_:     w.Created().UnixNano(),
		MemoryUsage_: i.RSS,
		status:       w.State().String(),
	}, nil
}

func (s *State) String() string {
	return s.status
}

func (s *State) Pid() int64 {
	return s.Pid_
}

func (s *State) Status() int64 {
	return s.Status_
}

func (s *State) NumExecs() uint64 {
	return s.NumExecs_
}

func (s *State) Created() int64 {
	return s.Created_
}

func (s *State) MemoryUsage() uint64 {
	return s.MemoryUsage_
}

func (s *State) CPUPercent() float64 {
	return s.CPUPercent_
}

func (s *State) Command() string {
	return s.Command_
}
