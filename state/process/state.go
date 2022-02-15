package process

import (
	ps "github.com/roadrunner-server/api/v2/state/process"
	"github.com/roadrunner-server/api/v2/worker"
	"github.com/roadrunner-server/errors"
	"github.com/shirou/gopsutil/process"
)

// WorkerProcessState creates new worker state definition.
func WorkerProcessState(w worker.BaseProcess) (*ps.State, error) {
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

	return &ps.State{
		CPUPercent:  percent,
		Pid:         int(w.Pid()),
		Status:      w.State().String(),
		NumJobs:     w.State().NumExecs(),
		Created:     w.Created().UnixNano(),
		MemoryUsage: i.RSS,
	}, nil
}
