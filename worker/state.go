package worker

import (
	"sync/atomic"

	"github.com/roadrunner-server/api/v2/worker"
)

type StateImpl struct {
	value    int64
	numExecs uint64
	// to be lightweight, use UnixNano
	lastUsed uint64
}

// NewWorkerState initializes a state for the sync.Worker
func NewWorkerState(value int64) *StateImpl {
	return &StateImpl{value: value}
}

// String returns current StateImpl as string.
func (s *StateImpl) String() string {
	switch s.Value() {
	case worker.StateInactive:
		return "inactive"
	case worker.StateReady:
		return "ready"
	case worker.StateWorking:
		return "working"
	case worker.StateInvalid:
		return "invalid"
	case worker.StateStopping:
		return "stopping"
	case worker.StateStopped:
		return "stopped"
	case worker.StateKilling:
		return "killing"
	case worker.StateErrored:
		return "errored"
	case worker.StateDestroyed:
		return "destroyed"
	}

	return "undefined"
}

// NumExecs returns number of registered WorkerProcess execs.
func (s *StateImpl) NumExecs() uint64 {
	return atomic.LoadUint64(&s.numExecs)
}

// Value StateImpl returns StateImpl value
func (s *StateImpl) Value() int64 {
	return atomic.LoadInt64(&s.value)
}

// IsActive returns true if WorkerProcess not Inactive or Stopped
func (s *StateImpl) IsActive() bool {
	val := s.Value()
	return val == worker.StateWorking || val == worker.StateReady
}

// Set change StateImpl value (status)
func (s *StateImpl) Set(value int64) {
	atomic.StoreInt64(&s.value, value)
}

// RegisterExec register new execution atomically
func (s *StateImpl) RegisterExec() {
	atomic.AddUint64(&s.numExecs, 1)
}

// SetLastUsed Update last used time
func (s *StateImpl) SetLastUsed(lu uint64) {
	atomic.StoreUint64(&s.lastUsed, lu)
}

func (s *StateImpl) LastUsed() uint64 {
	return atomic.LoadUint64(&s.lastUsed)
}
