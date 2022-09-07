package fsm

import (
	"sync"
	"sync/atomic"

	"github.com/roadrunner-server/errors"
	"go.uber.org/zap"
)

// NewFSM returns new FSM implementation based on initial state and callbacks to move from one state to another
func NewFSM(initialState int64, log *zap.Logger) *fsm {
	return &fsm{
		currentState: &initialState,
	}
}

// FSMImpl is endure FSM interface implementation
type fsm struct {
	mutex    sync.Mutex
	numExecs uint64
	// to be lightweight, use UnixNano
	lastUsed     uint64
	currentState *int64
}

// CurrentState (see interface)
func (s *fsm) CurrentState() int64 {
	return atomic.LoadInt64(s.currentState)
}

func (s *fsm) Compare(state int64) bool {
	return atomic.LoadInt64(s.currentState) == state
}

/*
Transition moves endure from one state to another
Rules:
Transition table:
Event -> Init. Error on other events (Start, Stop)
1. Initializing -> Initialized
Event -> Start. Error on other events (Initialize, Stop)
2. Starting -> Started
Event -> Stop. Error on other events (Start, Initialize)
3. Stopping -> Stopped
*/
func (s *fsm) Transition(to int64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	err := s.recognizer(to)
	if err != nil {
		return
	}

	atomic.StoreInt64(s.currentState, to)
}

// String returns current StateImpl as string.
func (s *fsm) String() string {
	switch atomic.LoadInt64(s.currentState) {
	case StateInactive:
		return "inactive"
	case StateReady:
		return "ready"
	case StateWorking:
		return "working"
	case StateInvalid:
		return "invalid"
	case StateStopping:
		return "stopping"
	case StateStopped:
		return "stopped"
	case StateKilling:
		return "killing"
	case StateErrored:
		return "errored"
	case StateDestroyed:
		return "destroyed"
	}

	return "undefined"
}

// NumExecs returns number of registered WorkerProcess execs.
func (s *fsm) NumExecs() uint64 {
	return atomic.LoadUint64(&s.numExecs)
}

// IsActive returns true if WorkerProcess not Inactive or Stopped
func (s *fsm) IsActive() bool {
	return atomic.LoadInt64(s.currentState) == StateWorking ||
		atomic.LoadInt64(s.currentState) == StateReady
}

// RegisterExec register new execution atomically
func (s *fsm) RegisterExec() {
	atomic.AddUint64(&s.numExecs, 1)
}

// SetLastUsed Update last used time
func (s *fsm) SetLastUsed(lu uint64) {
	atomic.StoreUint64(&s.lastUsed, lu)
}

func (s *fsm) LastUsed() uint64 {
	return atomic.LoadUint64(&s.lastUsed)
}

// Acceptors (also called detectors or recognizers) produce binary output,
// indicating whether or not the received input is accepted.
// Each event of an acceptor is either accepting or non accepting.
func (s *fsm) recognizer(to int64) error {
	const op = errors.Op("fsm_recognizer")
	switch to {
	// to
	case StateInactive:
		// from
		if atomic.LoadInt64(s.currentState) == StateDestroyed {
			return errors.E(op, errors.Errorf("can't transition from state: %s", s.String()))
		}
	// to
	case StateReady:
		// from
		switch atomic.LoadInt64(s.currentState) {
		case StateWorking, StateInactive:
			return nil
		}

		return errors.E(op, errors.Errorf("can't transition from state: %s", s.String()))
	// to
	case StateWorking:
		// from
		if atomic.LoadInt64(s.currentState) == StateReady {
			return nil
		}

		return errors.E(op, errors.Errorf("can't transition from state: %s", s.String()))
	// to
	case StateInvalid:
		// from
		if atomic.LoadInt64(s.currentState) == StateDestroyed {
			return errors.E(op, errors.Errorf("can't transition from state: %s", s.String()))
		}
	// to
	case StateStopping:
		// from
		if atomic.LoadInt64(s.currentState) == StateDestroyed {
			return errors.E(op, errors.Errorf("can't transition from state: %s", s.String()))
		}
	// to
	case StateKilling:
		// from
		if atomic.LoadInt64(s.currentState) == StateDestroyed {
			return errors.E(op, errors.Errorf("can't transition from state: %s", s.String()))
		}
	// to
	case StateDestroyed:
		return nil
	// to
	case StateMaxJobsReached:
		// from
		if atomic.LoadInt64(s.currentState) == StateDestroyed {
			return errors.E(op, errors.Errorf("can't transition from state: %s", s.String()))
		}
	// to
	case StateStopped:
		// from
		if atomic.LoadInt64(s.currentState) == StateDestroyed {
			return errors.E(op, errors.Errorf("can't transition from state: %s", s.String()))
		}
	// to
	case StateErrored:
		// from
		if atomic.LoadInt64(s.currentState) == StateDestroyed {
			return errors.E(op, errors.Errorf("can't transition from state: %s", s.String()))
		}
	}

	return nil
}
