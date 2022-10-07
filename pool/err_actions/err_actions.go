package err_actions //nolint:stylecheck

import (
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/sdk/v2/events"
	"github.com/roadrunner-server/sdk/v2/worker"
	"github.com/roadrunner-server/sdk/v2/worker/fsm"
	workerWatcher "github.com/roadrunner-server/sdk/v2/worker_watcher"
	"go.uber.org/zap"
)

// ErrActions method used to decide what to do with the worker based on the occurred error
/*
ExecTTL -> move to the invalid state
*/
func ErrActions(err error, ww *workerWatcher.WorkerWatcher, w *worker.Process, log *zap.Logger, maxJobs uint64) error {
	if maxJobs != 0 && w.State().NumExecs() >= maxJobs {
		// mark old as invalid and stop
		w.State().Transition(fsm.StateInvalid)
	}

	// just push event if on any stage was timeout error
	switch {
	// for this case, worker already killed in the ExecTTL function
	case errors.Is(errors.ExecTTL, err):
		log.Warn("worker stopped, and will be restarted", zap.String("reason", "execTTL timeout elapsed"), zap.Int64("pid", w.Pid()), zap.String("internal_event_name", events.EventExecTTL.String()), zap.Error(err))
		w.State().Transition(fsm.StateInvalid)
		ww.Release(w)

		return err
	case errors.Is(errors.SoftJob, err):
		log.Warn("worker stopped, and will be restarted", zap.String("reason", "worker error"), zap.Int64("pid", w.Pid()), zap.String("internal_event_name", events.EventWorkerError.String()), zap.Error(err))
		// soft jobs errors are allowed, just put the worker back
		ww.Release(w)

		return err
	case errors.Is(errors.Network, err):
		// in case of network error, we can't stop the worker, we should kill it
		w.State().Transition(fsm.StateInvalid)
		log.Warn("network error", zap.String("reason", "network"), zap.Int64("pid", w.Pid()), zap.String("internal_event_name", events.EventWorkerError.String()), zap.Error(err))
		// kill the worker instead of sending net packet to it
		_ = w.Kill()

		return err
	default:
		w.State().Transition(fsm.StateInvalid)
		log.Warn("worker will be restarted", zap.Int64("pid", w.Pid()), zap.String("internal_event_name", events.EventWorkerDestruct.String()), zap.Error(err))

		return err
	}
}
