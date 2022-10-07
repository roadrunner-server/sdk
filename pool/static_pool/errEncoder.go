package static_pool //nolint:stylecheck

import (
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/sdk/v2/events"
	"github.com/roadrunner-server/sdk/v2/worker"
	"github.com/roadrunner-server/sdk/v2/worker/fsm"
	"go.uber.org/zap"
)

func (sp *Pool) encodeErr(err error, w *worker.Worker) error {
	// just push event if on any stage was timeout error
	switch {
	// for this case, worker already killed in the ExecTTL function
	case errors.Is(errors.ExecTTL, err):
		sp.log.Warn("worker stopped, and will be restarted", zap.String("reason", "execTTL timeout elapsed"), zap.Int64("pid", w.Pid()), zap.String("internal_event_name", events.EventExecTTL.String()), zap.Error(err))
		w.State().Transition(fsm.StateInvalid)
		return err

	case errors.Is(errors.SoftJob, err):
		sp.log.Warn("worker stopped, and will be restarted", zap.String("reason", "worker error"), zap.Int64("pid", w.Pid()), zap.String("internal_event_name", events.EventWorkerError.String()), zap.Error(err))
		// if max jobs exceed
		if sp.cfg.MaxJobs != 0 && w.State().NumExecs() >= sp.cfg.MaxJobs {
			// mark old as invalid and stop
			w.State().Transition(fsm.StateInvalid)
			errS := w.Stop()
			if errS != nil {
				return errors.E(errors.SoftJob, errors.Errorf("err: %v\nerrStop: %v", err, errS))
			}

			return err
		}

		// soft jobs errors are allowed, just put the worker back
		sp.ww.Release(w)

		return err
	case errors.Is(errors.Network, err):
		// in case of network error, we can't stop the worker, we should kill it
		w.State().Transition(fsm.StateInvalid)
		sp.log.Warn("network error", zap.String("reason", "network"), zap.Int64("pid", w.Pid()), zap.String("internal_event_name", events.EventWorkerError.String()), zap.Error(err))
		// kill the worker instead of sending net packet to it
		_ = w.Kill()

		return err
	default:
		w.State().Transition(fsm.StateInvalid)
		sp.log.Warn("worker will be restarted", zap.Int64("pid", w.Pid()), zap.String("internal_event_name", events.EventWorkerDestruct.String()), zap.Error(err))
		// stop the worker, worker here might be in the broken state (network)
		errS := w.Stop()
		if errS != nil {
			return errors.E(errors.Errorf("err: %v\nerrStop: %v", err, errS))
		}

		return err
	}
}
