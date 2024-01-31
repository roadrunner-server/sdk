package static_pool //nolint:stylecheck

import (
	"context"
	"runtime"

	"github.com/roadrunner-server/goridge/v3/pkg/frame"
	"github.com/roadrunner-server/sdk/v4/events"
	"github.com/roadrunner-server/sdk/v4/fsm"
	"github.com/roadrunner-server/sdk/v4/payload"
	"go.uber.org/zap"
)

// execDebug used when debug mode was not set and exec_ttl is 0
// TODO DRY
func (sp *Pool) execDebug(ctx context.Context, p *payload.Payload, stopCh chan struct{}) (chan *PExec, error) {
	sp.log.Debug("executing in debug mode, worker will be destroyed after response is received")
	w, err := sp.allocator()
	if err != nil {
		return nil, err
	}

	go func() {
		// read the exit status to prevent process to become a zombie
		_ = w.Wait()
	}()

	rsp, err := w.Exec(ctx, p)
	if err != nil {
		return nil, err
	}

	switch {
	case rsp.Flags&frame.STREAM != 0:
		// create a channel for the stream (only if there are no errors)
		resp := make(chan *PExec, 5)
		// send the initial frame
		resp <- newPExec(rsp, nil)

		// in case of stream, we should not return worker immediately
		go func() {
			// would be called on Goexit
			defer func() {
				sp.log.Debug("stopping [stream] worker", zap.Int("pid", int(w.Pid())), zap.String("state", w.State().String()))
				close(resp)
				// destroy the worker
				errD := w.Stop()
				if errD != nil {
					sp.log.Debug(
						"debug mode: worker stopped with error",
						zap.String("reason", "worker error"),
						zap.Int64("pid", w.Pid()),
						zap.String("internal_event_name", events.EventWorkerError.String()),
						zap.Error(errD),
					)
				}
			}()

			// stream iterator
			for {
				select {
				// we received stop signal
				case <-stopCh:
					sp.log.Debug("stream stop signal received", zap.Int("pid", int(w.Pid())), zap.String("state", w.State().String()))
					ctxT, cancelT := context.WithTimeout(ctx, sp.cfg.StreamTimeout)
					err = w.StreamCancel(ctxT)
					cancelT()
					if err != nil {
						w.State().Transition(fsm.StateErrored)
						sp.log.Warn("stream cancel error", zap.Error(err))
					} else {
						// successfully canceled
						w.State().Transition(fsm.StateReady)
						sp.log.Debug("transition to the ready state", zap.String("from", w.State().String()))
					}

					runtime.Goexit()
				default:
					// we have to set a stream timeout on every request
					switch sp.supervisedExec {
					case true:
						ctxT, cancelT := context.WithTimeout(context.Background(), sp.cfg.Supervisor.ExecTTL)
						pld, next, errI := w.StreamIterWithContext(ctxT)
						cancelT()
						if errI != nil {
							sp.log.Warn("stream error", zap.Error(err))

							resp <- newPExec(nil, errI)

							// move worker to the invalid state to restart
							w.State().Transition(fsm.StateInvalid)
							runtime.Goexit()
						}

						resp <- newPExec(pld, nil)

						if !next {
							w.State().Transition(fsm.StateReady)
							// we've got the last frame
							runtime.Goexit()
						}
					case false:
						// non supervised execution, can potentially hang here
						pld, next, errI := w.StreamIter()
						if errI != nil {
							sp.log.Warn("stream iter error", zap.Error(err))
							// send error response
							resp <- newPExec(nil, errI)

							// move worker to the invalid state to restart
							w.State().Transition(fsm.StateInvalid)
							runtime.Goexit()
						}

						resp <- newPExec(pld, nil)

						if !next {
							w.State().Transition(fsm.StateReady)
							// we've got the last frame
							runtime.Goexit()
						}
					}
				}
			}
		}()

		return resp, nil
	default:
		resp := make(chan *PExec, 1)
		resp <- newPExec(rsp, nil)
		// close the channel
		close(resp)

		errD := w.Stop()
		if errD != nil {
			sp.log.Debug(
				"debug mode: worker stopped with error",
				zap.String("reason", "worker error"),
				zap.Int64("pid", w.Pid()),
				zap.String("internal_event_name", events.EventWorkerError.String()),
				zap.Error(errD),
			)
		}

		return resp, nil
	}
}
