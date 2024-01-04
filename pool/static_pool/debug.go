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
		// read the exit status to prevent process to be a zombie
		_ = w.Wait()
	}()

	rsp, err := w.Exec(ctx, p)
	if err != nil {
		return nil, err
	}

	// create a channel for the stream (only if there are no errors)
	resp := make(chan *PExec, 1)

	switch {
	case rsp.Flags&frame.STREAM != 0:
		// in case of stream, we should not return worker immediately
		go func() {
			// would be called on Goexit
			defer func() {
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

			// send the initial frame
			sendResponse(resp, rsp, nil)

			// stream iterator
			for {
				select {
				// we received stop signal
				case <-stopCh:
					ctxT, cancelT := context.WithTimeout(ctx, sp.cfg.StreamTimeout)
					err = w.StreamCancel(ctxT)
					if err != nil {
						sp.log.Warn("stream cancel error", zap.Error(err))
						w.State().Transition(fsm.StateInvalid)
					}

					cancelT()
					runtime.Goexit()
				default:
					pld, next, errI := w.StreamIter(ctx)
					if errI != nil {
						sendResponse(resp, nil, errI)
						runtime.Goexit()
					}

					sendResponse(resp, pld, nil)
					if !next {
						// we've got the last frame
						runtime.Goexit()
					}
				}
			}
		}()

		return resp, nil
	default:
		sendResponse(resp, rsp, nil)
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
