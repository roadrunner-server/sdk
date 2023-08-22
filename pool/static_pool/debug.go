package static_pool //nolint:stylecheck

import (
	"context"
	"runtime"

	"github.com/roadrunner-server/goridge/v3/pkg/frame"
	"github.com/roadrunner-server/sdk/v4/events"
	"github.com/roadrunner-server/sdk/v4/payload"
	"go.uber.org/zap"
)

// execDebug used when debug mode was not set and exec_ttl is 0
// TODO DRY
func (sp *Pool) execDebug(ctx context.Context, p *payload.Payload, stopCh chan struct{}) (chan *PExec, error) {
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

	// create channel for the stream (only if there are no errors)
	resp := make(chan *PExec, 1)

	switch {
	case rsp.Flags&frame.STREAM != 0:
		// in case of stream we should not return worker back immediately
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

			// stream iterator
			for {
				select {
				// we received stop signal
				case <-stopCh:
					err = w.StreamCancel(ctx)
					if err != nil {
						sp.log.Warn("stream cancel error", zap.Error(err))
					}
					runtime.Goexit()
				default:
					pld, next, err := w.StreamIter()
					if err != nil {
						resp <- newPExec(nil, err) // exit from the goroutine
						runtime.Goexit()
					}

					resp <- newPExec(pld, nil)
					if !next {
						runtime.Goexit()
					}
				}
			}
		}()

		return resp, nil
	default:
		resp <- newPExec(rsp, nil)
		// return worker back
		sp.ww.Release(w)
		// close the channel
		close(resp)
		return resp, nil
	}
}
