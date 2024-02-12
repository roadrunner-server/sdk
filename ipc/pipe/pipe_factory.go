package pipe

import (
	"context"
	"os/exec"

	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/goridge/v3/pkg/pipe"
	"github.com/roadrunner-server/sdk/v4/fsm"
	"github.com/roadrunner-server/sdk/v4/internal"
	"github.com/roadrunner-server/sdk/v4/worker"
	"go.uber.org/zap"
)

// Factory connects to stack using standard
// streams (STDIN, STDOUT pipes).
type Factory struct {
	log *zap.Logger
}

// NewPipeFactory returns new factory instance and starts
// listening
func NewPipeFactory(log *zap.Logger) *Factory {
	return &Factory{
		log: log,
	}
}

type sr struct {
	w   *worker.Process
	err error
}

// SpawnWorker creates new Process and connects it to goridge relay,
// method Wait() must be handled on level above.
func (f *Factory) SpawnWorker(ctx context.Context, cmd *exec.Cmd, options ...worker.Options) (*worker.Process, error) {
	spCh := make(chan sr)
	go func() {
		w, err := worker.InitBaseWorker(cmd, options...)
		if err != nil {
			select {
			case spCh <- sr{
				w:   nil,
				err: err,
			}:
				return
			default:
				return
			}
		}

		in, err := cmd.StdoutPipe()
		if err != nil {
			select {
			case spCh <- sr{
				w:   nil,
				err: err,
			}:
				return
			default:
				return
			}
		}

		out, err := cmd.StdinPipe()
		if err != nil {
			select {
			case spCh <- sr{
				w:   nil,
				err: err,
			}:
				return
			default:
				return
			}
		}

		// Init new PIPE relay
		relay := pipe.NewPipeRelay(in, out)
		w.AttachRelay(relay)

		// Start the worker
		err = w.Start()
		if err != nil {
			select {
			case spCh <- sr{
				w:   nil,
				err: err,
			}:
				return
			default:
				return
			}
		}

		// used as a ping
		_, err = internal.Pid(relay)
		if err != nil {
			go func() {
				_ = w.Wait()
			}()
			_ = w.Kill()
			select {
			case spCh <- sr{
				w:   nil,
				err: err,
			}:
				return
			default:
				_ = w.Kill()
				return
			}
		}

		// everything ok, set ready state
		w.State().Transition(fsm.StateReady)

		select {
		case
		// return worker
		spCh <- sr{
			w:   w,
			err: nil,
		}:
			return
		default:
			_ = w.Kill()
			return
		}
	}()

	select {
	case <-ctx.Done():
		return nil, errors.E(errors.TimeOut)
	case res := <-spCh:
		if res.err != nil {
			return nil, res.err
		}
		return res.w, nil
	}
}

// Close the factory.
func (f *Factory) Close() error {
	return nil
}
