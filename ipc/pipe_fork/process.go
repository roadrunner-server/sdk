//go:build linux || darwin

package pipefork

import (
	"context"
	"os/exec"

	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/goridge/v3/pkg/pipe"
	"github.com/roadrunner-server/sdk/v3/fsm"
	"github.com/roadrunner-server/sdk/v3/internal"
	"github.com/roadrunner-server/sdk/v3/worker"
	"go.uber.org/zap"
)

type PipeFork struct {
	log *zap.Logger
}

func NewPipeFork() *PipeFork {
	return &PipeFork{}
}

type sr struct {
	w   *worker.Process
	err error
}

// SpawnWorkerWithTimeout creates new Process and connects it to goridge relay,
// method Wait() must be handled on level above.
func (f *PipeFork) SpawnWorkerWithTimeout(ctx context.Context, cmd *exec.Cmd) (*worker.Process, error) {
	spCh := make(chan sr)
	go func() {
		w, err := worker.InitBaseWorker(cmd, worker.WithLog(f.log))
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

func (f *PipeFork) SpawnWorker(cmd *exec.Cmd) (*worker.Process, error) {
	w, err := worker.InitBaseWorker(cmd, worker.WithLog(f.log))
	if err != nil {
		return nil, err
	}

	in, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	out, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}

	// Init new PIPE relay
	relay := pipe.NewPipeRelay(in, out)
	w.AttachRelay(relay)

	// Start the worker
	err = w.Start()
	if err != nil {
		return nil, err
	}

	// errors bundle
	_, err = internal.Pid(relay)
	if err != nil {
		_ = w.Kill()
		return nil, err
	}

	// everything ok, set ready state
	w.State().Transition(fsm.StateReady)
	return w, nil
}

// Close the factory.
func (f *PipeFork) Close() error {
	return nil
}
