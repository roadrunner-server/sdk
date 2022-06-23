package worker

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/roadrunner-server/api/v2/worker"
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/goridge/v3/pkg/relay"
	"github.com/roadrunner-server/sdk/v2/internal"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

type Options func(p *Process)

// Process - supervised process with api over goridge.Relay.
type Process struct {
	// created indicates at what time Process has been created.
	created time.Time
	log     *zap.Logger

	// state holds information about current Process state,
	// number of Process executions, buf status change time.
	// publicly this object is receive-only and protected using Mutex
	// and atomic counter.
	state *StateImpl

	// underlying command with associated process, command must be
	// provided to Process from outside in non-started form. CmdSource
	// stdErr direction will be handled by Process to aggregate error message.
	cmd *exec.Cmd

	// pid of the process, points to pid of underlying process and
	// can be nil while process is not started.
	pid    int
	doneCh chan struct{}

	// communication bus with underlying process.
	relay relay.Relay
}

// InitBaseWorker creates new Process over given exec.cmd.
func InitBaseWorker(cmd *exec.Cmd, options ...Options) (*Process, error) {
	if cmd.Process != nil {
		return nil, fmt.Errorf("can't attach to running process")
	}

	w := &Process{
		created: time.Now(),
		cmd:     cmd,
		state:   NewWorkerState(worker.StateInactive),
		doneCh:  make(chan struct{}, 1),
	}

	// add options
	for i := 0; i < len(options); i++ {
		options[i](w)
	}

	if w.log == nil {
		z, err := zap.NewProduction()
		if err != nil {
			return nil, err
		}

		w.log = z
	}

	// set self as stderr implementation (Writer interface)
	rc, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}

	go func() {
		// https://linux.die.net/man/7/pipe
		// see pipe capacity
		buf := make([]byte, 65536)
		errCopy := copyBuffer(w, rc, buf)
		if errCopy != nil {
			w.log.Debug("stderr", zap.Error(errCopy))
		}
	}()

	return w, nil
}

func WithLog(z *zap.Logger) Options {
	return func(p *Process) {
		p.log = z
	}
}

// Pid returns worker pid.
func (w *Process) Pid() int64 {
	return int64(w.pid)
}

// Created returns time worker was created at.
func (w *Process) Created() time.Time {
	return w.created
}

// State return receive-only Process state object, state can be used to safely access
// Process status, time when status changed and number of Process executions.
func (w *Process) State() worker.State {
	return w.state
}

// AttachRelay attaches relay to the worker
func (w *Process) AttachRelay(rl relay.Relay) {
	w.relay = rl
}

// Relay returns relay attached to the worker
func (w *Process) Relay() relay.Relay {
	return w.relay
}

// String returns Process description. fmt.Stringer interface
func (w *Process) String() string {
	st := w.state.String()
	// we can safely compare pid to 0
	if w.pid != 0 {
		st = st + ", pid:" + strconv.Itoa(w.pid)
	}

	return fmt.Sprintf(
		"(`%s` [%s], num_execs: %v)",
		strings.Join(w.cmd.Args, " "),
		st,
		w.state.NumExecs(),
	)
}

func (w *Process) Start() error {
	err := w.cmd.Start()
	if err != nil {
		return err
	}
	w.pid = w.cmd.Process.Pid
	return nil
}

// Wait must be called once for each Process, call will be released once Process is
// complete and will return process error (if any), if stderr is presented it's value
// will be wrapped as WorkerError. Method will return error code if php process fails
// to find or Start the script.
func (w *Process) Wait() error {
	const op = errors.Op("process_wait")
	var err error
	err = w.cmd.Wait()
	w.doneCh <- struct{}{}

	// If worker was destroyed, just exit
	if w.State().Value() == worker.StateDestroyed {
		return nil
	}

	// If state is different, and err is not nil, append it to the errors
	if err != nil {
		w.State().Set(worker.StateErrored)
		err = multierr.Combine(err, errors.E(op, err))
	}

	// closeRelay
	// at this point according to the documentation (see cmd.Wait comment)
	// if worker finishes with an error, message will be written to the stderr first
	// and then process.cmd.Wait return an error
	err2 := w.closeRelay()
	if err2 != nil {
		w.State().Set(worker.StateErrored)
		return multierr.Append(err, errors.E(op, err2))
	}

	if w.cmd.ProcessState.Success() {
		w.State().Set(worker.StateStopped)
		return nil
	}

	return err
}

func (w *Process) closeRelay() error {
	if w.relay != nil {
		err := w.relay.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// Stop sends soft termination command to the Process and waits for process completion.
func (w *Process) Stop() error {
	const op = errors.Op("process_stop")

	select {
	// finished
	case <-w.doneCh:
		return nil
	default:
		w.state.Set(worker.StateStopping)
		err := internal.SendControl(w.relay, &internal.StopCommand{Stop: true})
		if err != nil {
			w.state.Set(worker.StateKilling)
			_ = w.cmd.Process.Signal(os.Kill)

			return errors.E(op, errors.Network, err)
		}

		<-w.doneCh
		w.state.Set(worker.StateStopped)
		return nil
	}
}

// Kill kills underlying process, make sure to call Wait() func to gather
// error log from the stderr. Does not wait for process completion!
func (w *Process) Kill() error {
	if w.State().Value() == worker.StateDestroyed {
		err := w.cmd.Process.Signal(os.Kill)
		if err != nil {
			return err
		}

		return nil
	}

	w.state.Set(worker.StateKilling)
	err := w.cmd.Process.Signal(os.Kill)
	if err != nil {
		return err
	}
	w.state.Set(worker.StateStopped)
	return nil
}

// Worker stderr
func (w *Process) Write(p []byte) (int, error) {
	// unsafe to use utils.AsString
	w.log.Info(string(p))
	return len(p), nil
}

// copyBuffer is the actual implementation of Copy and CopyBuffer.
func copyBuffer(dst io.Writer, src io.Reader, buf []byte) error {
	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw < 0 || nr < nw {
				nw = 0
				if ew == nil {
					ew = errors.Str("invalid write result")
				}
			}
			if ew != nil {
				return ew
			}
			if nr != nw {
				return io.ErrShortWrite
			}
		}
		if er != nil {
			if er != io.EOF {
				return er
			}
			break
		}
	}

	return nil
}
