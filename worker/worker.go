package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/goridge/v3/pkg/frame"
	"github.com/roadrunner-server/goridge/v3/pkg/relay"
	"github.com/roadrunner-server/sdk/v3/fsm"
	"github.com/roadrunner-server/sdk/v3/internal"
	"github.com/roadrunner-server/sdk/v3/payload"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

type Options func(p *Process)

// Process - supervised process with api over goridge.Relay.
type Process struct {
	// created indicates at what time Process has been created.
	created time.Time
	log     *zap.Logger

	// fsm holds information about current Process state,
	// number of Process executions, buf status change time.
	// publicly this object is receive-only and protected using Mutex
	// and atomic counter.
	fsm *fsm.Fsm

	// underlying command with associated process, command must be
	// provided to Process from outside in non-started form. CmdSource
	// stdErr direction will be handled by Process to aggregate error message.
	cmd *exec.Cmd

	// pid of the process, points to pid of underlying process and
	// can be nil while process is not started.
	pid int

	fPool  sync.Pool
	bPool  sync.Pool
	chPool sync.Pool

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
		doneCh:  make(chan struct{}, 1),

		fPool: sync.Pool{
			New: func() any {
				return frame.NewFrame()
			},
		},

		bPool: sync.Pool{
			New: func() any {
				return new(bytes.Buffer)
			},
		},

		chPool: sync.Pool{
			New: func() any {
				return make(chan wexec, 1)
			},
		},
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

	w.fsm = fsm.NewFSM(fsm.StateInactive)

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

// Created returns time, worker was created at.
func (w *Process) Created() time.Time {
	return w.created
}

// State return receive-only Process state object, state can be used to safely access
// Process status, time when status changed and number of Process executions.
func (w *Process) State() *fsm.Fsm {
	return w.fsm
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
	st := w.fsm.String()
	// we can safely compare pid to 0
	if w.pid != 0 {
		st = st + ", pid:" + strconv.Itoa(w.pid)
	}

	return fmt.Sprintf(
		"(`%s` [%s], num_execs: %v)",
		strings.Join(w.cmd.Args, " "),
		st,
		w.fsm.NumExecs(),
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
// complete and will return process error (if any), if stderr is presented it is value
// will be wrapped as WorkerError. Method will return error code if php process fails
// to find or Start the script.
func (w *Process) Wait() error {
	const op = errors.Op("process_wait")
	var err error
	err = w.cmd.Wait()
	w.doneCh <- struct{}{}

	// If worker was destroyed, just exit
	if w.State().Compare(fsm.StateDestroyed) {
		return nil
	}

	// If state is different, and err is not nil, append it to the errors
	if err != nil {
		w.State().Transition(fsm.StateErrored)
		err = multierr.Combine(err, errors.E(op, err))
	}

	// closeRelay
	// at this point according to the documentation (see cmd.Wait comment)
	// if worker finishes with an error, message will be written to the stderr first
	// and then process.cmd.Wait return an error
	err2 := w.closeRelay()
	if err2 != nil {
		w.State().Transition(fsm.StateErrored)
		return multierr.Append(err, errors.E(op, err2))
	}

	if w.cmd.ProcessState.Success() {
		w.State().Transition(fsm.StateStopped)
		return nil
	}

	return err
}

// Exec payload without TTL timeout.
func (w *Process) Exec(p *payload.Payload) (*payload.Payload, error) {
	const op = errors.Op("worker_exec")

	if len(p.Body) == 0 && len(p.Context) == 0 {
		return nil, errors.E(op, errors.Str("payload can not be empty"))
	}

	if !w.State().Compare(fsm.StateReady) {
		return nil, errors.E(op, errors.Retry, errors.Errorf("Process is not ready (%s)", w.State().String()))
	}

	// set last used time
	w.State().SetLastUsed(uint64(time.Now().UnixNano()))
	w.State().Transition(fsm.StateWorking)

	rsp, err := w.execPayload(p)
	w.State().RegisterExec()
	if err != nil && !errors.Is(errors.Stop, err) {
		// just to be more verbose
		if !errors.Is(errors.SoftJob, err) {
			w.State().Transition(fsm.StateErrored)
		}
		return nil, errors.E(op, err)
	}

	// supervisor may set state of the worker during the work
	// in this case we should not re-write the worker state
	if !w.State().Compare(fsm.StateWorking) {
		return rsp, nil
	}

	w.State().Transition(fsm.StateReady)

	return rsp, nil
}

type wexec struct {
	payload *payload.Payload
	err     error
}

// ExecWithTTL executes payload without TTL timeout.
func (w *Process) ExecWithTTL(ctx context.Context, p *payload.Payload) (*payload.Payload, error) {
	const op = errors.Op("worker_exec_with_timeout")

	if len(p.Body) == 0 && len(p.Context) == 0 {
		return nil, errors.E(op, errors.Str("payload can not be empty"))
	}

	c := w.getCh()
	defer w.putCh(c)

	// worker was killed before it started to work (supervisor)
	if !w.State().Compare(fsm.StateReady) {
		return nil, errors.E(op, errors.Retry, errors.Errorf("Process is not ready (%s)", w.State().String()))
	}
	// set last used time
	w.State().SetLastUsed(uint64(time.Now().UnixNano()))
	w.State().Transition(fsm.StateWorking)

	go func() {
		rsp, err := w.execPayload(p)
		if err != nil {
			// just to be more verbose
			if !errors.Is(errors.SoftJob, err) {
				w.State().Transition(fsm.StateErrored)
				w.State().RegisterExec()
			}
			c <- wexec{
				err: errors.E(op, err),
			}
			return
		}

		if !w.State().Compare(fsm.StateWorking) {
			w.State().RegisterExec()
			c <- wexec{
				payload: rsp,
				err:     nil,
			}
			return
		}

		w.State().Transition(fsm.StateReady)
		w.State().RegisterExec()

		c <- wexec{
			payload: rsp,
			err:     nil,
		}
	}()

	select {
	// exec TTL reached
	case <-ctx.Done():
		errK := w.Kill()
		err := multierr.Combine(errK)
		// we should wait for the exit from the worker
		// 'c' channel here should return an error or nil
		// because the goroutine holds the payload pointer (from the sync.Pool)
		<-c
		if err != nil {
			// append timeout error
			err = multierr.Append(err, errors.E(op, errors.ExecTTL))
			return nil, multierr.Append(err, ctx.Err())
		}
		return nil, errors.E(op, errors.ExecTTL, ctx.Err())
	case res := <-c:
		if res.err != nil {
			return nil, res.err
		}
		return res.payload, nil
	}
}

// Stop sends soft termination command to the Process and waits for process completion.
func (w *Process) Stop() error {
	const op = errors.Op("process_stop")

	go func() {
		w.fsm.Transition(fsm.StateStopping)
		w.log.Debug("sending stop request to the worker", zap.Int("pid", w.pid))
		err := internal.SendControl(w.relay, &internal.StopCommand{Stop: true})
		if err == nil {
			w.fsm.Transition(fsm.StateStopped)
		}
	}()

	select {
	// finished, sent to the doneCh is made in the Wait() method
	// If we successfully sent a stop request, Wait() method will send a struct{} to the doneCh and we're done here
	// otherwise we have 10 seconds before we kill the process
	case <-w.doneCh:
		return nil
	case <-time.After(time.Second * 10):
		// kill process
		w.log.Warn("worker doesn't respond on stop command, killing process", zap.Int64("PID", w.Pid()))
		w.fsm.Transition(fsm.StateKilling)
		_ = w.cmd.Process.Signal(os.Kill)
		w.fsm.Transition(fsm.StateStopped)
		return errors.E(op, errors.Network)
	}
}

// Kill kills underlying process, make sure to call Wait() func to gather
// error log from the stderr. Does not wait for process completion!
func (w *Process) Kill() error {
	w.fsm.Transition(fsm.StateKilling)
	err := w.cmd.Process.Kill()
	if err != nil {
		return err
	}
	w.fsm.Transition(fsm.StateStopped)
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

func (w *Process) execPayload(p *payload.Payload) (*payload.Payload, error) {
	const op = errors.Op("sync_worker_exec_payload")

	// get a frame
	fr := w.getFrame()
	defer w.putFrame(fr)

	// can be 0 here
	fr.WriteVersion(fr.Header(), frame.Version1)
	fr.WriteFlags(fr.Header(), p.Codec)

	// obtain a buffer
	buf := w.get()

	buf.Write(p.Context)
	buf.Write(p.Body)

	// Context offset
	fr.WriteOptions(fr.HeaderPtr(), uint32(len(p.Context)))
	fr.WritePayloadLen(fr.Header(), uint32(buf.Len()))
	fr.WritePayload(buf.Bytes())

	fr.WriteCRC(fr.Header())

	// return buffer
	w.put(buf)

	err := w.Relay().Send(fr)
	if err != nil {
		return nil, errors.E(op, errors.Network, err)
	}

	frameR := w.getFrame()
	defer w.putFrame(frameR)

	err = w.Relay().Receive(frameR)
	if err != nil {
		return nil, errors.E(op, errors.Network, err)
	}
	if frameR == nil {
		return nil, errors.E(op, errors.Network, errors.Str("nil frame received"))
	}

	flags := frameR.ReadFlags()

	if flags&frame.ERROR != byte(0) {
		return nil, errors.E(op, errors.SoftJob, errors.Str(string(frameR.Payload())))
	}

	options := frameR.ReadOptions(frameR.Header())
	if len(options) != 1 {
		return nil, errors.E(op, errors.Decode, errors.Str("options length should be equal 1 (body offset)"))
	}

	// bound check
	if len(frameR.Payload()) < int(options[0]) {
		return nil, errors.E(errors.Network, errors.Errorf("bad payload %s", frameR.Payload()))
	}

	pld := &payload.Payload{
		Codec:   flags,
		Body:    make([]byte, len(frameR.Payload()[options[0]:])),
		Context: make([]byte, len(frameR.Payload()[:options[0]])),
	}

	// by copying we free frame's payload slice
	// we do not hold the pointer from the smaller slice to the initial (which should be in the sync.Pool)
	// https://blog.golang.org/slices-intro#TOC_6.
	copy(pld.Body, frameR.Payload()[options[0]:])
	copy(pld.Context, frameR.Payload()[:options[0]])

	return pld, nil
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

func (w *Process) get() *bytes.Buffer {
	return w.bPool.Get().(*bytes.Buffer)
}

func (w *Process) put(b *bytes.Buffer) {
	b.Reset()
	w.bPool.Put(b)
}

func (w *Process) getFrame() *frame.Frame {
	return w.fPool.Get().(*frame.Frame)
}

func (w *Process) putFrame(f *frame.Frame) {
	f.Reset()
	w.fPool.Put(f)
}

func (w *Process) getCh() chan wexec {
	return w.chPool.Get().(chan wexec)
}

func (w *Process) putCh(ch chan wexec) {
	// just check if the chan is not empty
	select {
	case <-ch:
		w.chPool.Put(ch)
	default:
		w.chPool.Put(ch)
	}
}
