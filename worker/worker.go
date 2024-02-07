package worker

import (
	"bytes"
	"context"
	stderr "errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/goridge/v3/pkg/frame"
	"github.com/roadrunner-server/goridge/v3/pkg/relay"
	"github.com/roadrunner-server/sdk/v4/fsm"
	"github.com/roadrunner-server/sdk/v4/internal"
	"github.com/roadrunner-server/sdk/v4/payload"
	"go.uber.org/zap"
)

type Options func(p *Process)

// Process - supervised process with api over goridge.Relay.
type Process struct {
	// created indicates at what time Process has been created.
	created time.Time
	log     *zap.Logger

	maxExecs uint64

	callback func()
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

// internal struct to pass data between goroutines
type wexec struct {
	payload *payload.Payload
	err     error
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
				return make(chan *wexec, 1)
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

	w.fsm = fsm.NewFSM(fsm.StateInactive, w.log)

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

func (w *Process) AddCallback(cb func()) {
	w.callback = cb
}

func (w *Process) Callback() {
	if w.callback == nil {
		return
	}
	w.callback()
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
		err = stderr.Join(err, errors.E(op, err))
	}

	// closeRelay
	// at this point according to the documentation (see cmd.Wait comment)
	// if worker finishes with an error, message will be written to the stderr first
	// and then process.cmd.Wait return an error
	err2 := w.closeRelay()
	if err2 != nil {
		w.State().Transition(fsm.StateErrored)
		return stderr.Join(err, errors.E(op, err2))
	}

	if w.cmd.ProcessState.Success() {
		w.State().Transition(fsm.StateStopped)
	}

	return err
}

func (w *Process) StreamIter() (*payload.Payload, bool, error) {
	pld, err := w.receiveFrame()
	if err != nil {
		return nil, false, err
	}

	// PING, we should respond with PONG
	if pld.Flags&frame.PING != 0 {
		if err := w.sendPONG(); err != nil {
			return nil, false, err
		}
	}

	// !=0 -> we have stream bit set, so stream is available
	return pld, pld.Flags&frame.STREAM != 0, nil
}

// StreamIter returns true if stream is available and payload
func (w *Process) StreamIterWithContext(ctx context.Context) (*payload.Payload, bool, error) {
	c := w.getCh()

	go func() {
		rsp, err := w.receiveFrame()
		if err != nil {
			c <- &wexec{
				err: err,
			}

			w.log.Debug("stream iter error", zap.Int64("pid", w.Pid()), zap.Error(err))
			// trash response
			rsp = nil
			runtime.Goexit()
		}

		c <- &wexec{
			payload: rsp,
		}
	}()

	select {
	// exec TTL reached
	case <-ctx.Done():
		// we should kill the process here to ensure that it exited
		errK := w.Kill()
		err := stderr.Join(errK, ctx.Err())
		// we should wait for the exit from the worker
		// 'c' channel here should return an error or nil
		// because the goroutine holds the payload pointer (from the sync.Pool)
		<-c
		w.putCh(c)
		return nil, false, errors.E(errors.ExecTTL, err)
	case res := <-c:
		w.putCh(c)

		if res.err != nil {
			return nil, false, res.err
		}

		// PING, we should respond with PONG
		if res.payload.Flags&frame.PING != 0 {
			if err := w.sendPONG(); err != nil {
				return nil, false, err
			}
		}

		// pld.Flags&frame.STREAM !=0 -> we have stream bit set, so stream is available
		return res.payload, res.payload.Flags&frame.STREAM != 0, nil
	}
}

// StreamCancel sends stop bit to the worker
func (w *Process) StreamCancel(ctx context.Context) error {
	const op = errors.Op("sync_worker_send_frame")
	if !w.State().Compare(fsm.StateWorking) {
		return errors.Errorf("worker is not in the Working state, actual state: (%s)", w.State().String())
	}

	w.log.Debug("stream was canceled, sending stop bit", zap.Int64("pid", w.Pid()))
	// get a frame
	fr := w.getFrame()

	fr.WriteVersion(fr.Header(), frame.Version1)

	fr.SetStopBit(fr.Header())
	fr.WriteCRC(fr.Header())

	err := w.Relay().Send(fr)
	w.State().RegisterExec()
	if err != nil {
		w.putFrame(fr)
		return errors.E(op, errors.Network, err)
	}

	w.putFrame(fr)
	c := w.getCh()

	w.log.Debug("stop bit was sent, waiting for the response", zap.Int64("pid", w.Pid()))

	go func() {
		for {
			rsp, errrf := w.receiveFrame()
			if errrf != nil {
				c <- &wexec{
					err: errrf,
				}

				w.log.Debug("stream cancel error", zap.Int64("pid", w.Pid()), zap.Error(errrf))
				// trash response
				rsp = nil
				runtime.Goexit()
			}

			// stream has ended
			if rsp.Flags&frame.STREAM == 0 {
				w.log.Debug("stream has ended", zap.Int64("pid", w.Pid()))
				c <- &wexec{}
				// trash response
				rsp = nil
				runtime.Goexit()
			}
		}
	}()

	select {
	// exec TTL reached
	case <-ctx.Done():
		errK := w.Kill()
		err := stderr.Join(errK, ctx.Err())
		// we should wait for the exit from the worker
		// 'c' channel here should return an error or nil
		// because the goroutine holds the payload pointer (from the sync.Pool)
		<-c
		w.putCh(c)
		return errors.E(op, errors.ExecTTL, err)
	case res := <-c:
		w.putCh(c)
		if res.err != nil {
			return res.err
		}
		return nil
	}
}

// Exec executes payload with TTL timeout in the context.
func (w *Process) Exec(ctx context.Context, p *payload.Payload) (*payload.Payload, error) {
	const op = errors.Op("worker_exec_with_timeout")

	// worker was killed before it started to work (supervisor)
	if !w.State().Compare(fsm.StateReady) {
		return nil, errors.E(op, errors.Retry, errors.Errorf("Process is not ready (%s)", w.State().String()))
	}

	c := w.getCh()
	// set last used time
	w.State().SetLastUsed(uint64(time.Now().UnixNano()))
	w.State().Transition(fsm.StateWorking)

	go func() {
		err := w.sendFrame(p)
		if err != nil {
			c <- &wexec{
				err: err,
			}
			runtime.Goexit()
		}

		w.State().RegisterExec()
		rsp, err := w.receiveFrame()
		if err != nil {
			c <- &wexec{
				payload: rsp,
				err:     err,
			}

			runtime.Goexit()
		}

		c <- &wexec{
			payload: rsp,
		}
	}()

	select {
	// exec TTL reached
	case <-ctx.Done():
		errK := w.Kill()
		err := stderr.Join(errK, ctx.Err())
		// we should wait for the exit from the worker
		// 'c' channel here should return an error or nil
		// because the goroutine holds the payload pointer (from the sync.Pool)
		<-c
		w.putCh(c)
		return nil, errors.E(op, errors.ExecTTL, err)
	case res := <-c:
		w.putCh(c)
		if res.err != nil {
			return nil, res.err
		}
		return res.payload, nil
	}
}

// Stop sends soft termination command to the Process and waits for process completion.
func (w *Process) Stop() error {
	const op = errors.Op("process_stop")
	w.fsm.Transition(fsm.StateStopping)

	go func() {
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
		w.log.Debug("worker stopped", zap.Int("pid", w.pid))
		return nil
	case <-time.After(time.Second * 10):
		// kill process
		w.log.Warn("worker doesn't respond on stop command, killing process", zap.Int64("PID", w.Pid()))
		_ = w.cmd.Process.Signal(os.Kill)
		w.fsm.Transition(fsm.StateStopped)
		return errors.E(op, errors.Network)
	}
}

// Kill kills underlying process, make sure to call Wait() func to gather
// error log from the stderr. Does not wait for process completion!
func (w *Process) Kill() error {
	w.fsm.Transition(fsm.StateStopping)
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

func (w *Process) SetMaxExecs(maxExecs uint64) {
	w.maxExecs = maxExecs
}

func (w *Process) MaxJobsReached() bool {
	return w.State().NumExecs() >= w.maxExecs
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

// sendFrame sends frame to the worker
func (w *Process) sendFrame(p *payload.Payload) error {
	const op = errors.Op("sync_worker_send_frame")
	// get a frame
	fr := w.getFrame()
	buf := w.get()

	// can be 0 here
	fr.WriteVersion(fr.Header(), frame.Version1)
	fr.WriteFlags(fr.Header(), p.Codec)

	// obtain a buffer
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
		w.putFrame(fr)
		return errors.E(op, errors.Network, err)
	}
	w.putFrame(fr)
	return nil
}

func (w *Process) receiveFrame() (*payload.Payload, error) {
	const op = errors.Op("sync_worker_receive_frame")

	frameR := w.getFrame()

	err := w.Relay().Receive(frameR)
	if err != nil {
		w.putFrame(frameR)
		return nil, errors.E(op, errors.Network, err)
	}

	if frameR == nil {
		w.putFrame(frameR)
		return nil, errors.E(op, errors.Network, errors.Str("nil frame received"))
	}

	codec := frameR.ReadFlags()

	if codec&frame.ERROR != byte(0) {
		// we need to copy the payload because we will put the frame back to the pool
		cp := make([]byte, len(frameR.Payload()))
		copy(cp, frameR.Payload())

		w.putFrame(frameR)
		return nil, errors.E(op, errors.SoftJob, errors.Str(string(cp)))
	}

	options := frameR.ReadOptions(frameR.Header())
	if len(options) != 1 {
		w.putFrame(frameR)
		return nil, errors.E(op, errors.Decode, errors.Str("options length should be equal 1 (body offset)"))
	}

	// bound check
	if len(frameR.Payload()) < int(options[0]) {
		// we need to copy the payload because we will put the frame back to the pool
		cp := make([]byte, len(frameR.Payload()))
		copy(cp, frameR.Payload())

		w.putFrame(frameR)
		return nil, errors.E(errors.Network, errors.Errorf("bad payload %s", cp))
	}

	// stream + stop -> waste
	// stream + ping -> response
	flags := frameR.Header()[10]
	pld := &payload.Payload{
		Flags:   flags,
		Codec:   codec,
		Body:    make([]byte, len(frameR.Payload()[options[0]:])),
		Context: make([]byte, len(frameR.Payload()[:options[0]])),
	}

	// by copying we free frame's payload slice
	// we do not hold the pointer from the smaller slice to the initial (which should be in the sync.Pool)
	// https://blog.golang.org/slices-intro#TOC_6.
	copy(pld.Body, frameR.Payload()[options[0]:])
	copy(pld.Context, frameR.Payload()[:options[0]])

	w.putFrame(frameR)
	return pld, nil
}

func (w *Process) sendPONG() error {
	// get a frame
	fr := w.getFrame()
	fr.WriteVersion(fr.Header(), frame.Version1)

	fr.SetPongBit(fr.Header())
	fr.WriteCRC(fr.Header())

	err := w.Relay().Send(fr)
	w.State().RegisterExec()
	if err != nil {
		w.putFrame(fr)
		w.State().Transition(fsm.StateErrored)
		return errors.E(errors.Network, err)
	}

	w.putFrame(fr)
	return nil
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

func (w *Process) getCh() chan *wexec {
	return w.chPool.Get().(chan *wexec)
}

func (w *Process) putCh(ch chan *wexec) {
	// just check if the chan is not empty
	select {
	case <-ch:
	default:
	}
	w.chPool.Put(ch)
}
