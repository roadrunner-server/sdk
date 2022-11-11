package child

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

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

func WithLog(z *zap.Logger) Options {
	return func(p *Process) {
		p.log = z
	}
}

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
	// process args
	args []string

	fPool  sync.Pool
	bPool  sync.Pool
	chPool sync.Pool

	doneCh chan struct{}

	// communication bus with underlying process.
	relay relay.Relay
}

type wexec struct {
	payload *payload.Payload
	err     error
}

// InitChildWorker creates new child PHP Process.
func InitChildWorker(pid int, args []string, options ...Options) (*Process, error) {
	if pid == 0 {
		return nil, fmt.Errorf("can't attach to the base process")
	}

	w := &Process{
		created: time.Now(),
		pid:     pid,
		args:    args,
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
		}}

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

	return w, nil
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
		strings.Join(w.args, " "),
		st,
		w.fsm.NumExecs(),
	)
}

const _P_ALL = 1

// blockUntilWaitable attempts to block until a call to p.Wait will
// succeed immediately, and reports whether it has done so.
// It does not actually call p.Wait.
func (w *Process) blockUntilWaitable(p uintptr) (bool, error) {
	// The waitid system call expects a pointer to a siginfo_t,
	// which is 128 bytes on all Linux systems.
	// On darwin/amd64, it requires 104 bytes.
	// We don't care about the values it returns.
	var siginfo [16]uint64
	psig := &siginfo[0]
	var e syscall.Errno
	for {
		_, _, e = syscall.Syscall6(syscall.SYS_WAITID, _P_ALL, p, uintptr(unsafe.Pointer(psig)), syscall.WUNTRACED, 0, 0)
		if e != syscall.EINTR {
			break
		}
	}
	runtime.KeepAlive(w)
	if e != 0 {
		// waitid has been available since Linux 2.6.9, but
		// reportedly is not available in Ubuntu on Windows.
		// See issue 16610.
		if e == syscall.ENOSYS {
			return false, nil
		}
		return false, os.NewSyscallError("waitid", e)
	}
	return true, nil
}

// Wait must be called once for each Process, call will be released once Process is
// complete and will return process error (if any), if stderr is presented it is value
// will be wrapped as WorkerError. Method will return error code if php process fails
// to find or Start the script.
func (w *Process) Wait() error {
	// pidfd_open
	const SYS_PIDFD_OPEN = 434

	var (
		status syscall.WaitStatus
		rusage syscall.Rusage
		pid1   int
		err    error
	)
	for {
		pid1, err = syscall.Wait4(w.pid, &status, 0, &rusage)
		if err != syscall.EINTR {
			break
		}
	}
	if err != nil {
		return os.NewSyscallError("wait", err)
	}

	_ = pid1

	//fd, _, errno := syscall.Syscall(SYS_PIDFD_OPEN, uintptr(w.pid), uintptr(0), uintptr(0))
	//if errno != 0 {
	//	return os.NewSyscallError("SYS_PIDFD_OPEN", errno)
	//}
	//
	//res, err := w.blockUntilWaitable(fd)
	//if err != nil {
	//	return os.NewSyscallError("wait", err)
	//}
	//
	//_ = res

	w.State().Transition(fsm.StateStopped)
	w.doneCh <- struct{}{}

	// If worker was destroyed, just exit
	if w.State().Compare(fsm.StateDestroyed) {
		return nil
	}

	// closeRelay
	// at this point according to the documentation (see cmd.Wait comment)
	// if worker finishes with an error, message will be written to the stderr first
	// and then process.cmd.Wait return an error
	err2 := w.closeRelay()
	if err2 != nil {
		w.State().Transition(fsm.StateErrored)
		return multierr.Append(err, err2)
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
		_ = syscall.Kill(w.pid, syscall.SIGKILL)
		w.fsm.Transition(fsm.StateStopped)
		return errors.E(op, errors.Network)
	}
}

// Kill kills underlying process, make sure to call Wait() func to gather
// error log from the stderr. Does not wait for process completion!
func (w *Process) Kill() error {
	w.fsm.Transition(fsm.StateKilling)
	err := syscall.Kill(w.pid, syscall.SIGKILL)
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
