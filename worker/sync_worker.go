package worker

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/roadrunner-server/api/v2/payload"
	"github.com/roadrunner-server/api/v2/worker"
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/goridge/v3/pkg/frame"
	"github.com/roadrunner-server/goridge/v3/pkg/relay"
	"github.com/roadrunner-server/sdk/v2/worker/fsm"
	"go.uber.org/multierr"
)

// Allocator is responsible for worker allocation in the pool
type Allocator func() (worker.Worker, error)

type Worker struct {
	process    worker.BaseProcess
	fPool      sync.Pool
	bPool      sync.Pool
	chPool     sync.Pool
	respChPool sync.Pool
}

// From creates SyncWorker from BaseProcess
func From(process worker.BaseProcess) *Worker {
	return &Worker{
		process: process,
		fPool: sync.Pool{New: func() any {
			return frame.NewFrame()
		}},
		bPool: sync.Pool{New: func() any {
			return new(bytes.Buffer)
		}},

		chPool: sync.Pool{New: func() any {
			return make(chan wexec, 1)
		}},
		respChPool: sync.Pool{New: func() any {
			return make(chan *payload.Payload)
		}},
	}
}

// Exec payload without TTL timeout.
func (tw *Worker) Exec(p *payload.Payload) (*payload.Payload, error) {
	const op = errors.Op("sync_worker_exec")

	if len(p.Body) == 0 && len(p.Context) == 0 {
		return nil, errors.E(op, errors.Str("payload can not be empty"))
	}

	if !tw.process.State().Compare(fsm.StateReady) {
		return nil, errors.E(op, errors.Retry, errors.Errorf("Process is not ready (%s)", tw.process.State().String()))
	}

	// set last used time
	tw.process.State().SetLastUsed(uint64(time.Now().UnixNano()))
	tw.process.State().Transition(fsm.StateWorking)

	rsp, err := tw.execPayload(p)
	if err != nil && !errors.Is(errors.Stop, err) {
		// just to be more verbose
		if !errors.Is(errors.SoftJob, err) {
			tw.process.State().Transition(fsm.StateErrored)
			tw.process.State().RegisterExec()
		}
		return nil, errors.E(op, err)
	}

	// supervisor may set state of the worker during the work
	// in this case we should not re-write the worker state
	if !tw.process.State().Compare(fsm.StateWorking) {
		tw.process.State().RegisterExec()
		return rsp, nil
	}

	tw.process.State().Transition(fsm.StateReady)
	tw.process.State().RegisterExec()

	return rsp, nil
}

type wexec struct {
	payload *payload.Payload
	err     error
}

// ExecWithTTL executes payload without TTL timeout.
func (tw *Worker) ExecWithTTL(ctx context.Context, p *payload.Payload) (*payload.Payload, error) {
	const op = errors.Op("sync_worker_exec_worker_with_timeout")

	if len(p.Body) == 0 && len(p.Context) == 0 {
		return nil, errors.E(op, errors.Str("payload can not be empty"))
	}

	c := tw.getCh()
	defer tw.putCh(c)

	// worker was killed before it started to work (supervisor)
	if !tw.process.State().Compare(fsm.StateReady) {
		return nil, errors.E(op, errors.Retry, errors.Errorf("Process is not ready (%s)", tw.process.State().String()))
	}
	// set last used time
	tw.process.State().SetLastUsed(uint64(time.Now().UnixNano()))
	tw.process.State().Transition(fsm.StateWorking)

	go func() {
		rsp, err := tw.execPayload(p)
		if err != nil {
			// just to be more verbose
			if errors.Is(errors.SoftJob, err) == false { //nolint:gosimple
				tw.process.State().Transition(fsm.StateErrored)
				tw.process.State().RegisterExec()
			}
			c <- wexec{
				err: errors.E(op, err),
			}
			return
		}

		if !tw.process.State().Compare(fsm.StateWorking) {
			tw.process.State().RegisterExec()
			c <- wexec{
				payload: rsp,
				err:     nil,
			}
			return
		}

		tw.process.State().Transition(fsm.StateReady)
		tw.process.State().RegisterExec()

		c <- wexec{
			payload: rsp,
			err:     nil,
		}
	}()

	select {
	// exec TTL reached
	case <-ctx.Done():
		errK := tw.Kill()
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

func (tw *Worker) String() string {
	return tw.process.String()
}

func (tw *Worker) Pid() int64 {
	return tw.process.Pid()
}

func (tw *Worker) Created() time.Time {
	return tw.process.Created()
}

func (tw *Worker) State() worker.FSM {
	return tw.process.State()
}

func (tw *Worker) Start() error {
	return tw.process.Start()
}

func (tw *Worker) Wait() error {
	return tw.process.Wait()
}

func (tw *Worker) Stop() error {
	return tw.process.Stop()
}

func (tw *Worker) Kill() error {
	return tw.process.Kill()
}

func (tw *Worker) Relay() relay.Relay {
	return tw.process.Relay()
}

func (tw *Worker) AttachRelay(rl relay.Relay) {
	tw.process.AttachRelay(rl)
}

// Private

func (tw *Worker) execPayload(p *payload.Payload) (*payload.Payload, error) {
	const op = errors.Op("sync_worker_exec_payload")

	// get a frame
	fr := tw.getFrame()
	defer tw.putFrame(fr)

	// can be 0 here
	fr.WriteVersion(fr.Header(), frame.Version1)
	fr.WriteFlags(fr.Header(), p.Codec)

	// obtain a buffer
	buf := tw.get()

	buf.Write(p.Context)
	buf.Write(p.Body)

	// Context offset
	fr.WriteOptions(fr.HeaderPtr(), uint32(len(p.Context)))
	fr.WritePayloadLen(fr.Header(), uint32(buf.Len()))
	fr.WritePayload(buf.Bytes())

	fr.WriteCRC(fr.Header())

	// return buffer
	tw.put(buf)

	err := tw.Relay().Send(fr)
	if err != nil {
		return nil, errors.E(op, errors.Network, err)
	}

	frameR := tw.getFrame()
	defer tw.putFrame(frameR)

	err = tw.process.Relay().Receive(frameR)
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

func (tw *Worker) get() *bytes.Buffer {
	return tw.bPool.Get().(*bytes.Buffer)
}

func (tw *Worker) put(b *bytes.Buffer) {
	b.Reset()
	tw.bPool.Put(b)
}

func (tw *Worker) getFrame() *frame.Frame {
	return tw.fPool.Get().(*frame.Frame)
}

func (tw *Worker) putFrame(f *frame.Frame) {
	f.Reset()
	tw.fPool.Put(f)
}

func (tw *Worker) getCh() chan wexec {
	return tw.chPool.Get().(chan wexec)
}

func (tw *Worker) putCh(ch chan wexec) {
	// just check if the chan is not empty
	select {
	case <-ch:
		tw.chPool.Put(ch)
	default:
		tw.chPool.Put(ch)
	}
}
