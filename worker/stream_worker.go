package worker

import (
	"context"
	"time"

	"github.com/roadrunner-server/api/v2/payload"
	"github.com/roadrunner-server/api/v2/worker"
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/goridge/v3/pkg/frame"
	"github.com/roadrunner-server/sdk/v2/utils"
	"go.uber.org/multierr"
)

const (
	// StopRequest can be sent by worker to indicate that restart is required.
	// should be in sync with pool
	StopRequest = `{"stop":true}`
)

func (tw *Worker) ExecStream(p *payload.Payload, resp chan *payload.Payload) error {
	const op = errors.Op("sync_worker_exec")

	if len(p.Body) == 0 && len(p.Context) == 0 {
		return errors.E(op, errors.Str("payload can not be empty"))
	}

	if tw.process.State().Value() != worker.StateReady {
		return errors.E(op, errors.Retry, errors.Errorf("Process is not ready (%s)", tw.process.State().String()))
	}

	// set last used time
	tw.process.State().SetLastUsed(uint64(time.Now().UnixNano()))
	tw.process.State().Set(worker.StateWorking)

	err := tw.execStreamPayload(p, resp)
	if err != nil && !errors.Is(errors.Stop, err) {
		// just to be more verbose
		if !errors.Is(errors.SoftJob, err) {
			tw.process.State().Set(worker.StateErrored)
			tw.process.State().RegisterExec()
		}
		return errors.E(op, err)
	}

	// supervisor may set state of the worker during the work
	// in this case we should not re-write the worker state
	if tw.process.State().Value() != worker.StateWorking {
		tw.process.State().RegisterExec()
		// can be stop request here
		return err
	}

	tw.process.State().Set(worker.StateReady)
	tw.process.State().RegisterExec()

	// can be stop request here
	return err
}

func (tw *Worker) ExecStreamWithTTL(ctx context.Context, p *payload.Payload, resp chan *payload.Payload) error {
	const op = errors.Op("sync_worker_exec_worker_with_timeout")

	if len(p.Body) == 0 && len(p.Context) == 0 {
		return errors.E(op, errors.Str("payload can not be empty"))
	}

	c := tw.getCh()
	defer tw.putCh(c)

	// worker was killed before it started to work (supervisor)
	if tw.process.State().Value() != worker.StateReady {
		return errors.E(op, errors.Retry, errors.Errorf("Process is not ready (%s)", tw.process.State().String()))
	}
	// set last used time
	tw.process.State().SetLastUsed(uint64(time.Now().UnixNano()))
	tw.process.State().Set(worker.StateWorking)

	go func() {
		err := tw.execStreamPayload(p, resp)
		if err != nil && !errors.Is(errors.Stop, err) {
			// just to be more verbose
			if errors.Is(errors.SoftJob, err) == false { //nolint:gosimple
				tw.process.State().Set(worker.StateErrored)
				tw.process.State().RegisterExec()
			}
			c <- wexec{
				err: err,
			}
			return
		}

		if tw.process.State().Value() != worker.StateWorking {
			tw.process.State().RegisterExec()
			c <- wexec{
				err: err,
			}
			return
		}

		tw.process.State().Set(worker.StateReady)
		tw.process.State().RegisterExec()

		c <- wexec{
			err: err,
		}
	}()

	select {
	// exec TTL reached
	case <-ctx.Done():
		err := multierr.Combine(tw.Kill())
		if err != nil {
			// append timeout error
			err = multierr.Append(err, errors.E(op, errors.ExecTTL))
			return multierr.Append(err, ctx.Err())
		}
		return errors.E(op, errors.ExecTTL, ctx.Err())
	case res := <-c:
		if res.err != nil {
			// nil or stop request
			return res.err
		}
		return nil
	}
}

func (tw *Worker) execStreamPayload(p *payload.Payload, resp chan *payload.Payload) error {
	const op = errors.Op("streamer_worker_exec_payload")

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
		close(resp)
		return errors.E(op, errors.Network, err)
	}

	frameR := tw.getFrame()
	defer tw.putFrame(frameR)

stream:
	err = tw.process.Relay().Receive(frameR)
	if err != nil {
		close(resp)
		return errors.E(op, errors.Network, err)
	}
	if frameR == nil {
		close(resp)
		return errors.E(op, errors.Network, errors.Str("nil frame received"))
	}

	flags := frameR.ReadFlags()

	if flags&frame.ERROR != byte(0) {
		close(resp)
		return errors.E(op, errors.SoftJob, errors.Str(string(frameR.Payload())))
	}

	options := frameR.ReadOptions(frameR.Header())
	if len(options) != 1 {
		close(resp)
		return errors.E(op, errors.Decode, errors.Str("options length should be equal 1 (body offset)"))
	}

	// bound check
	if len(frameR.Payload()) < int(options[0]) {
		return errors.E(errors.Network, errors.Errorf("bad payload %s", frameR.Payload()))
	}

	if frameR.IsStream(frameR.Header()) {
		// worker requested stop
		// worker want's to be terminated

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

		resp <- pld

		frameR.Reset()
		goto stream
	}

	// worker requested stop
	// worker want's to be terminated
	if len(frameR.Payload()[options[0]:]) == 0 && utils.AsString(frameR.Payload()[:options[0]]) == StopRequest {
		// do not close the channel here
		return errors.E(errors.Stop)
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

	resp <- pld

	close(resp)
	return nil
}
