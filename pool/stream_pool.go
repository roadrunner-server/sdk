package pool

import (
	"context"
	"sync/atomic"

	"github.com/roadrunner-server/api/v2/payload"
	"github.com/roadrunner-server/api/v2/worker"
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/sdk/v2/events"
	"go.uber.org/zap"
)

func (sp *Pool) ExecStream(p *payload.Payload, resp chan *payload.Payload, _ chan struct{}) error {
	const op = errors.Op("static_pool_exec")
	if sp.cfg.Debug {
		return sp.execDebugStream(p, resp)
	}

	atomic.AddUint64(&sp.queue, 1)
	defer atomic.AddUint64(&sp.queue, ^uint64(0))

	// see notes at the end of the file
begin:
	ctxGetFree, cancel := context.WithTimeout(context.Background(), sp.cfg.AllocateTimeout)
	defer cancel()
	w, err := sp.takeWorker(ctxGetFree, op)
	if err != nil {
		// only one reason to close the channel is when it did get into the Exec
		close(resp)
		return errors.E(op, err)
	}

	// in the Exec* channel managed by the underlying functions
	err = w.(worker.Streamer).ExecStream(p, resp, nil)
	if err != nil {
		if errors.Is(errors.Retry, err) {
			sp.ww.Release(w)
			goto begin
		}

		// worker requested stop
		if errors.Is(errors.Stop, err) {
			sp.stopWorker(w)
			goto begin
		}

		// other types of errors
		return sp.encodeErr(err, w)
	}

	if sp.cfg.MaxJobs != 0 {
		sp.checkMaxJobs(w)
		return nil
	}

	// return worker back
	sp.ww.Release(w)
	return nil
}

func (sp *Pool) ExecStreamWithTTL(ctx context.Context, p *payload.Payload, resp chan *payload.Payload, _ chan struct{}) error {
	const op = errors.Op("stream_pool_exec_with_context")
	if sp.cfg.Debug {
		return sp.streamExecDebugWithTTL(ctx, p, resp)
	}

	atomic.AddUint64(&sp.queue, 1)
	defer atomic.AddUint64(&sp.queue, ^uint64(0))

	// see notes at the end of the file
begin:
	ctxGetFree, cancel := context.WithTimeout(context.Background(), sp.cfg.AllocateTimeout)
	defer cancel()
	w, err := sp.takeWorker(ctxGetFree, op)
	if err != nil {
		// only one reason to close the channel is when it did get into the Exec
		close(resp)
		return errors.E(op, err)
	}

	// in the Exec* channel managed by the underlying functions
	err = w.(worker.Streamer).ExecStreamWithTTL(ctx, p, resp, nil)
	if err != nil {
		if errors.Is(errors.Retry, err) {
			sp.ww.Release(w)
			goto begin
		}

		// worker requested stop
		if errors.Is(errors.Stop, err) {
			// we don't need to release worker here, because it'll be released by the worker watcher
			sp.stopWorker(w)
			goto begin
		}

		// other types of errors
		return sp.encodeErr(err, w)
	}

	if sp.cfg.MaxJobs != 0 {
		sp.checkMaxJobs(w)
		return nil
	}

	// return worker back
	sp.ww.Release(w)
	return nil
}

// execDebug used when debug mode was not set and exec_ttl is 0
func (sp *Pool) execDebugStream(p *payload.Payload, resp chan *payload.Payload) error {
	sw, err := sp.allocator()
	if err != nil {
		return err
	}

	if _, ok := sw.(worker.Streamer); !ok {
		return errors.Str("executed on stream worker, but stream worker is not initialized")
	}

	// redirect call to the workers' exec method (without ttl)
	err = sw.(worker.Streamer).ExecStream(p, resp, nil)
	if err != nil {
		return err
	}

	go func() {
		// read the exit status to prevent process to be a zombie
		_ = sw.Wait()
	}()

	// destroy the worker
	err = sw.Stop()
	if err != nil {
		sp.log.Debug("debug mode: worker stopped", zap.String("reason", "worker error"), zap.Int64("pid", sw.Pid()), zap.String("internal_event_name", events.EventWorkerError.String()), zap.Error(err))
		return err
	}

	return nil
}

// execDebugWithTTL used when user set debug mode and exec_ttl
func (sp *Pool) streamExecDebugWithTTL(ctx context.Context, p *payload.Payload, resp chan *payload.Payload) error {
	sw, err := sp.allocator()
	if err != nil {
		return err
	}

	// redirect call to the worker with TTL
	err = sw.(worker.Streamer).ExecStreamWithTTL(ctx, p, resp, nil)
	if err != nil {
		return err
	}

	go func() {
		// read the exit status to prevent process to be a zombie
		_ = sw.Wait()
	}()

	err = sw.Stop()
	if err != nil {
		sp.log.Debug("debug mode: worker stopped", zap.String("reason", "worker error"), zap.Int64("pid", sw.Pid()), zap.String("internal_event_name", events.EventWorkerError.String()), zap.Error(err))
		return err
	}

	return nil
}
