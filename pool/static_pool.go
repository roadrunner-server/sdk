package pool

import (
	"context"
	"os/exec"
	"sync/atomic"

	"github.com/roadrunner-server/api/v2/ipc"
	"github.com/roadrunner-server/api/v2/payload"
	"github.com/roadrunner-server/api/v2/pool"
	"github.com/roadrunner-server/api/v2/worker"
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/sdk/v2/events"
	"github.com/roadrunner-server/sdk/v2/utils"
	workerWatcher "github.com/roadrunner-server/sdk/v2/worker_watcher"
	"go.uber.org/zap"
)

const (
	// StopRequest can be sent by worker to indicate that restart is required.
	StopRequest = `{"stop":true}`
)

// ErrorEncoder encode error or make a decision based on the error type
type ErrorEncoder func(err error, w worker.BaseProcess) (*payload.Payload, error)

type Options func(p *StaticPool)

type Command func(cmd string) *exec.Cmd

// StaticPool controls worker creation, destruction and task routing. Pool uses fixed amount of stack.
type StaticPool struct {
	cfg *Config
	log *zap.Logger

	// worker command creator
	cmd Command

	// creates and connects to stack
	factory ipc.Factory

	// manages worker states and TTLs
	ww worker.Watcher

	// allocate new worker
	allocator worker.Allocator

	// errEncoder is the default Exec error encoder
	errEncoder ErrorEncoder

	// exec queue size
	queue uint64
}

// NewStaticPool creates new worker pool and task multiplexer. StaticPool will initiate with one worker.
func NewStaticPool(ctx context.Context, cmd Command, factory ipc.Factory, conf interface{}, log *zap.Logger) (pool.Pool, error) {
	if factory == nil {
		return nil, errors.Str("no factory initialized")
	}

	cfg := conf.(*Config)

	cfg.InitDefaults()

	if cfg.Debug {
		cfg.NumWorkers = 0
		cfg.MaxJobs = 1
	}

	p := &StaticPool{
		cfg:     cfg,
		cmd:     cmd,
		factory: factory,
		log:     log,
		queue:   0,
	}

	p.errEncoder = defaultErrEncoder(p)

	if p.log == nil {
		z, err := zap.NewProduction()
		if err != nil {
			return nil, err
		}

		p.log = z
	}

	// set up workers allocator
	p.allocator = p.newPoolAllocator(ctx, p.cfg.AllocateTimeout, factory, cmd)
	// set up workers watcher
	p.ww = workerWatcher.NewSyncWorkerWatcher(p.allocator, p.log, p.cfg.NumWorkers, p.cfg.AllocateTimeout)

	// allocate requested number of workers
	workers, err := p.parallelAllocator(p.cfg.NumWorkers)
	if err != nil {
		return nil, err
	}

	// add workers to the watcher
	err = p.ww.Watch(workers)
	if err != nil {
		return nil, err
	}

	// if supervised config not nil, guess, that pool wanted to be supervised
	if cfg.Supervisor != nil {
		sp := supervisorWrapper(p, p.log, p.cfg.Supervisor)
		// start watcher timer
		sp.Start()
		return sp, nil
	}

	return p, nil
}

// GetConfig returns associated pool configuration. Immutable.
func (sp *StaticPool) GetConfig() interface{} {
	return sp.cfg
}

// Workers returns worker list associated with the pool.
func (sp *StaticPool) Workers() (workers []worker.BaseProcess) {
	return sp.ww.List()
}

func (sp *StaticPool) RemoveWorker(wb worker.BaseProcess) error {
	sp.ww.Remove(wb)
	return nil
}

// Exec executes provided payload on the worker
func (sp *StaticPool) Exec(p *payload.Payload) (*payload.Payload, error) {
	const op = errors.Op("static_pool_exec")
	if sp.cfg.Debug {
		return sp.execDebug(p)
	}

	atomic.AddUint64(&sp.queue, 1)
	defer atomic.AddUint64(&sp.queue, ^uint64(0))

	// see notes at the end of the file
begin:
	ctxGetFree, cancel := context.WithTimeout(context.Background(), sp.cfg.AllocateTimeout)
	defer cancel()
	w, err := sp.takeWorker(ctxGetFree, op)
	if err != nil {
		return nil, errors.E(op, err)
	}

	rsp, err := w.(worker.SyncWorker).Exec(p)
	if err != nil {
		if errors.Is(errors.Retry, err) {
			sp.ww.Release(w)
			goto begin
		}
		return sp.errEncoder(err, w)
	}

	// worker want's to be terminated
	if len(rsp.Body) == 0 && utils.AsString(rsp.Context) == StopRequest {
		sp.stopWorker(w)
		goto begin
	}

	if sp.cfg.MaxJobs != 0 {
		sp.checkMaxJobs(w)
		return rsp, nil
	}
	// return worker back
	sp.ww.Release(w)
	return rsp, nil
}

// ExecWithTTL sync with pool.Exec method
func (sp *StaticPool) ExecWithTTL(ctx context.Context, p *payload.Payload) (*payload.Payload, error) {
	const op = errors.Op("static_pool_exec_with_context")
	if sp.cfg.Debug {
		return sp.execDebugWithTTL(ctx, p)
	}

	atomic.AddUint64(&sp.queue, 1)
	defer atomic.AddUint64(&sp.queue, ^uint64(0))

	// see notes at the end of the file
begin:
	ctxGetFree, cancel := context.WithTimeout(context.Background(), sp.cfg.AllocateTimeout)
	defer cancel()
	w, err := sp.takeWorker(ctxGetFree, op)
	if err != nil {
		return nil, errors.E(op, err)
	}

	rsp, err := w.(worker.SyncWorker).ExecWithTTL(ctx, p)
	if err != nil {
		if errors.Is(errors.Retry, err) {
			sp.ww.Release(w)
			goto begin
		}
		return sp.errEncoder(err, w)
	}

	// worker want's to be terminated
	if len(rsp.Body) == 0 && utils.AsString(rsp.Context) == StopRequest {
		sp.stopWorker(w)
		goto begin
	}

	if sp.cfg.MaxJobs != 0 {
		sp.checkMaxJobs(w)
		return rsp, nil
	}

	// return worker back
	sp.ww.Release(w)
	return rsp, nil
}

func (sp *StaticPool) QueueSize() uint64 {
	return atomic.LoadUint64(&sp.queue)
}

// Destroy all underlying stack (but let them complete the task).
func (sp *StaticPool) Destroy(ctx context.Context) {
	sp.ww.Destroy(ctx)
	atomic.StoreUint64(&sp.queue, 0)
}

func (sp *StaticPool) Reset(ctx context.Context) error {
	// destroy all workers
	sp.ww.Reset(ctx)
	workers, err := sp.parallelAllocator(sp.cfg.NumWorkers)
	if err != nil {
		return err
	}
	// add the NEW workers to the watcher
	err = sp.ww.Watch(workers)
	if err != nil {
		return err
	}

	return nil
}

func defaultErrEncoder(sp *StaticPool) ErrorEncoder {
	return func(err error, w worker.BaseProcess) (*payload.Payload, error) {
		// just push event if on any stage was timeout error
		switch {
		case errors.Is(errors.ExecTTL, err):
			sp.log.Warn("worker stopped, and will be restarted", zap.String("reason", "execTTL timeout elapsed"), zap.Int64("pid", w.Pid()), zap.String("internal_event_name", events.EventExecTTL.String()), zap.Error(err))
			w.State().Set(worker.StateInvalid)
			return nil, err

		case errors.Is(errors.SoftJob, err):
			sp.log.Warn("worker stopped, and will be restarted", zap.String("reason", "worker error"), zap.Int64("pid", w.Pid()), zap.String("internal_event_name", events.EventWorkerError.String()), zap.Error(err))
			// if max jobs exceed
			if sp.cfg.MaxJobs != 0 && w.State().NumExecs() >= sp.cfg.MaxJobs {
				// mark old as invalid and stop
				w.State().Set(worker.StateInvalid)
				errS := w.Stop()
				if errS != nil {
					return nil, errors.E(errors.SoftJob, errors.Errorf("err: %v\nerrStop: %v", err, errS))
				}

				return nil, err
			}

			// soft jobs errors are allowed, just put the worker back
			sp.ww.Release(w)

			return nil, err
		case errors.Is(errors.Network, err):
			// in case of network error, we can't stop the worker, we should kill it
			w.State().Set(worker.StateInvalid)
			sp.log.Warn("network error, worker will be restarted", zap.String("reason", "network"), zap.Int64("pid", w.Pid()), zap.String("internal_event_name", events.EventWorkerError.String()), zap.Error(err))
			// kill the worker instead of sending net packet to it
			_ = w.Kill()

			return nil, err
		default:
			w.State().Set(worker.StateInvalid)
			sp.log.Warn("worker will be restarted", zap.Int64("pid", w.Pid()), zap.String("internal_event_name", events.EventWorkerDestruct.String()), zap.Error(err))
			// stop the worker, worker here might be in the broken state (network)
			errS := w.Stop()
			if errS != nil {
				return nil, errors.E(errors.Errorf("err: %v\nerrStop: %v", err, errS))
			}

			return nil, err
		}
	}
}

func (sp *StaticPool) stopWorker(w worker.BaseProcess) {
	w.State().Set(worker.StateInvalid)
	err := w.Stop()
	if err != nil {
		sp.log.Warn("user requested worker to be stopped", zap.String("reason", "user event"), zap.Int64("pid", w.Pid()), zap.String("internal_event_name", events.EventWorkerError.String()), zap.Error(err))
	}
}

// checkMaxJobs check for worker number of executions and kill workers if that number more than sp.cfg.MaxJobs
func (sp *StaticPool) checkMaxJobs(w worker.BaseProcess) {
	if w.State().NumExecs() >= sp.cfg.MaxJobs {
		w.State().Set(worker.StateMaxJobsReached)
	}

	sp.ww.Release(w)
}

func (sp *StaticPool) takeWorker(ctxGetFree context.Context, op errors.Op) (worker.BaseProcess, error) {
	// Get function consumes context with timeout
	w, err := sp.ww.Take(ctxGetFree)
	if err != nil {
		// if the error is of kind NoFreeWorkers, it means, that we can't get worker from the stack during the allocate timeout
		if errors.Is(errors.NoFreeWorkers, err) {
			sp.log.Error("no free workers in the pool, wait timeout exceed", zap.String("reason", "no free workers"), zap.String("internal_event_name", events.EventNoFreeWorkers.String()), zap.Error(err))
			return nil, errors.E(op, err)
		}
		// else if err not nil - return error
		return nil, errors.E(op, err)
	}
	return w, nil
}

// execDebug used when debug mode was not set and exec_ttl is 0
func (sp *StaticPool) execDebug(p *payload.Payload) (*payload.Payload, error) {
	sw, err := sp.allocator()
	if err != nil {
		return nil, err
	}

	// redirect call to the workers' exec method (without ttl)
	r, err := sw.(worker.Worker).Exec(p)
	if err != nil {
		return nil, err
	}

	go func() {
		// read the exit status to prevent process to be a zombie
		_ = sw.Wait()
	}()

	// destroy the worker
	err = sw.Stop()
	if err != nil {
		sp.log.Debug("debug mode: worker stopped", zap.String("reason", "worker error"), zap.Int64("pid", sw.Pid()), zap.String("internal_event_name", events.EventWorkerError.String()), zap.Error(err))
		return nil, err
	}

	return r, nil
}

// execDebugWithTTL used when user set debug mode and exec_ttl
func (sp *StaticPool) execDebugWithTTL(ctx context.Context, p *payload.Payload) (*payload.Payload, error) {
	sw, err := sp.allocator()
	if err != nil {
		return nil, err
	}

	// redirect call to the worker with TTL
	r, err := sw.(worker.Worker).ExecWithTTL(ctx, p)
	if err != nil {
		return nil, err
	}

	go func() {
		// read the exit status to prevent process to be a zombie
		_ = sw.Wait()
	}()

	err = sw.Stop()
	if err != nil {
		sp.log.Debug("debug mode: worker stopped", zap.String("reason", "worker error"), zap.Int64("pid", sw.Pid()), zap.String("internal_event_name", events.EventWorkerError.String()), zap.Error(err))
		return nil, err
	}

	return r, err
}

/*
Difference between recursion function call vs goto:

RECURSION:
        0x0000 00000 (main.go:15)       TEXT    "".foo(SB), ABIInternal, $24-16
        0x0000 00000 (main.go:15)       CMPQ    SP, 16(R14)
        0x0004 00004 (main.go:15)       JLS     57
        0x0006 00006 (main.go:15)       SUBQ    $24, SP
        0x000a 00010 (main.go:15)       MOVQ    BP, 16(SP)
        0x000f 00015 (main.go:15)       LEAQ    16(SP), BP
        0x0014 00020 (main.go:16)       CALL    "".foo2(SB)
        0x0019 00025 (main.go:17)       TESTQ   AX, AX
        0x001c 00028 (main.go:17)       JEQ     47
        0x001e 00030 (main.go:18)       LEAQ    go.string."bar"(SB), AX
        0x0025 00037 (main.go:18)       MOVL    $3, BX
        0x002a 00042 (main.go:18)       CALL    "".foo(SB)
        0x002f 00047 (main.go:20)       MOVQ    16(SP), BP
        0x0034 00052 (main.go:20)       ADDQ    $24, SP
        0x0038 00056 (main.go:20)       RET
        0x0039 00057 (main.go:20)       NOP
        0x0039 00057 (main.go:15)       CALL    runtime.morestack_noctxt(SB)
        0x003e 00062 (main.go:15)       NOP
        0x0040 00064 (main.go:15)       JMP     0

GOTO:
        0x0000 00000 (main.go:15)       TEXT    "".foo(SB), ABIInternal, $8-16
        0x0000 00000 (main.go:15)       CMPQ    SP, 16(R14)
        0x0004 00004 (main.go:15)       JLS     37
        0x0006 00006 (main.go:15)       SUBQ    $8, SP
        0x000a 00010 (main.go:15)       MOVQ    BP, (SP)
        0x000e 00014 (main.go:15)       LEAQ    (SP), BP
        0x0012 00018 (main.go:17)       CALL    "".foo2(SB)
        0x0017 00023 (main.go:18)       TESTQ   AX, AX
        0x001a 00026 (main.go:18)       JNE     18
        0x001c 00028 (main.go:21)       MOVQ    (SP), BP
        0x0020 00032 (main.go:21)       ADDQ    $8, SP
        0x0024 00036 (main.go:21)       RET
        0x0025 00037 (main.go:21)       NOP
        0x0025 00037 (main.go:15)       CALL    runtime.morestack_noctxt(SB)
        0x002a 00042 (main.go:15)       JMP     0

The difference:
1. Stack (24-16 vs 8-16)
2. JNE vs Direct CALL
3 TESTQ   AX, AX (if err != nil)

So, the goto is a little bit more performant
BenchmarkRecursion
BenchmarkRecursion-32    	378020806	         3.185 ns/op	       0 B/op	       0 allocs/op
BenchmarkGoTo
BenchmarkGoTo-32         	407569994	         2.930 ns/op	       0 B/op	       0 allocs/op
*/
