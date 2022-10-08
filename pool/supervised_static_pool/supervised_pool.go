package supervised_static_pool //nolint:stylecheck

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/sdk/v2/events"
	"github.com/roadrunner-server/sdk/v2/payload"
	"github.com/roadrunner-server/sdk/v2/pool"
	"github.com/roadrunner-server/sdk/v2/pool/err_actions"
	"github.com/roadrunner-server/sdk/v2/state/process"
	"github.com/roadrunner-server/sdk/v2/utils"
	"github.com/roadrunner-server/sdk/v2/worker"
	"github.com/roadrunner-server/sdk/v2/worker/fsm"
	workerWatcher "github.com/roadrunner-server/sdk/v2/worker_watcher"
	"go.uber.org/zap"
)

const (
	MB = 1024 * 1024
)

// NSEC_IN_SEC nanoseconds in second
const NSEC_IN_SEC int64 = 1000000000 //nolint:stylecheck

// Pool controls worker creation, destruction and task routing. Pool uses fixed amount of stack.
type Pool struct {
	mu sync.RWMutex
	// pool configuration
	cfg *pool.Config

	// logger
	log *zap.Logger

	// worker command creator
	cmd pool.Command

	// creates and connects to stack
	factory pool.Factory

	// manages worker states and TTLs
	ww *workerWatcher.WorkerWatcher

	// allocate new worker
	allocator pool.Allocator

	// exec queue size
	queue uint64

	stopCh chan struct{}
}

// NewSupervisedPool creates new worker pool and task multiplexer. Pool will initiate with one worker by default
func NewSupervisedPool(ctx context.Context, cmd pool.Command, factory pool.Factory, cfg *pool.Config, log *zap.Logger) (*Pool, error) {
	if factory == nil {
		return nil, errors.Str("no factory initialized")
	}

	if cfg == nil {
		return nil, errors.Str("nil configuration provided")
	}

	if cfg.Supervisor == nil {
		return nil, errors.Str("supervised pool initialization without a supervisor configuration")
	}

	cfg.InitDefaults()

	if cfg.Debug {
		cfg.NumWorkers = 0
		cfg.MaxJobs = 1
	}

	p := &Pool{
		cfg:     cfg,
		cmd:     cmd,
		factory: factory,
		log:     log,
		queue:   0,
		stopCh:  make(chan struct{}),
	}

	if p.log == nil {
		var err error
		p.log, err = zap.NewDevelopment()
		if err != nil {
			return nil, err
		}
	}

	// set up workers allocator
	p.allocator = pool.NewPoolAllocator(ctx, p.cfg.AllocateTimeout, factory, cmd, p.cfg.Command, p.log)
	// set up workers watcher
	p.ww = workerWatcher.NewSyncWorkerWatcher(p.allocator, p.log, p.cfg.NumWorkers, p.cfg.AllocateTimeout)

	// allocate requested number of workers
	workers, err := pool.AllocateParallel(p.cfg.NumWorkers, p.allocator)
	if err != nil {
		return nil, err
	}

	// add workers to the watcher
	err = p.ww.Watch(workers)
	if err != nil {
		return nil, err
	}

	// start workers watcher
	p.Start()

	return p, nil
}

// GetConfig returns associated pool configuration. Immutable.
func (sp *Pool) GetConfig() *pool.Config {
	return sp.cfg
}

// Workers returns worker list associated with the pool.
func (sp *Pool) Workers() (workers []*worker.Process) {
	return sp.ww.List()
}

// RemoveWorker function should not be used outside the `Wait` function
func (sp *Pool) RemoveWorker(wb *worker.Process) error {
	sp.ww.Remove(wb)
	return nil
}

// Exec sync with pool.Exec method
func (sp *Pool) Exec(ctx context.Context, p *payload.Payload) (*payload.Payload, error) {
	const op = errors.Op("supervised_static_pool_exec_with_context")
	if sp.cfg.Debug {
		return sp.execDebugWithTTL(ctx, p)
	}

	var cancel context.CancelFunc

	if sp.cfg.Supervisor.ExecTTL > 0 {
		ctx, cancel = context.WithTimeout(ctx, sp.cfg.Supervisor.ExecTTL)
		defer cancel()
	}

	atomic.AddUint64(&sp.queue, 1)
	defer atomic.AddUint64(&sp.queue, ^uint64(0))

	// see notes at the end of the file
begin:
	ctxGetFree, cancelGet := context.WithTimeout(context.Background(), sp.cfg.AllocateTimeout)
	defer cancelGet()

	w, err := sp.takeWorker(ctxGetFree, op)
	if err != nil {
		return nil, errors.E(op, err)
	}

	rsp, err := w.ExecWithTTL(ctx, p)
	if err != nil {
		if errors.Is(errors.Retry, err) {
			sp.ww.Release(w)
			goto begin
		}
		return nil, err_actions.ErrActions(err, sp.ww, w, sp.log, sp.cfg.MaxJobs)
	}

	// worker want's to be terminated
	if len(rsp.Body) == 0 && utils.AsString(rsp.Context) == pool.StopRequest {
		sp.stopWorker(w)
		goto begin
	}

	if sp.cfg.MaxJobs != 0 {
		if w.State().NumExecs() >= sp.cfg.MaxJobs {
			w.State().Transition(fsm.StateMaxJobsReached)
		}
	}
	// return worker back
	sp.ww.Release(w)
	return rsp, nil
}

func (sp *Pool) QueueSize() uint64 {
	return atomic.LoadUint64(&sp.queue)
}

// Destroy all underlying stack (but let them complete the task).
func (sp *Pool) Destroy(ctx context.Context) {
	// stop the watcher
	sp.stopCh <- struct{}{}

	sp.ww.Destroy(ctx)
	atomic.StoreUint64(&sp.queue, 0)
}

func (sp *Pool) Reset(ctx context.Context) error {
	// destroy all workers
	sp.ww.Reset(ctx)
	workers, err := pool.AllocateParallel(sp.cfg.NumWorkers, sp.allocator)
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

func (sp *Pool) stopWorker(w *worker.Process) {
	w.State().Transition(fsm.StateInvalid)
	err := w.Stop()
	if err != nil {
		sp.log.Warn("user requested worker to be stopped", zap.String("reason", "user event"), zap.Int64("pid", w.Pid()), zap.String("internal_event_name", events.EventWorkerError.String()), zap.Error(err))
	}
}

func (sp *Pool) takeWorker(ctxGetFree context.Context, op errors.Op) (*worker.Process, error) {
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

// execDebugWithTTL used when user set debug mode and exec_ttl
func (sp *Pool) execDebugWithTTL(ctx context.Context, p *payload.Payload) (*payload.Payload, error) {
	sw, err := sp.allocator()
	if err != nil {
		return nil, err
	}

	// redirect call to the worker with TTL
	r, err := sw.ExecWithTTL(ctx, p)
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

func (sp *Pool) Start() {
	go func() {
		watchTout := time.NewTicker(sp.cfg.Supervisor.WatchTick)
		defer watchTout.Stop()

		for {
			select {
			case <-sp.stopCh:
				return
			// stop here
			case <-watchTout.C:
				sp.mu.Lock()
				sp.control()
				sp.mu.Unlock()
			}
		}
	}()
}

func (sp *Pool) Stop() {
	sp.stopCh <- struct{}{}
}

func (sp *Pool) control() {
	now := time.Now()

	// MIGHT BE OUTDATED
	// It's a copy of the Workers pointers
	workers := sp.Workers()

	for i := 0; i < len(workers); i++ {
		// if worker not in the Ready OR working state
		// skip such worker
		switch workers[i].State().CurrentState() {
		case
			fsm.StateInvalid,
			fsm.StateErrored,
			fsm.StateDestroyed,
			fsm.StateInactive,
			fsm.StateStopped,
			fsm.StateStopping,
			fsm.StateMaxJobsReached,
			fsm.StateKilling:

			// stop the bad worker
			if workers[i] != nil {
				_ = workers[i].Stop()
			}
			continue
		}

		s, err := process.WorkerProcessState(workers[i])
		if err != nil {
			// worker not longer valid for supervision
			continue
		}

		if sp.cfg.Supervisor.TTL != 0 && now.Sub(workers[i].Created()).Seconds() >= sp.cfg.Supervisor.TTL.Seconds() {
			/*
				worker at this point might be in the middle of request execution:

				---> REQ ---> WORKER -----------------> RESP (at this point we should not set the Ready state) ------> | ----> Worker gets between supervisor checks and get killed in the ww.Release
											 ^
				                           TTL Reached, state - invalid                                                |
																														-----> Worker Stopped here
			*/
			workers[i].State().Transition(fsm.StateInvalid)
			sp.log.Debug("ttl", zap.String("reason", "ttl is reached"), zap.Int64("pid", workers[i].Pid()), zap.String("internal_event_name", events.EventTTL.String()))
			continue
		}

		if sp.cfg.Supervisor.MaxWorkerMemory != 0 && s.MemoryUsage >= sp.cfg.Supervisor.MaxWorkerMemory*MB {
			/*
				worker at this point might be in the middle of request execution:

				---> REQ ---> WORKER -----------------> RESP (at this point we should not set the Ready state) ------> | ----> Worker gets between supervisor checks and get killed in the ww.Release
											 ^
				                           TTL Reached, state - invalid                                                |
																														-----> Worker Stopped here
			*/
			workers[i].State().Transition(fsm.StateInvalid)
			sp.log.Debug("memory_limit", zap.String("reason", "max memory is reached"), zap.Int64("pid", workers[i].Pid()), zap.String("internal_event_name", events.EventMaxMemory.String()))
			continue
		}

		// firs we check maxWorker idle
		if sp.cfg.Supervisor.IdleTTL != 0 {
			// then check for the worker state
			if !workers[i].State().Compare(fsm.StateReady) {
				continue
			}

			/*
				Calculate idle time
				If worker in the StateReady, we read it LastUsed timestamp as UnixNano uint64
				2. For example maxWorkerIdle is equal to 5sec, then, if (time.Now - LastUsed) > maxWorkerIdle
				we are guessing that worker overlap idle time and has to be killed
			*/

			// 1610530005534416045 lu
			// lu - now = -7811150814 - nanoseconds
			// 7.8 seconds
			// get last used unix nano
			lu := workers[i].State().LastUsed()
			// worker not used, skip
			if lu == 0 {
				continue
			}

			// convert last used to unixNano and sub time.now to seconds
			// negative number, because lu always in the past, except for the `back to the future` :)
			res := ((int64(lu) - now.UnixNano()) / NSEC_IN_SEC) * -1

			// maxWorkerIdle more than diff between now and last used
			// for example:
			// After exec worker goes to the rest
			// And resting for the 5 seconds
			// IdleTTL is 1 second.
			// After the control check, res will be 5, idle is 1
			// 5 - 1 = 4, more than 0, YOU ARE FIRED (removed). Done.
			if int64(sp.cfg.Supervisor.IdleTTL.Seconds())-res <= 0 {
				/*
					worker at this point might be in the middle of request execution:

					---> REQ ---> WORKER -----------------> RESP (at this point we should not set the Ready state) ------> | ----> Worker gets between supervisor checks and get killed in the ww.Release
												 ^
					                           TTL Reached, state - invalid                                                |
																															-----> Worker Stopped here
				*/

				workers[i].State().Transition(fsm.StateInvalid)
				sp.log.Debug("idle_ttl", zap.String("reason", "idle ttl is reached"), zap.Int64("pid", workers[i].Pid()), zap.String("internal_event_name", events.EventTTL.String()))
			}
		}
	}
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
