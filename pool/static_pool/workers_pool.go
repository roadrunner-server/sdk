package static_pool //nolint:stylecheck

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/sdk/v4/events"
	"github.com/roadrunner-server/sdk/v4/fsm"
	"github.com/roadrunner-server/sdk/v4/payload"
	"github.com/roadrunner-server/sdk/v4/pool"
	"github.com/roadrunner-server/sdk/v4/worker"
	workerWatcher "github.com/roadrunner-server/sdk/v4/worker_watcher"
	"go.uber.org/zap"
)

// Pool controls worker creation, destruction and task routing. Pool uses fixed amount of stack.
type Pool struct {
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
	allocator func() (*worker.Process, error)
	// exec queue size
	queue uint64
	// used in the supervised mode
	supervisedExec bool
	stopCh         chan struct{}
	mu             sync.RWMutex
}

// NewPool creates new worker pool and task multiplexer. Pool will initiate with one worker. If supervisor configuration is provided -> pool will be turned into a supervisedExec mode
func NewPool(ctx context.Context, cmd pool.Command, factory pool.Factory, cfg *pool.Config, log *zap.Logger, options ...Options) (*Pool, error) {
	if factory == nil {
		return nil, errors.Str("no factory initialized")
	}

	if cfg == nil {
		return nil, errors.Str("nil configuration provided")
	}

	cfg.InitDefaults()

	// for debug mode we need to set the number of workers to 0 (no pre-allocated workers) and max jobs to 1
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
	}

	// apply options
	for i := 0; i < len(options); i++ {
		options[i](p)
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

	if p.cfg.Supervisor != nil {
		if p.cfg.Supervisor.ExecTTL != 0 {
			// we use supervisedExec ExecWithTTL mode only when ExecTTL is set
			// otherwise we may use a faster Exec
			p.supervisedExec = true
		}
		// start the supervisor
		p.Start()
	}

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

// Exec executes provided payload on the worker
func (sp *Pool) Exec(ctx context.Context, p *payload.Payload, stopCh chan struct{}) (chan *PExec, error) {
	const op = errors.Op("static_pool_exec")

	if len(p.Body) == 0 && len(p.Context) == 0 {
		return nil, errors.E(op, errors.Str("payload can not be empty"))
	}

	if sp.cfg.Debug {
		switch sp.supervisedExec {
		case true:
			ctxTTL, cancel := context.WithTimeout(ctx, sp.cfg.Supervisor.ExecTTL)
			defer cancel()
			return sp.execDebug(ctxTTL, p)
		case false:
			return sp.execDebug(context.Background(), p)
		}
	}

	/*
		register request in the QUEUE
	*/
	atomic.AddUint64(&sp.queue, 1)
	defer atomic.AddUint64(&sp.queue, ^uint64(0))

	// see notes at the end of the file
begin:
	ctxGetFree, cancel := context.WithTimeout(ctx, sp.cfg.AllocateTimeout)
	defer cancel()
	w, err := sp.takeWorker(ctxGetFree, op)
	if err != nil {
		return nil, errors.E(op, err)
	}

	var rsp *payload.Payload
	switch sp.supervisedExec {
	case true:
		// in the supervisedExec mode we're limiting the allowed time for the execution inside the PHP worker
		ctxT, cancelT := context.WithTimeout(ctx, sp.cfg.Supervisor.ExecTTL)
		defer cancelT()
		rsp, err = w.Exec(ctxT, p)
	case false:
		// no context here
		rsp, err = w.Exec(context.Background(), p)
	}

	if err != nil {
		if errors.Is(errors.Retry, err) {
			sp.ww.Release(w)
			goto begin
		}

		// just push event if on any stage was timeout error
		switch {
		// for this case, worker already killed in the ExecTTL function
		case errors.Is(errors.ExecTTL, err):
			sp.log.Warn("worker stopped, and will be restarted", zap.String("reason", "execTTL timeout elapsed"), zap.Int64("pid", w.Pid()), zap.String("internal_event_name", events.EventExecTTL.String()), zap.Error(err))
			w.State().Transition(fsm.StateInvalid)
			sp.ww.Release(w)

			return nil, err
		case errors.Is(errors.SoftJob, err):
			sp.log.Warn("worker stopped, and will be restarted", zap.String("reason", "worker error"), zap.Int64("pid", w.Pid()), zap.String("internal_event_name", events.EventWorkerError.String()), zap.Error(err))
			// soft jobs errors are allowed, just put the worker back
			if sp.cfg.MaxJobs != 0 && w.State().NumExecs() >= sp.cfg.MaxJobs {
				// mark old as invalid and stop
				w.State().Transition(fsm.StateInvalid)
			}
			sp.ww.Release(w)

			return nil, err
		case errors.Is(errors.Network, err):
			// in case of network error, we can't stop the worker, we should kill it
			w.State().Transition(fsm.StateInvalid)
			sp.log.Warn("network error", zap.String("reason", "network"), zap.Int64("pid", w.Pid()), zap.String("internal_event_name", events.EventWorkerError.String()), zap.Error(err))
			// kill the worker instead of sending net packet to it
			_ = w.Kill()

			return nil, err
		default:
			w.State().Transition(fsm.StateInvalid)
			sp.log.Warn("worker will be restarted", zap.Int64("pid", w.Pid()), zap.String("internal_event_name", events.EventWorkerDestruct.String()), zap.Error(err))

			return nil, err
		}
	}

	if sp.cfg.MaxJobs != 0 {
		if w.State().NumExecs() >= sp.cfg.MaxJobs {
			w.State().Transition(fsm.StateMaxJobsReached)
		}
	}

	// create channel for the stream (only if there are no errors)
	resp := make(chan *PExec, 1)

	switch rsp.IsStream {
	case true:
		// in case of stream we should not return worker back immediately
		go func() {
			// would be called on Goexit
			defer func() {
				close(resp)
				sp.ww.Release(w)
			}()

			// stream iterator
			for {
				select {
				// we received stop signal
				case <-stopCh:
					err = w.StreamCancel()
					if err != nil {
						resp <- newPExec(nil, err)
					}
					runtime.Goexit()
				default:
					pld, next, err := w.StreamIter()
					if err != nil {
						resp <- newPExec(nil, err) // exit from the goroutine
						runtime.Goexit()
					}

					resp <- newPExec(pld, nil)
					if !next {
						runtime.Goexit()
					}
				}
			}
		}()

		return resp, nil
	case false:
		resp <- newPExec(rsp, nil)
		// return worker back
		sp.ww.Release(w)
		return resp, nil
	default:
		panic("workers_pool unreachable!")
	}
}

func (sp *Pool) QueueSize() uint64 {
	return atomic.LoadUint64(&sp.queue)
}

// Destroy all underlying stack (but let them complete the task).
func (sp *Pool) Destroy(ctx context.Context) {
	sp.log.Info("destroy signal received", zap.Duration("timeout", sp.cfg.DestroyTimeout))
	var cancel context.CancelFunc
	_, ok := ctx.Deadline()
	if !ok {
		ctx, cancel = context.WithTimeout(ctx, sp.cfg.DestroyTimeout)
		defer cancel()
	}
	sp.ww.Destroy(ctx)
	atomic.StoreUint64(&sp.queue, 0)
}

func (sp *Pool) Reset(ctx context.Context) error {
	// set timeout
	ctx, cancel := context.WithTimeout(ctx, sp.cfg.ResetTimeout)
	defer cancel()
	// reset all workers
	sp.ww.Reset(ctx)
	// re-allocate all workers
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

func (sp *Pool) takeWorker(ctxGetFree context.Context, op errors.Op) (*worker.Process, error) {
	// Get function consumes context with timeout
	w, err := sp.ww.Take(ctxGetFree)
	if err != nil {
		// if the error is of kind NoFreeWorkers, it means, that we can't get worker from the stack during the allocate timeout
		if errors.Is(errors.NoFreeWorkers, err) {
			sp.log.Error(
				"no free workers in the pool, wait timeout exceed",
				zap.String("reason", "no free workers"),
				zap.String("internal_event_name", events.EventNoFreeWorkers.String()),
				zap.Error(err),
			)
			return nil, errors.E(op, err)
		}
		// else if err not nil - return error
		return nil, errors.E(op, err)
	}
	return w, nil
}

// execDebug used when debug mode was not set and exec_ttl is 0
func (sp *Pool) execDebug(ctx context.Context, p *payload.Payload) (chan *PExec, error) {
	sw, err := sp.allocator()
	if err != nil {
		return nil, err
	}

	go func() {
		// read the exit status to prevent process to be a zombie
		_ = sw.Wait()
	}()

	pld, err := sw.Exec(ctx, p)
	if err != nil {
		return nil, err
	}

	resp := make(chan *PExec, 1)
	resp <- newPExec(pld, nil)
	// destroy the worker
	err = sw.Stop()
	if err != nil {
		sp.log.Debug(
			"debug mode: worker stopped",
			zap.String("reason", "worker error"),
			zap.Int64("pid", sw.Pid()),
			zap.String("internal_event_name", events.EventWorkerError.String()),
			zap.Error(err),
		)
	}

	// we can skip stop error in debug mode. It's more important to send back a response
	return resp, nil
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
