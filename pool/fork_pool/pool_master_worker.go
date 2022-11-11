package fork_pool //nolint:stylecheck

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/sdk/v3/events"
	"github.com/roadrunner-server/sdk/v3/fsm"
	"github.com/roadrunner-server/sdk/v3/pool"
	"github.com/roadrunner-server/sdk/v3/worker"
	"github.com/roadrunner-server/sdk/v3/worker/child"
	workerWatcher "github.com/roadrunner-server/sdk/v3/worker_watcher"
	"go.uber.org/zap"
)

// MasterWorker controls worker creation, destruction and task routing. Pool uses fixed amount of stack.
type MasterWorker struct {
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

	// used in the supervised mode
	mu sync.RWMutex
}

// NewMasterWorker creates new worker pool and task multiplexer. Pool will initiate with one worker. If supervisor configuration is provided -> pool will be turned into a supervisedExec mode
func NewMasterWorker(ctx context.Context, cmd pool.Command, factory pool.Factory, cfg *pool.Config, log *zap.Logger) (*MasterWorker, error) {
	if factory == nil {
		return nil, errors.Str("no factory initialized")
	}

	if cfg == nil {
		return nil, errors.Str("nil configuration provided")
	}

	cfg.InitDefaults()

	cfg.NumWorkers = 1

	p := &MasterWorker{
		cfg:     cfg,
		cmd:     cmd,
		factory: factory,
		log:     log,
		queue:   0,
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
			//p.supervisedExec = true
		}
		// start the supervisor
		//p.Start()
	}

	return p, nil
}

func (sp *MasterWorker) Fork(ctx context.Context) (*child.Process, error) {
	w, err := sp.takeWorker(ctx)
	defer sp.ww.Release(w)
	if err != nil {
		return nil, err
	}

	return sp.factory.ForkWorker(ctx, w)
}

// Destroy all underlying stack (but let them complete the task).
func (sp *MasterWorker) Destroy(ctx context.Context) {
	sp.ww.Destroy(ctx)
	atomic.StoreUint64(&sp.queue, 0)
}

func (sp *MasterWorker) Reset(ctx context.Context) error {
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

func (sp *MasterWorker) stopWorker(w *worker.Process) {
	w.State().Transition(fsm.StateInvalid)
	err := w.Stop()
	if err != nil {
		sp.log.Warn("user requested worker to be stopped", zap.String("reason", "user event"), zap.Int64("pid", w.Pid()), zap.String("internal_event_name", events.EventWorkerError.String()), zap.Error(err))
	}
}

func (sp *MasterWorker) takeWorker(ctxGetFree context.Context) (*worker.Process, error) {
	// Get function consumes context with timeout
	w, err := sp.ww.Take(ctxGetFree)
	if err != nil {
		// if the error is of kind NoFreeWorkers, it means, that we can't get worker from the stack during the allocate timeout
		if errors.Is(errors.NoFreeWorkers, err) {
			sp.log.Error("no free workers in the pool, wait timeout exceed", zap.String("reason", "no free workers"), zap.String("internal_event_name", events.EventNoFreeWorkers.String()), zap.Error(err))
			return nil, err
		}
		// else if err not nil - return error
		return nil, err
	}
	return w, nil
}
