package worker_watcher //nolint:stylecheck

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/roadrunner-server/sdk/v3/pool"
	"github.com/roadrunner-server/sdk/v3/worker"

	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/sdk/v3/events"
	"github.com/roadrunner-server/sdk/v3/fsm"
	"github.com/roadrunner-server/sdk/v3/utils"
	"github.com/roadrunner-server/sdk/v3/worker_watcher/container/channel"
	"go.uber.org/zap"
)

type WorkerWatcher struct {
	sync.RWMutex
	// actually don't have a lot of impl here, so interface not needed
	container *channel.Vec
	// used to control Destroy stage (that all workers are in the container)
	numWorkers *uint64
	eventBus   *events.Bus

	workers map[int64]*worker.Process

	log *zap.Logger

	allocator       pool.Allocator
	allocateTimeout time.Duration
}

// NewSyncWorkerWatcher is a constructor for the Watcher
func NewSyncWorkerWatcher(allocator pool.Allocator, log *zap.Logger, numWorkers uint64, allocateTimeout time.Duration) *WorkerWatcher {
	eb, _ := events.NewEventBus()
	return &WorkerWatcher{
		container: channel.NewVector(numWorkers),

		log:      log,
		eventBus: eb,
		// pass a ptr to the number of workers to avoid blocking in the TTL loop
		numWorkers:      utils.Uint64(numWorkers),
		allocateTimeout: allocateTimeout,
		//workers:         make([]*worker.Process, 0, numWorkers),
		workers: make(map[int64]*worker.Process, numWorkers),

		allocator: allocator,
	}
}

func (ww *WorkerWatcher) Watch(workers []*worker.Process) error {
	ww.Lock()
	defer ww.Unlock()
	for i := 0; i < len(workers); i++ {
		ii := i
		ww.container.Push(workers[ii])
		// add worker to watch slice
		ww.workers[workers[ii].Pid()] = workers[ii]
		ww.addToWatch(workers[ii])
	}
	return nil
}

// Take is not a thread safe operation
func (ww *WorkerWatcher) Take(ctx context.Context) (*worker.Process, error) {
	const op = errors.Op("worker_watcher_get_free_worker")
	// we need lock here to prevent Pop operation when ww in the resetting state
	// thread safe operation
	w, err := ww.container.Pop(ctx)

	if err != nil {
		if errors.Is(errors.WatcherStopped, err) {
			return nil, errors.E(op, errors.WatcherStopped)
		}

		return nil, errors.E(op, err)
	}

	// fast path, worker not nil and in the ReadyState
	if w.State().Compare(fsm.StateReady) {
		return w, nil
	}

	// =========================================================
	// SLOW PATH
	_ = w.Kill()
	// no free workers in the container or worker not in the ReadyState (TTL-ed)
	// try to continuously get free one
	for {
		w, err = ww.container.Pop(ctx)
		if err != nil {
			if errors.Is(errors.WatcherStopped, err) {
				return nil, errors.E(op, errors.WatcherStopped)
			}
			return nil, errors.E(op, err)
		}

		switch w.State().CurrentState() {
		// return only workers in the Ready state
		// check first
		case fsm.StateReady:
			return w, nil
		case fsm.StateWorking: // how??
			ww.container.Push(w) // put it back, let worker finish the work
			continue
		default:
			// worker doing no work because it in the container
			// so we can safely kill it (inconsistent state)
			_ = w.Stop()
			// try to get new worker
			continue
		}
	}
}

func (ww *WorkerWatcher) Allocate() error {
	const op = errors.Op("worker_watcher_allocate_new")

	sw, err := ww.allocator()
	if err != nil {
		// log incident
		ww.log.Error("allocate", zap.Error(err))
		// if no timeout, return error immediately
		if ww.allocateTimeout == 0 {
			return errors.E(op, errors.WorkerAllocate, err)
		}

		// every second
		allocateFreq := time.NewTicker(time.Millisecond * 1000)

		tt := time.After(ww.allocateTimeout)
		for {
			select {
			case <-tt:
				// reduce number of workers
				atomic.AddUint64(ww.numWorkers, ^uint64(0))
				allocateFreq.Stop()
				// timeout exceed, worker can't be allocated
				return errors.E(op, errors.WorkerAllocate, err)

			case <-allocateFreq.C:
				sw, err = ww.allocator()
				if err != nil {
					// log incident
					ww.log.Error("allocate retry attempt failed", zap.String("internal_event_name", events.EventWorkerError.String()), zap.Error(err))
					continue
				}

				// reallocated
				allocateFreq.Stop()
				goto done
			}
		}
	}

done:
	// add worker to Wait
	ww.addToWatch(sw)

	ww.Lock()
	// add new worker to the workers slice (to get information about workers in parallel)
	ww.workers[sw.Pid()] = sw
	ww.Unlock()

	// push the worker to the container
	ww.Release(sw)
	return nil
}

// Remove worker
func (ww *WorkerWatcher) Remove(wb *worker.Process) {
	ww.Lock()
	defer ww.Unlock()

	// worker will be removed on the Get operation
	delete(ww.workers, wb.Pid())
}

// Release O(1) operation
func (ww *WorkerWatcher) Release(w *worker.Process) {
	switch w.State().CurrentState() {
	case fsm.StateReady:
		ww.container.Push(w)
	case
		// all the possible wrong states, when we can send a stop signal
		fsm.StateInactive,
		fsm.StateDestroyed,
		fsm.StateErrored,
		fsm.StateInvalid,
		fsm.StateMaxJobsReached:

		err := w.Stop()
		if err != nil {
			ww.log.Debug("worker release", zap.Error(err))
		}
	default:
		// in all other cases, we have no choice rather than kill the worker
		_ = w.Kill()
	}
}

func (ww *WorkerWatcher) Reset(ctx context.Context) {
	ww.Lock()
	// do not release new workers
	ww.container.Reset()
	ww.Unlock()

	tt := time.NewTicker(time.Millisecond * 10)
	defer tt.Stop()
	for {
		select {
		case <-tt.C:
			ww.RLock()

			// that might be one of the workers is working
			// to proceed, all workers should be inside a channel
			if atomic.LoadUint64(ww.numWorkers) != uint64(ww.container.Len()) {
				ww.RUnlock()
				continue
			}
			ww.RUnlock()
			// All workers at this moment are in the container
			// Pop operation is blocked, push can't be done, since it's not possible to pop
			ww.Lock()

			// drain channel
			ww.container.Drain()

			wg := &sync.WaitGroup{}
			wg.Add(len(ww.workers))

			for _, v := range ww.workers {
				v := v
				go func() {
					defer wg.Done()
					v.State().Transition(fsm.StateDestroyed)
					// kill the worker
					_ = v.Stop()
				}()
			}

			wg.Wait()
			// optimization, only 1 call
			for k := range ww.workers {
				delete(ww.workers, k)
			}

			ww.container.ResetDone()
			ww.Unlock()
			return
		case <-ctx.Done():
			// drain channel
			ww.container.Drain()
			// kill workers
			ww.Lock()
			// drain workers slice
			wg := &sync.WaitGroup{}
			wg.Add(len(ww.workers))

			for _, v := range ww.workers {
				go func(w *worker.Process) {
					defer wg.Done()
					w.State().Transition(fsm.StateDestroyed)
					// kill the worker
					_ = w.Stop()
				}(v)
			}

			wg.Wait()
			// optimization, only 1 call
			for k := range ww.workers {
				delete(ww.workers, k)
			}

			ww.container.ResetDone()
			ww.Unlock()
			return
		}
	}
}

// Destroy all underlying container (but let them complete the task)
func (ww *WorkerWatcher) Destroy(ctx context.Context) {
	ww.Lock()
	// do not release new workers
	ww.container.Destroy()
	ww.Unlock()

	tt := time.NewTicker(time.Millisecond * 10)
	// destroy container, we don't use ww mutex here, since we should be able to push worker
	defer tt.Stop()
	for {
		select {
		case <-tt.C:
			ww.RLock()
			// that might be one of the workers is working
			if atomic.LoadUint64(ww.numWorkers) != uint64(ww.container.Len()) {
				ww.RUnlock()
				continue
			}

			ww.RUnlock()
			// All container at this moment are in the container
			// Pop operation is blocked, push can't be done, since it's not possible to pop

			ww.Lock()
			// drain channel, will not actually pop, only drain a channel
			_, _ = ww.container.Pop(ctx)
			wg := &sync.WaitGroup{}
			wg.Add(len(ww.workers))
			for _, v := range ww.workers {
				go func(w *worker.Process) {
					defer wg.Done()
					w.State().Transition(fsm.StateDestroyed)
					// kill the worker
					_ = w.Stop()
				}(v)
			}

			wg.Wait()
			// optimization, only 1 call
			for k := range ww.workers {
				delete(ww.workers, k)
			}

			ww.Unlock()
			return
		case <-ctx.Done():
			// drain channel
			_, _ = ww.container.Pop(ctx)
			// kill workers
			ww.Lock()
			wg := &sync.WaitGroup{}
			wg.Add(len(ww.workers))
			for _, v := range ww.workers {
				v := v
				go func() {
					defer wg.Done()
					v.State().Transition(fsm.StateDestroyed)
					// kill the worker
					_ = v.Stop()
				}()
			}

			wg.Wait()
			// optimization, only 1 call
			for k := range ww.workers {
				delete(ww.workers, k)
			}

			ww.Unlock()
			return
		}
	}
}

// List - this is O(n) operation, and it will return copy of the actual workers
func (ww *WorkerWatcher) List() []*worker.Process {
	ww.RLock()
	defer ww.RUnlock()

	if len(ww.workers) == 0 {
		return nil
	}

	base := make([]*worker.Process, 0, len(ww.workers))

	for _, v := range ww.workers {
		base = append(base, v)
	}

	return base
}

func (ww *WorkerWatcher) wait(w *worker.Process) {
	const op = errors.Op("worker_watcher_wait")
	err := w.Wait()
	if err != nil {
		ww.log.Debug("worker stopped", zap.String("internal_event_name", events.EventWorkerWaitExit.String()), zap.Error(err))
	}

	// remove worker
	ww.Remove(w)

	if w.State().Compare(fsm.StateDestroyed) {
		// worker was manually destroyed, no need to replace
		ww.log.Debug("worker destroyed", zap.Int64("pid", w.Pid()), zap.String("internal_event_name", events.EventWorkerDestruct.String()), zap.Error(err))
		return
	}

	err = ww.Allocate()
	if err != nil {
		ww.log.Error("failed to allocate the worker", zap.String("internal_event_name", events.EventWorkerError.String()), zap.Error(err))

		// no workers at all, panic
		ww.Lock()
		defer ww.Unlock()

		if len(ww.workers) == 0 && atomic.LoadUint64(ww.numWorkers) == 0 {
			panic(errors.E(op, errors.WorkerAllocate, errors.Errorf("can't allocate workers: %v, no workers in the pool", err)))
		}
	}

	// this event used mostly for the temporal plugin
	ww.eventBus.Send(events.NewEvent(events.EventWorkerStopped, "worker_watcher", fmt.Sprintf("process exited, pid: %d", w.Pid())))
}

func (ww *WorkerWatcher) addToWatch(wb *worker.Process) {
	go func() {
		ww.wait(wb)
	}()
}
