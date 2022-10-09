package pool

import (
	"context"
	"time"

	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/sdk/v3/events"
	"github.com/roadrunner-server/sdk/v3/worker"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Allocator is responsible for worker allocation in the pool
type Allocator func() (*worker.Process, error)

// NewPoolAllocator initializes a allocator of the workers
func NewPoolAllocator(ctx context.Context, timeout time.Duration, factory Factory, cmd Command, command string, log *zap.Logger) Allocator {
	return func() (*worker.Process, error) {
		ctxT, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		w, err := factory.SpawnWorkerWithTimeout(ctxT, cmd(command))
		if err != nil {
			// context deadline
			if errors.Is(errors.TimeOut, err) {
				return nil, errors.Str("failed to spawn a worker, possible reasons: https://roadrunner.dev/docs/known-issues-allocate-timeout/2.x/en")
			}
			return nil, err
		}

		// wrap sync worker
		log.Debug("worker is allocated", zap.Int64("pid", w.Pid()), zap.String("internal_event_name", events.EventWorkerConstruct.String()))
		return w, nil
	}
}

// AllocateParallel allocate required number of stack
func AllocateParallel(numWorkers uint64, allocator Allocator) ([]*worker.Process, error) {
	const op = errors.Op("static_pool_allocate_workers")

	workers := make([]*worker.Process, numWorkers)
	eg := new(errgroup.Group)

	// constant number of stack simplify logic
	for i := uint64(0); i < numWorkers; i++ {
		ii := i
		eg.Go(func() error {
			w, err := allocator()
			if err != nil {
				return errors.E(op, errors.WorkerAllocate, err)
			}

			workers[ii] = w
			return nil
		})
	}

	err := eg.Wait()
	if err != nil {
		for j := 0; j < len(workers); j++ {
			jj := j
			if workers[jj] != nil {
				go func() {
					_ = workers[jj].Wait()
				}()

				_ = workers[jj].Kill()
			}
		}
		return nil, err
	}

	return workers, nil
}
