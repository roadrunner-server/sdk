package pool

import (
	"context"
	"time"

	"github.com/roadrunner-server/api/v2/ipc"
	"github.com/roadrunner-server/api/v2/worker"
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/sdk/v2/events"
	workerImpl "github.com/roadrunner-server/sdk/v2/worker"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func (sp *Pool) newPoolAllocator(ctx context.Context, timeout time.Duration, factory ipc.Factory, cmd Command) worker.Allocator {
	return func() (worker.BaseProcess, error) {
		ctxT, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		w, err := factory.SpawnWorkerWithTimeout(ctxT, cmd(sp.cfg.Command))
		if err != nil {
			return nil, err
		}

		// wrap sync worker
		sw := workerImpl.From(w)
		sp.log.Debug("worker is allocated", zap.Int64("pid", w.Pid()), zap.String("internal_event_name", events.EventWorkerConstruct.String()))
		return sw, nil
	}
}

// allocate required number of stack
func (sp *Pool) parallelAllocator(numWorkers uint64) ([]worker.BaseProcess, error) {
	const op = errors.Op("static_pool_allocate_workers")

	workers := make([]worker.BaseProcess, numWorkers)
	eg := new(errgroup.Group)

	// constant number of stack simplify logic
	for i := uint64(0); i < numWorkers; i++ {
		ii := i
		eg.Go(func() error {
			w, err := sp.allocator()
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
