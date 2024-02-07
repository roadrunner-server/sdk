package pool

import (
	"context"
	"math"
	"math/rand"
	"os/exec"

	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/sdk/v4/events"
	"github.com/roadrunner-server/sdk/v4/worker"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Factory is responsible for wrapping given command into tasks WorkerProcess.
type Factory interface {
	// SpawnWorkerWithTimeout creates a new WorkerProcess process based on given command with context.
	// Process must not be started.
	SpawnWorkerWithTimeout(context.Context, *exec.Cmd) (*worker.Process, error)
	// SpawnWorker creates a new WorkerProcess process based on given command.
	// Process must not be started.
	SpawnWorker(*exec.Cmd) (*worker.Process, error)
	// Close the factory and underlying connections.
	Close() error
}

// NewPoolAllocator initializes allocator of the workers
func NewPoolAllocator(ctx context.Context, cfg *Config, factory Factory, cmd Command, log *zap.Logger) func() (*worker.Process, error) {
	return func() (*worker.Process, error) {
		ctxT, cancel := context.WithTimeout(ctx, cfg.AllocateTimeout)
		defer cancel()

		w, err := factory.SpawnWorkerWithTimeout(ctxT, cmd(cfg.Command))
		if err != nil {
			// context deadline
			if errors.Is(errors.TimeOut, err) {
				return nil, errors.Str("failed to spawn a worker, possible reasons: https://roadrunner.dev/docs/known-issues-allocate-timeout/2023.x/en")
			}
			return nil, err
		}

		// should be in factory.SpawnWorkerWithTimeout, but this method is missing cfg parameter
		w.SetMaxExecs(calculateMaxExecs(cfg.MaxJobs, cfg.MaxJobsDispersion))

		// wrap sync worker
		log.Debug("worker is allocated", zap.Int64("pid", w.Pid()), zap.String("internal_event_name", events.EventWorkerConstruct.String()))
		return w, nil
	}
}

// AllocateParallel allocate required number of stack
func AllocateParallel(numWorkers uint64, allocator func() (*worker.Process, error)) ([]*worker.Process, error) {
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

// https://github.com/Baldinof/roadrunner-bundle/blob/ed13b632c5c916d81a217d68dec2c7daf81b2631/src/Reboot/MaxJobsRebootStrategy.php#L14
func calculateMaxExecs(maxJobs uint64, dispersion float64) uint64 {
	minJobs := maxJobs - uint64(math.Round(float64(maxJobs)*dispersion)) // (:

	return randomBetween(minJobs, maxJobs)
}

func randomBetween(min, max uint64) uint64 {
	rnd := rand.Uint64()
	rnd %= max - min
	rnd += min
	return rnd
}
