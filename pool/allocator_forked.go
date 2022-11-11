package pool

import (
	"context"
	"time"

	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/sdk/v3/events"
	"github.com/roadrunner-server/sdk/v3/worker"
	"go.uber.org/zap"
)

// NewForkedWorkersAllocator initializes an allocator of the workers
func NewForkedWorkersAllocator(ctx context.Context, timeout time.Duration, factory Factory, cmd Command, command string, log *zap.Logger) Allocator {
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
