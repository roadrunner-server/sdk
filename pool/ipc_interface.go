package pool

import (
	"context"
	"os/exec"

	"github.com/roadrunner-server/sdk/v3/worker"
	"github.com/roadrunner-server/sdk/v3/worker/child"
)

// Factory is responsible for wrapping given command into tasks WorkerProcess.
type Factory interface {
	// SpawnWorkerWithTimeout creates new WorkerProcess process based on given command with context.
	// Process must not be started.
	SpawnWorkerWithTimeout(context.Context, *exec.Cmd) (*worker.Process, error)
	// SpawnWorker creates new WorkerProcess process based on given command.
	// Process must not be started.
	SpawnWorker(*exec.Cmd) (*worker.Process, error)
	// ForkWorker used to send a fork command to fork the master-process
	ForkWorker(ctx context.Context, process *worker.Process) (*child.Process, error)
	// Close the factory and underlying connections.
	Close() error
}
