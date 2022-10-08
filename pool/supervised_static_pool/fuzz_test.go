package supervised_static_pool //nolint:stylecheck

import (
	"context"
	"os/exec"
	"testing"
	"time"

	"github.com/roadrunner-server/sdk/v2/ipc/pipe"
	"github.com/roadrunner-server/sdk/v2/payload"
	"github.com/roadrunner-server/sdk/v2/pool"
	"github.com/stretchr/testify/assert"
)

func FuzzStaticPoolEcho(f *testing.F) {
	f.Add([]byte("hello"))

	var cfgSupervisedFuzz = &pool.Config{
		NumWorkers:      uint64(1),
		AllocateTimeout: time.Second,
		DestroyTimeout:  time.Second,
		Supervisor: &pool.SupervisorConfig{
			WatchTick:       1 * time.Second,
			TTL:             100 * time.Second,
			IdleTTL:         100 * time.Second,
			ExecTTL:         100 * time.Second,
			MaxWorkerMemory: 100,
		},
	}

	ctx := context.Background()
	p, err := NewSupervisedPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/client.php", "echo", "pipes") },
		pipe.NewPipeFactory(log),
		cfgSupervisedFuzz,
		log,
	)
	assert.NoError(f, err)
	assert.NotNil(f, p)

	f.Fuzz(func(t *testing.T, data []byte) {
		// data can't be empty
		if len(data) == 0 {
			data = []byte("1")
		}
		res, err := p.Exec(context.Background(), &payload.Payload{Body: data})

		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.NotNil(t, res.Body)
		assert.Empty(t, res.Context)

		assert.Equal(t, data, res.Body)
	})

	p.Destroy(ctx)
}
