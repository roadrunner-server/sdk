package static_pool //nolint:stylecheck

import (
	"context"
	"os/exec"
	"testing"

	"github.com/roadrunner-server/api/v2/payload"
	"github.com/roadrunner-server/sdk/v2/ipc/pipe"
	"github.com/stretchr/testify/assert"
)

func FuzzStaticPoolEcho(f *testing.F) {
	f.Add([]byte("hello"))

	ctx := context.Background()
	p, err := NewStaticPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/client.php", "echo", "pipes") },
		pipe.NewPipeFactory(log),
		testCfg,
		log,
	)
	assert.NoError(f, err)
	assert.NotNil(f, p)

	f.Fuzz(func(t *testing.T, data []byte) {
		// data can't be empty
		if len(data) == 0 {
			data = []byte("1")
		}
		res, err := p.Exec(&payload.Payload{Body: data})

		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.NotNil(t, res.Body)
		assert.Empty(t, res.Context)

		assert.Equal(t, data, res.Body)
	})

	p.Destroy(ctx)
}
