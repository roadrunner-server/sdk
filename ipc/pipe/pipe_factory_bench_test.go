package pipe

import (
	"context"
	"os/exec"
	"testing"

	"github.com/roadrunner-server/sdk/v4/payload"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func Benchmark_WorkerPipeTTL(b *testing.B) {
	cmd := exec.Command("php", "../../tests/client.php", "echo", "pipes")
	ctx := context.Background()

	log, _ = zap.NewDevelopment()
	w, err := NewPipeFactory(log).SpawnWorkerWithContext(ctx, cmd)
	require.NoError(b, err)

	go func() {
		_ = w.Wait()
	}()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		res, err := w.Exec(ctx, &payload.Payload{Body: []byte("hello")})
		assert.NoError(b, err)
		assert.NotNil(b, res)
	}

	b.Cleanup(func() {
		assert.NoError(b, w.Stop())
	})
}
