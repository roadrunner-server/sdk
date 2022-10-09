package static_pool //nolint:stylecheck

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/roadrunner-server/sdk/v2/ipc/pipe"
	"github.com/roadrunner-server/sdk/v2/payload"
	"github.com/roadrunner-server/sdk/v2/pool"
	"github.com/roadrunner-server/sdk/v2/worker/fsm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var cfgSupervised = &pool.Config{
	NumWorkers:      uint64(1),
	AllocateTimeout: time.Second * 10,
	DestroyTimeout:  time.Second * 10,
	Supervisor: &pool.SupervisorConfig{
		WatchTick:       1 * time.Second,
		TTL:             100 * time.Second,
		IdleTTL:         100 * time.Second,
		ExecTTL:         100 * time.Second,
		MaxWorkerMemory: 100,
	},
}

func Test_SupervisedPool_Exec(t *testing.T) {
	ctx := context.Background()
	logger, _ := zap.NewDevelopment()
	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/memleak.php", "pipes") },
		pipe.NewPipeFactory(logger),
		cfgSupervised,
		logger,
	)

	require.NoError(t, err)
	require.NotNil(t, p)

	time.Sleep(time.Second)

	pidBefore := p.Workers()[0].Pid()

	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)
		_, err = p.Exec(ctx, &payload.Payload{
			Context: []byte(""),
			Body:    []byte("foo"),
		})
		require.NoError(t, err)
	}

	require.NotEqual(t, pidBefore, p.Workers()[0].Pid())

	ctxNew, cancel := context.WithTimeout(ctx, time.Second)
	p.Destroy(ctxNew)
	cancel()
}

func Test_SupervisedPool_ImmediateDestroy(t *testing.T) {
	ctx := context.Background()

	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/client.php", "echo", "pipes") },
		pipe.NewPipeFactory(log),
		&pool.Config{
			AllocateTimeout: time.Second * 10,
			DestroyTimeout:  time.Second * 10,
			Supervisor: &pool.SupervisorConfig{
				WatchTick:       1 * time.Second,
				TTL:             100 * time.Second,
				IdleTTL:         100 * time.Second,
				ExecTTL:         100 * time.Second,
				MaxWorkerMemory: 100,
			},
		},
		log,
	)
	assert.NoError(t, err)
	assert.NotNil(t, p)

	_, _ = p.Exec(ctx, &payload.Payload{Body: []byte("hello"), Context: nil})

	ctx, cancel := context.WithTimeout(ctx, time.Nanosecond)
	defer cancel()

	p.Destroy(ctx)
}

func Test_SupervisedPool_NilFactory(t *testing.T) {
	ctx := context.Background()
	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/client.php", "echo", "pipes") },
		pipe.NewPipeFactory(log),
		nil,
		log,
	)
	assert.Error(t, err)
	assert.Nil(t, p)
}

func Test_SupervisedPool_NilConfig(t *testing.T) {
	ctx := context.Background()
	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/client.php", "echo", "pipes") },
		nil,
		cfgSupervised,
		log,
	)
	assert.Error(t, err)
	assert.Nil(t, p)
}

func Test_SupervisedPool_RemoveWorker(t *testing.T) {
	ctx := context.Background()

	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/client.php", "echo", "pipes") },
		pipe.NewPipeFactory(log),
		cfgSupervised,
		log,
	)
	assert.NoError(t, err)
	assert.NotNil(t, p)

	_, err = p.Exec(ctx, &payload.Payload{Body: []byte("hello"), Context: nil})
	assert.NoError(t, err)

	wrks := p.Workers()
	for i := 0; i < len(wrks); i++ {
		assert.NoError(t, p.RemoveWorker(wrks[i]))
	}

	_, err = p.Exec(ctx, &payload.Payload{Body: []byte("hello"), Context: nil})
	assert.NoError(t, err)

	assert.Len(t, p.Workers(), 0)

	p.Destroy(ctx)
}

func Test_SupervisedPoolReset(t *testing.T) {
	ctx := context.Background()
	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/client.php", "echo", "pipes") },
		pipe.NewPipeFactory(log),
		cfgSupervised,
		log,
	)
	assert.NoError(t, err)
	assert.NotNil(t, p)

	w := p.Workers()
	if len(w) == 0 {
		t.Fatal("should be workers inside")
	}

	pid := w[0].Pid()
	require.NoError(t, p.Reset(context.Background()))

	w2 := p.Workers()
	if len(w2) == 0 {
		t.Fatal("should be workers inside")
	}

	require.NotEqual(t, pid, w2[0].Pid())
	p.Destroy(ctx)
}

// This test should finish without freezes
func TestSupervisedPool_ExecWithDebugMode(t *testing.T) {
	ctx := context.Background()
	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/supervised.php") },
		pipe.NewPipeFactory(log),
		&pool.Config{
			Debug:           true,
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
		},
		log,
	)

	assert.NoError(t, err)
	assert.NotNil(t, p)

	time.Sleep(time.Second)

	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)
		_, err = p.Exec(ctx, &payload.Payload{
			Context: []byte(""),
			Body:    []byte("foo"),
		})
		assert.NoError(t, err)
	}

	p.Destroy(context.Background())
}

func TestSupervisedPool_ExecTTL_TimedOut(t *testing.T) {
	var cfgExecTTL = &pool.Config{
		NumWorkers:      uint64(1),
		AllocateTimeout: time.Second,
		DestroyTimeout:  time.Second,
		Supervisor: &pool.SupervisorConfig{
			WatchTick:       1 * time.Second,
			TTL:             100 * time.Second,
			IdleTTL:         100 * time.Second,
			ExecTTL:         1 * time.Second,
			MaxWorkerMemory: 100,
		},
	}
	ctx := context.Background()
	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/sleep.php", "pipes") },
		pipe.NewPipeFactory(log),
		cfgExecTTL,
		log,
	)

	assert.NoError(t, err)
	assert.NotNil(t, p)
	defer p.Destroy(context.Background())

	pid := p.Workers()[0].Pid()

	resp, err := p.Exec(ctx, &payload.Payload{
		Context: []byte(""),
		Body:    []byte("foo"),
	})

	assert.Error(t, err)
	assert.Empty(t, resp)

	time.Sleep(time.Second * 1)
	// should be new worker with new pid
	assert.NotEqual(t, pid, p.Workers()[0].Pid())
}

func TestSupervisedPool_TTL_WorkerRestarted(t *testing.T) {
	var cfgExecTTL = &pool.Config{
		NumWorkers: uint64(1),
		Supervisor: &pool.SupervisorConfig{
			WatchTick: 1 * time.Second,
			TTL:       5 * time.Second,
		},
	}
	ctx := context.Background()
	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/sleep-ttl.php") },
		pipe.NewPipeFactory(log),
		cfgExecTTL,
		log,
	)

	assert.NoError(t, err)
	assert.NotNil(t, p)

	pid := p.Workers()[0].Pid()

	resp, err := p.Exec(ctx, &payload.Payload{
		Context: []byte(""),
		Body:    []byte("foo"),
	})

	assert.NoError(t, err)
	assert.Equal(t, string(resp.Body), "hello world")
	assert.Empty(t, resp.Context)

	time.Sleep(time.Second)
	assert.NotEqual(t, pid, p.Workers()[0].Pid())
	require.Equal(t, p.Workers()[0].State().CurrentState(), fsm.StateReady)
	pid = p.Workers()[0].Pid()

	resp, err = p.Exec(ctx, &payload.Payload{
		Context: []byte(""),
		Body:    []byte("foo"),
	})

	assert.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, string(resp.Body), "hello world")
	assert.Empty(t, resp.Context)

	time.Sleep(time.Second)
	// should be new worker with new pid
	assert.NotEqual(t, pid, p.Workers()[0].Pid())
	require.Equal(t, p.Workers()[0].State().CurrentState(), fsm.StateReady)

	p.Destroy(context.Background())
}

func TestSupervisedPool_Idle(t *testing.T) {
	var cfgExecTTL = &pool.Config{
		NumWorkers:      uint64(1),
		AllocateTimeout: time.Second,
		DestroyTimeout:  time.Second,
		Supervisor: &pool.SupervisorConfig{
			WatchTick:       1 * time.Second,
			TTL:             100 * time.Second,
			IdleTTL:         1 * time.Second,
			ExecTTL:         100 * time.Second,
			MaxWorkerMemory: 100,
		},
	}
	ctx := context.Background()
	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/idle.php", "pipes") },
		pipe.NewPipeFactory(log),
		cfgExecTTL,
		log,
	)

	assert.NoError(t, err)
	assert.NotNil(t, p)

	pid := p.Workers()[0].Pid()

	resp, err := p.Exec(ctx, &payload.Payload{
		Context: []byte(""),
		Body:    []byte("foo"),
	})

	assert.NoError(t, err)
	assert.Empty(t, resp.Body)
	assert.Empty(t, resp.Context)

	time.Sleep(time.Second * 5)

	// worker should be marked as invalid and reallocated
	rsp, err := p.Exec(ctx, &payload.Payload{
		Context: []byte(""),
		Body:    []byte("foo"),
	})
	assert.NoError(t, err)
	require.NotNil(t, rsp)
	time.Sleep(time.Second * 2)
	require.Len(t, p.Workers(), 1)
	// should be new worker with new pid
	assert.NotEqual(t, pid, p.Workers()[0].Pid())
	p.Destroy(context.Background())
}

func TestSupervisedPool_IdleTTL_StateAfterTimeout(t *testing.T) {
	var cfgExecTTL = &pool.Config{
		NumWorkers:      uint64(1),
		AllocateTimeout: time.Second,
		DestroyTimeout:  time.Second,
		Supervisor: &pool.SupervisorConfig{
			WatchTick:       1 * time.Second,
			IdleTTL:         1 * time.Second,
			MaxWorkerMemory: 100,
		},
	}
	ctx := context.Background()
	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/exec_ttl.php", "pipes") },
		pipe.NewPipeFactory(log),
		cfgExecTTL,
		log,
	)

	assert.NoError(t, err)
	assert.NotNil(t, p)

	pid := p.Workers()[0].Pid()

	time.Sleep(time.Millisecond * 100)
	resp, err := p.Exec(ctx, &payload.Payload{
		Context: []byte(""),
		Body:    []byte("foo"),
	})

	assert.NoError(t, err)
	assert.Empty(t, resp.Body)
	assert.Empty(t, resp.Context)

	time.Sleep(time.Second * 5)

	if len(p.Workers()) < 1 {
		t.Fatal("should be at least 1 worker")
		return
	}

	// should be destroyed, state should be Ready, not Invalid
	assert.NotEqual(t, pid, p.Workers()[0].Pid())
	assert.Equal(t, fsm.StateReady, p.Workers()[0].State().CurrentState())
	p.Destroy(context.Background())
}

func TestSupervisedPool_ExecTTL_OK(t *testing.T) {
	var cfgExecTTL = &pool.Config{
		NumWorkers:      uint64(1),
		AllocateTimeout: time.Second,
		DestroyTimeout:  time.Second,
		Supervisor: &pool.SupervisorConfig{
			WatchTick:       1 * time.Second,
			TTL:             100 * time.Second,
			IdleTTL:         100 * time.Second,
			ExecTTL:         4 * time.Second,
			MaxWorkerMemory: 100,
		},
	}
	ctx := context.Background()
	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/exec_ttl.php", "pipes") },
		pipe.NewPipeFactory(log),
		cfgExecTTL,
		log,
	)

	assert.NoError(t, err)
	assert.NotNil(t, p)
	defer p.Destroy(context.Background())

	pid := p.Workers()[0].Pid()

	time.Sleep(time.Millisecond * 100)
	resp, err := p.Exec(ctx, &payload.Payload{
		Context: []byte(""),
		Body:    []byte("foo"),
	})

	assert.NoError(t, err)
	assert.Empty(t, resp.Body)
	assert.Empty(t, resp.Context)

	time.Sleep(time.Second * 1)
	// should be the same pid
	assert.Equal(t, pid, p.Workers()[0].Pid())
}

func TestSupervisedPool_MaxMemoryReached(t *testing.T) {
	var cfgExecTTL = &pool.Config{
		NumWorkers:      uint64(1),
		AllocateTimeout: time.Second,
		DestroyTimeout:  time.Second,
		Supervisor: &pool.SupervisorConfig{
			WatchTick:       1 * time.Second,
			TTL:             100 * time.Second,
			IdleTTL:         100 * time.Second,
			ExecTTL:         4 * time.Second,
			MaxWorkerMemory: 1,
		},
	}

	// constructed
	// max memory
	// constructed
	ctx := context.Background()
	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/memleak.php", "pipes") },
		pipe.NewPipeFactory(log),
		cfgExecTTL,
		log,
	)

	assert.NoError(t, err)
	assert.NotNil(t, p)

	resp, err := p.Exec(ctx, &payload.Payload{
		Context: []byte(""),
		Body:    []byte("foo"),
	})

	assert.NoError(t, err)
	assert.Empty(t, resp.Body)
	assert.Empty(t, resp.Context)

	time.Sleep(time.Second)
	p.Destroy(context.Background())
}

func Test_SupervisedPool_FastCancel(t *testing.T) {
	ctx := context.Background()
	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/sleep.php") },
		pipe.NewPipeFactory(log),
		cfgSupervised,
		log,
	)
	assert.NoError(t, err)
	defer p.Destroy(ctx)

	assert.NotNil(t, p)

	newCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	res, err := p.Exec(newCtx, &payload.Payload{Body: []byte("hello")})

	assert.Error(t, err)
	assert.Nil(t, res)
	assert.Contains(t, err.Error(), "context deadline exceeded")
}

func Test_SupervisedPool_AllocateFailedOK(t *testing.T) {
	var cfgExecTTL = &pool.Config{
		NumWorkers:      uint64(2),
		AllocateTimeout: time.Second * 15,
		DestroyTimeout:  time.Second * 5,
		Supervisor: &pool.SupervisorConfig{
			WatchTick: 1 * time.Second,
			TTL:       5 * time.Second,
		},
	}

	ctx := context.Background()
	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/allocate-failed.php") },
		pipe.NewPipeFactory(log),
		cfgExecTTL,
		log,
	)

	assert.NoError(t, err)
	require.NotNil(t, p)

	time.Sleep(time.Second)

	// should be ok
	_, err = p.Exec(ctx, &payload.Payload{
		Context: []byte(""),
		Body:    []byte("foo"),
	})

	require.NoError(t, err)

	// after creating this file, PHP will fail
	file, err := os.Create("break")
	require.NoError(t, err)

	time.Sleep(time.Second * 5)
	assert.NoError(t, file.Close())
	assert.NoError(t, os.Remove("break"))

	defer func() {
		if r := recover(); r != nil {
			assert.Fail(t, "panic should not be fired!")
		} else {
			p.Destroy(context.Background())
		}
	}()
}

func Test_SupervisedPool_NoFreeWorkers(t *testing.T) {
	ctx := context.Background()

	p, err := NewPool(
		ctx,
		// sleep for the 3 seconds
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/sleep.php", "pipes") },
		pipe.NewPipeFactory(log),
		&pool.Config{
			Debug:           false,
			NumWorkers:      1,
			AllocateTimeout: time.Second,
			DestroyTimeout:  time.Second,
			Supervisor:      &pool.SupervisorConfig{},
		},
		log,
	)
	assert.NoError(t, err)
	assert.NotNil(t, p)

	go func() {
		ctxNew, cancel := context.WithTimeout(ctx, time.Second*5)
		defer cancel()
		_, _ = p.Exec(ctxNew, &payload.Payload{Body: []byte("hello")})
	}()

	time.Sleep(time.Second)
	res, err := p.Exec(ctx, &payload.Payload{Body: []byte("hello")})
	assert.Error(t, err)
	assert.Nil(t, res)

	time.Sleep(time.Second)

	p.Destroy(ctx)
}
