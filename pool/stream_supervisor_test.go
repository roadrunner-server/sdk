package pool

import (
	"context"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/roadrunner-server/api/v2/payload"
	"github.com/roadrunner-server/api/v2/pool"
	"github.com/roadrunner-server/api/v2/worker"
	"github.com/roadrunner-server/sdk/v2/ipc/pipe"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamSupervisedPool_Exec(t *testing.T) {
	ctx := context.Background()
	p, err := NewStaticPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../tests/memleak.php", "pipes") },
		pipe.NewPipeFactory(log),
		cfgSupervised,
		log,
	)

	require.NoError(t, err)
	require.NotNil(t, p)
	require.Len(t, p.Workers(), 1)

	pidBefore := p.Workers()[0].Pid()
	wg := &sync.WaitGroup{}

	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)
		resp := make(chan *payload.Payload)
		wg.Add(1)
		go func() {
			defer wg.Done()
			errStr := p.(pool.Streamer).ExecStreamWithTTL(ctx, &payload.Payload{
				Context: []byte(""),
				Body:    []byte("foo"),
			}, resp)
			assert.NoError(t, errStr)
		}()

		for range resp {
		}
	}

	assert.NotEqual(t, pidBefore, p.Workers()[0].Pid())
	wg.Wait()

	p.Destroy(context.Background())
}

// This test should finish without freezes
func TestStreamSupervisedPool_ExecWithDebugMode(t *testing.T) {
	var cfgSupervised = cfgSupervised
	cfgSupervised.Debug = true

	ctx := context.Background()
	p, err := NewStaticPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../tests/supervised.php") },
		pipe.NewPipeFactory(log),
		cfgSupervised,
		log,
	)

	assert.NoError(t, err)
	assert.NotNil(t, p)

	time.Sleep(time.Second)
	wg := &sync.WaitGroup{}

	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)
		resp := make(chan *payload.Payload)
		wg.Add(1)
		go func() {
			defer wg.Done()
			errStr := p.(pool.Streamer).ExecStreamWithTTL(ctx, &payload.Payload{
				Context: []byte(""),
				Body:    []byte("foo"),
			}, resp)
			assert.NoError(t, errStr)
		}()

		for range resp {
		}
	}

	wg.Wait()
	p.Destroy(context.Background())
}

func TestStreamSupervisedPool_ExecTTL_TimedOut(t *testing.T) {
	var cfgExecTTL = &Config{
		NumWorkers:      uint64(1),
		AllocateTimeout: time.Second,
		DestroyTimeout:  time.Second,
		Supervisor: &SupervisorConfig{
			WatchTick:       1 * time.Second,
			TTL:             100 * time.Second,
			IdleTTL:         100 * time.Second,
			ExecTTL:         1 * time.Second,
			MaxWorkerMemory: 100,
		},
	}
	ctx := context.Background()
	p, err := NewStaticPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../tests/sleep.php", "pipes") },
		pipe.NewPipeFactory(log),
		cfgExecTTL,
		log,
	)

	assert.NoError(t, err)
	assert.NotNil(t, p)
	defer p.Destroy(context.Background())

	pid := p.Workers()[0].Pid()

	wg := &sync.WaitGroup{}
	wg.Add(1)
	resp := make(chan *payload.Payload)
	go func() {
		defer wg.Done()
		time.Sleep(time.Millisecond * 100)
		ctxNew, cancel := context.WithTimeout(ctx, time.Second*5)
		defer cancel()
		errStr := p.(pool.Streamer).ExecStreamWithTTL(ctxNew, &payload.Payload{
			Context: []byte(""),
			Body:    []byte("foo"),
		}, resp)
		require.Error(t, errStr)
	}()

	for r := range resp {
		require.Empty(t, r)
	}

	time.Sleep(time.Second * 1)
	// should be new worker with new pid
	assert.NotEqual(t, pid, p.Workers()[0].Pid())
}

func TestStreamSupervisedPool_ExecTTL_WorkerRestarted(t *testing.T) {
	var cfgExecTTL = &Config{
		NumWorkers: uint64(1),
		Supervisor: &SupervisorConfig{
			WatchTick: 1 * time.Second,
			TTL:       5 * time.Second,
		},
	}
	ctx := context.Background()
	p, err := NewStaticPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../tests/sleep-ttl.php") },
		pipe.NewPipeFactory(log),
		cfgExecTTL,
		log,
	)

	assert.NoError(t, err)
	assert.NotNil(t, p)

	pid := p.Workers()[0].Pid()

	wg := &sync.WaitGroup{}
	wg.Add(1)
	respCh1 := make(chan *payload.Payload)
	go func() {
		defer wg.Done()
		errStr := p.(pool.Streamer).ExecStreamWithTTL(ctx, &payload.Payload{
			Context: []byte(""),
			Body:    []byte("foo"),
		}, respCh1)
		require.NoError(t, errStr)
	}()

	for r := range respCh1 {
		assert.Equal(t, string(r.Body), "hello world")
		assert.Empty(t, r.Context)
	}

	wg.Wait()

	time.Sleep(time.Second)
	assert.NotEqual(t, pid, p.Workers()[0].Pid())
	require.Equal(t, p.Workers()[0].State().Value(), worker.StateReady)
	pid = p.Workers()[0].Pid()

	wg.Add(1)
	respCh2 := make(chan *payload.Payload)
	go func() {
		defer wg.Done()
		errStr := p.(pool.Streamer).ExecStreamWithTTL(ctx, &payload.Payload{
			Context: []byte(""),
			Body:    []byte("foo"),
		}, respCh2)
		require.NoError(t, errStr)
	}()

	for r := range respCh2 {
		assert.Equal(t, string(r.Body), "hello world")
		assert.Empty(t, r.Context)
	}

	wg.Wait()

	time.Sleep(time.Second)
	// should be new worker with new pid
	assert.NotEqual(t, pid, p.Workers()[0].Pid())
	require.Equal(t, p.Workers()[0].State().Value(), worker.StateReady)

	p.Destroy(context.Background())
}

func TestStreamSupervisedPool_Idle(t *testing.T) {
	var cfgExecTTL = &Config{
		NumWorkers:      uint64(1),
		AllocateTimeout: time.Second,
		DestroyTimeout:  time.Second,
		Supervisor: &SupervisorConfig{
			WatchTick:       1 * time.Second,
			TTL:             100 * time.Second,
			IdleTTL:         1 * time.Second,
			ExecTTL:         100 * time.Second,
			MaxWorkerMemory: 100,
		},
	}
	ctx := context.Background()
	p, err := NewStaticPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../tests/idle.php", "pipes") },
		pipe.NewPipeFactory(log),
		cfgExecTTL,
		log,
	)

	assert.NoError(t, err)
	assert.NotNil(t, p)

	pid := p.Workers()[0].Pid()

	wg := &sync.WaitGroup{}
	wg.Add(1)
	respCh1 := make(chan *payload.Payload)
	go func() {
		defer wg.Done()
		errStr := p.(pool.Streamer).ExecStream(&payload.Payload{
			Context: []byte(""),
			Body:    []byte("foo"),
		}, respCh1)
		require.NoError(t, errStr)
	}()

	for r := range respCh1 {
		assert.Empty(t, r.Body)
		assert.Empty(t, r.Context)
	}

	wg.Wait()

	time.Sleep(time.Second * 5)

	// worker should be marked as invalid and reallocated
	wg.Add(1)
	respCh2 := make(chan *payload.Payload)
	go func() {
		defer wg.Done()
		errStr := p.(pool.Streamer).ExecStream(&payload.Payload{
			Context: []byte(""),
			Body:    []byte("foo"),
		}, respCh2)
		require.NoError(t, errStr)
	}()

	for r := range respCh2 {
		require.NotNil(t, r)
	}

	wg.Wait()

	time.Sleep(time.Second * 2)
	require.Len(t, p.Workers(), 1)
	// should be new worker with new pid
	assert.NotEqual(t, pid, p.Workers()[0].Pid())
	p.Destroy(context.Background())
}

func TestStreamSupervisedPool_IdleTTL_StateAfterTimeout(t *testing.T) {
	var cfgExecTTL = &Config{
		NumWorkers:      uint64(1),
		AllocateTimeout: time.Second,
		DestroyTimeout:  time.Second,
		Supervisor: &SupervisorConfig{
			WatchTick:       1 * time.Second,
			TTL:             1 * time.Second,
			IdleTTL:         1 * time.Second,
			MaxWorkerMemory: 100,
		},
	}
	ctx := context.Background()
	p, err := NewStaticPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../tests/exec_ttl.php", "pipes") },
		pipe.NewPipeFactory(log),
		cfgExecTTL,
		log,
	)

	assert.NoError(t, err)
	assert.NotNil(t, p)
	defer p.Destroy(context.Background())

	pid := p.Workers()[0].Pid()

	time.Sleep(time.Millisecond * 100)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	respCh1 := make(chan *payload.Payload)
	go func() {
		defer wg.Done()
		errStr := p.(pool.Streamer).ExecStream(&payload.Payload{
			Context: []byte(""),
			Body:    []byte("foo"),
		}, respCh1)
		require.NoError(t, errStr)
	}()

	for r := range respCh1 {
		assert.Empty(t, r.Body)
		assert.Empty(t, r.Context)
	}

	wg.Wait()

	time.Sleep(time.Second * 2)

	if len(p.Workers()) < 1 {
		t.Fatal("should be at least 1 worker")
		return
	}

	// should be destroyed, state should be Ready, not Invalid
	assert.NotEqual(t, pid, p.Workers()[0].Pid())
	assert.Equal(t, int64(3), p.Workers()[0].State().Value())
}

func TestStreamSupervisedPool_ExecTTL_OK(t *testing.T) {
	var cfgExecTTL = &Config{
		NumWorkers:      uint64(1),
		AllocateTimeout: time.Second,
		DestroyTimeout:  time.Second,
		Supervisor: &SupervisorConfig{
			WatchTick:       1 * time.Second,
			TTL:             100 * time.Second,
			IdleTTL:         100 * time.Second,
			ExecTTL:         4 * time.Second,
			MaxWorkerMemory: 100,
		},
	}
	ctx := context.Background()
	p, err := NewStaticPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../tests/exec_ttl.php", "pipes") },
		pipe.NewPipeFactory(log),
		cfgExecTTL,
		log,
	)

	assert.NoError(t, err)
	assert.NotNil(t, p)
	defer p.Destroy(context.Background())

	pid := p.Workers()[0].Pid()

	time.Sleep(time.Millisecond * 100)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	respCh1 := make(chan *payload.Payload)
	go func() {
		defer wg.Done()
		errStr := p.(pool.Streamer).ExecStream(&payload.Payload{
			Context: []byte(""),
			Body:    []byte("foo"),
		}, respCh1)
		require.NoError(t, errStr)
	}()

	for r := range respCh1 {
		assert.Empty(t, r.Body)
		assert.Empty(t, r.Context)
	}

	wg.Wait()

	time.Sleep(time.Second * 1)
	// should be the same pid
	assert.Equal(t, pid, p.Workers()[0].Pid())
}

func TestStreamSupervisedPool_MaxMemoryReached(t *testing.T) {
	var cfgExecTTL = &Config{
		NumWorkers:      uint64(1),
		AllocateTimeout: time.Second,
		DestroyTimeout:  time.Second,
		Supervisor: &SupervisorConfig{
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
	p, err := NewStaticPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../tests/memleak.php", "pipes") },
		pipe.NewPipeFactory(log),
		cfgExecTTL,
		log,
	)

	assert.NoError(t, err)
	assert.NotNil(t, p)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	respCh1 := make(chan *payload.Payload)
	go func() {
		defer wg.Done()
		errStr := p.(pool.Streamer).ExecStream(&payload.Payload{
			Context: []byte(""),
			Body:    []byte("foo"),
		}, respCh1)
		require.NoError(t, errStr)
	}()

	for r := range respCh1 {
		assert.Empty(t, r.Body)
		assert.Empty(t, r.Context)
	}

	wg.Wait()

	time.Sleep(time.Second)
	p.Destroy(context.Background())
}

func TestStreamSupervisedPool_AllocateFailedOK(t *testing.T) {
	var cfgExecTTL = &Config{
		NumWorkers:      uint64(2),
		AllocateTimeout: time.Second * 15,
		DestroyTimeout:  time.Second * 5,
		Supervisor: &SupervisorConfig{
			WatchTick: 1 * time.Second,
			TTL:       5 * time.Second,
		},
	}

	ctx := context.Background()
	p, err := NewStaticPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../tests/allocate-failed.php") },
		pipe.NewPipeFactory(log),
		cfgExecTTL,
		log,
	)

	assert.NoError(t, err)
	require.NotNil(t, p)

	time.Sleep(time.Second)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	respCh1 := make(chan *payload.Payload)
	go func() {
		defer wg.Done()
		errStr := p.(pool.Streamer).ExecStream(&payload.Payload{
			Context: []byte(""),
			Body:    []byte("foo"),
		}, respCh1)
		require.NoError(t, errStr)
	}()

	for range respCh1 {
	}

	wg.Wait()

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
