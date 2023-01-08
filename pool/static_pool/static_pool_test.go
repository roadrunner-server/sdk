package static_pool //nolint:stylecheck

import (
	"context"
	l "log"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/sdk/v4/fsm"
	"github.com/roadrunner-server/sdk/v4/ipc/pipe"
	"github.com/roadrunner-server/sdk/v4/payload"
	"github.com/roadrunner-server/sdk/v4/pool"
	"github.com/roadrunner-server/sdk/v4/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var testCfg = &pool.Config{
	NumWorkers:      uint64(runtime.NumCPU()),
	AllocateTimeout: time.Second * 500,
	DestroyTimeout:  time.Second * 500,
}

var log = zap.NewNop()

func Test_NewPool(t *testing.T) {
	ctx := context.Background()
	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/client.php", "echo", "pipes") },
		pipe.NewPipeFactory(log),
		testCfg,
		log,
	)
	assert.NoError(t, err)

	defer p.Destroy(ctx)

	assert.NotNil(t, p)
}

func Test_StaticPool_NilFactory(t *testing.T) {
	ctx := context.Background()
	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/client.php", "echo", "pipes") },
		nil,
		testCfg,
		log,
	)
	assert.Error(t, err)
	assert.Nil(t, p)
}

func Test_StaticPool_NilConfig(t *testing.T) {
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

func Test_StaticPool_ImmediateDestroy(t *testing.T) {
	ctx := context.Background()

	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/client.php", "echo", "pipes") },
		pipe.NewPipeFactory(log),
		testCfg,
		log,
	)
	assert.NoError(t, err)
	assert.NotNil(t, p)

	_, _ = p.Exec(ctx, &payload.Payload{Body: []byte("hello"), Context: nil})

	ctx, cancel := context.WithTimeout(ctx, time.Nanosecond)
	defer cancel()

	p.Destroy(ctx)
}

func Test_StaticPool_RemoveWorker(t *testing.T) {
	ctx := context.Background()

	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/client.php", "echo", "pipes") },
		pipe.NewPipeFactory(log),
		testCfg,
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

func Test_Poll_Reallocate(t *testing.T) {
	var testCfg2 = &pool.Config{
		NumWorkers:      1,
		AllocateTimeout: time.Second * 500,
		DestroyTimeout:  time.Second * 500,
	}

	logDev, err := zap.NewDevelopment()
	require.NoError(t, err)

	ctx := context.Background()
	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/client.php", "echo", "pipes") },
		pipe.NewPipeFactory(log),
		testCfg2,
		logDev,
	)
	require.NoError(t, err)
	require.NotNil(t, p)

	wg := sync.WaitGroup{}
	wg.Add(1)

	require.NoError(t, os.Rename("../../tests/client.php", "../../tests/client.bak"))

	go func() {
		for i := 0; i < 50; i++ {
			time.Sleep(time.Millisecond * 100)
			_, errResp := p.Exec(ctx, &payload.Payload{Body: []byte("hello"), Context: nil})
			require.NoError(t, errResp)
		}
		wg.Done()
	}()

	_ = p.Workers()[0].Kill()

	time.Sleep(time.Second * 5)
	require.NoError(t, os.Rename("../../tests/client.bak", "../../tests/client.php"))

	wg.Wait()

	t.Cleanup(func() {
		p.Destroy(ctx)
	})
}

func Test_NewPoolReset(t *testing.T) {
	ctx := context.Background()

	var testCfg2 = &pool.Config{
		NumWorkers:      1,
		AllocateTimeout: time.Second * 500,
		DestroyTimeout:  time.Second * 500,
	}

	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/client.php", "echo", "pipes") },
		pipe.NewPipeFactory(log),
		testCfg2,
		log,
	)
	assert.NoError(t, err)
	assert.NotNil(t, p)

	w := p.Workers()
	if len(w) == 0 {
		t.Fatal("should be workers inside")
	}
	pid := w[0].Pid()

	pld, err := p.Exec(ctx, &payload.Payload{Body: []byte("hello"), Context: nil})
	require.NoError(t, err)
	require.NotNil(t, pld.Body)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for i := 0; i < 100; i++ {
			time.Sleep(time.Millisecond * 10)
			pldG, errG := p.Exec(ctx, &payload.Payload{Body: []byte("hello"), Context: nil})
			require.NoError(t, errG)
			require.NotNil(t, pldG.Body)
		}

		wg.Done()
	}()

	require.NoError(t, p.Reset(context.Background()))

	pld, err = p.Exec(ctx, &payload.Payload{Body: []byte("hello"), Context: nil})
	require.NoError(t, err)
	require.NotNil(t, pld.Body)

	w2 := p.Workers()
	if len(w2) == 0 {
		t.Fatal("should be workers inside")
	}

	require.NotEqual(t, pid, w2[0].Pid())
	wg.Wait()
	p.Destroy(ctx)
}

func Test_StaticPool_Invalid(t *testing.T) {
	p, err := NewPool(
		context.Background(),
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/invalid.php") },
		pipe.NewPipeFactory(log),
		testCfg,
		log,
	)

	assert.Nil(t, p)
	assert.Error(t, err)
}

func Test_ConfigNoErrorInitDefaults(t *testing.T) {
	p, err := NewPool(
		context.Background(),
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/client.php", "echo", "pipes") },
		pipe.NewPipeFactory(log),
		&pool.Config{
			AllocateTimeout: time.Second,
			DestroyTimeout:  time.Second,
		},
		log,
	)

	assert.NotNil(t, p)
	assert.NoError(t, err)
	p.Destroy(context.Background())
}

func Test_StaticPool_Echo(t *testing.T) {
	ctx := context.Background()
	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/client.php", "echo", "pipes") },
		pipe.NewPipeFactory(log),
		testCfg,
		log,
	)
	assert.NoError(t, err)

	defer p.Destroy(ctx)

	assert.NotNil(t, p)

	res, err := p.Exec(ctx, &payload.Payload{Body: []byte("hello")})

	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.NotNil(t, res.Body)
	assert.Empty(t, res.Context)

	assert.Equal(t, "hello", res.String())
}

func Test_StaticPool_Echo_NilContext(t *testing.T) {
	ctx := context.Background()
	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/client.php", "echo", "pipes") },
		pipe.NewPipeFactory(log),
		testCfg,
		log,
	)
	assert.NoError(t, err)

	defer p.Destroy(ctx)

	assert.NotNil(t, p)

	res, err := p.Exec(ctx, &payload.Payload{Body: []byte("hello"), Context: nil})

	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.NotNil(t, res.Body)
	assert.Empty(t, res.Context)

	assert.Equal(t, "hello", res.String())
}

func Test_StaticPool_Echo_Context(t *testing.T) {
	ctx := context.Background()
	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/client.php", "head", "pipes") },
		pipe.NewPipeFactory(log),
		testCfg,
		log,
	)
	assert.NoError(t, err)

	defer p.Destroy(ctx)

	assert.NotNil(t, p)

	res, err := p.Exec(ctx, &payload.Payload{Body: []byte("hello"), Context: []byte("world")})

	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.Empty(t, res.Body)
	assert.NotNil(t, res.Context)

	assert.Equal(t, "world", string(res.Context))
}

func Test_StaticPool_JobError(t *testing.T) {
	ctx := context.Background()
	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/client.php", "error", "pipes") },
		pipe.NewPipeFactory(log),
		testCfg,
		log,
	)
	assert.NoError(t, err)
	assert.NotNil(t, p)

	time.Sleep(time.Second * 2)

	res, err := p.Exec(ctx, &payload.Payload{Body: []byte("hello")})
	assert.Error(t, err)
	assert.Nil(t, res)

	if errors.Is(errors.SoftJob, err) == false {
		t.Fatal("error should be of type errors.Exec")
	}

	assert.Contains(t, err.Error(), "hello")
	p.Destroy(ctx)
}

func Test_StaticPool_Broken_Replace(t *testing.T) {
	ctx := context.Background()

	z, err := zap.NewProduction()
	require.NoError(t, err)

	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/client.php", "broken", "pipes") },
		pipe.NewPipeFactory(log),
		testCfg,
		z,
	)
	assert.NoError(t, err)
	assert.NotNil(t, p)

	time.Sleep(time.Second)
	res, err := p.Exec(ctx, &payload.Payload{Body: []byte("hello")})
	assert.Error(t, err)
	assert.Nil(t, res)

	p.Destroy(ctx)
}

func Test_StaticPool_Broken_FromOutside(t *testing.T) {
	ctx := context.Background()

	var cfg2 = &pool.Config{
		NumWorkers:      1,
		AllocateTimeout: time.Second * 5,
		DestroyTimeout:  time.Second * 5,
	}

	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/client.php", "echo", "pipes") },
		pipe.NewPipeFactory(log),
		cfg2,
		nil,
	)
	assert.NoError(t, err)
	assert.NotNil(t, p)
	time.Sleep(time.Second)

	res, err := p.Exec(ctx, &payload.Payload{Body: []byte("hello")})

	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.NotNil(t, res.Body)
	assert.Empty(t, res.Context)

	assert.Equal(t, "hello", res.String())
	assert.Equal(t, 1, len(p.Workers()))

	// first creation
	time.Sleep(time.Second * 2)
	// killing random worker and expecting pool to replace it
	err = p.Workers()[0].Kill()
	if err != nil {
		t.Errorf("error killing the process: error %v", err)
	}

	// re-creation
	time.Sleep(time.Second * 2)
	list := p.Workers()
	for _, w := range list {
		assert.Equal(t, fsm.StateReady, w.State().CurrentState())
	}
	p.Destroy(context.Background())
}

func Test_StaticPool_AllocateTimeout(t *testing.T) {
	p, err := NewPool(
		context.Background(),
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/client.php", "delay", "pipes") },
		pipe.NewPipeFactory(log),
		&pool.Config{
			NumWorkers:      1,
			AllocateTimeout: time.Nanosecond * 1,
			DestroyTimeout:  time.Second * 2,
		},
		log,
	)
	assert.Error(t, err)
	if !errors.Is(errors.WorkerAllocate, err) {
		t.Fatal("error should be of type WorkerAllocate")
	}
	assert.Nil(t, p)
}

func Test_StaticPool_Replace_Worker(t *testing.T) {
	ctx := context.Background()
	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/client.php", "pid", "pipes") },
		pipe.NewPipeFactory(log),
		&pool.Config{
			NumWorkers:      1,
			MaxJobs:         1,
			AllocateTimeout: time.Second * 5,
			DestroyTimeout:  time.Second,
		},
		log,
	)
	assert.NoError(t, err)
	assert.NotNil(t, p)

	// prevent process is not ready
	time.Sleep(time.Second)

	var lastPID string
	lastPID = strconv.Itoa(int(p.Workers()[0].Pid()))

	res, err := p.Exec(ctx, &payload.Payload{Body: []byte("hello")})
	require.Equal(t, lastPID, string(res.Body))
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		res, err = p.Exec(ctx, &payload.Payload{Body: []byte("hello")})
		require.NoError(t, err)
		require.NotNil(t, res)
		require.NotNil(t, res.Body)
		require.Empty(t, res.Context)

		require.NotEqual(t, lastPID, string(res.Body))
		lastPID = string(res.Body)
	}

	p.Destroy(context.Background())
}

func Test_StaticPool_Debug_Worker(t *testing.T) {
	ctx := context.Background()
	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/client.php", "pid", "pipes") },
		pipe.NewPipeFactory(log),
		&pool.Config{
			Debug:           true,
			AllocateTimeout: time.Second,
			DestroyTimeout:  time.Second,
		},
		log,
	)
	assert.NoError(t, err)
	assert.NotNil(t, p)

	// prevent process is not ready
	time.Sleep(time.Second)
	assert.Len(t, p.Workers(), 0)

	var lastPID string
	res, _ := p.Exec(ctx, &payload.Payload{Body: []byte("hello")})
	assert.NotEqual(t, lastPID, string(res.Body))

	assert.Len(t, p.Workers(), 0)

	for i := 0; i < 10; i++ {
		assert.Len(t, p.Workers(), 0)
		res, err := p.Exec(ctx, &payload.Payload{Body: []byte("hello")})

		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.NotNil(t, res.Body)
		assert.Empty(t, res.Context)

		assert.NotEqual(t, lastPID, string(res.Body))
		lastPID = string(res.Body)
	}

	p.Destroy(context.Background())
}

// identical to replace but controlled on worker side
func Test_StaticPool_Stop_Worker(t *testing.T) {
	ctx := context.Background()
	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/client.php", "stop", "pipes") },
		pipe.NewPipeFactory(log),
		&pool.Config{
			NumWorkers:      1,
			AllocateTimeout: time.Second,
			DestroyTimeout:  time.Second,
		},
		log,
	)
	assert.NoError(t, err)
	assert.NotNil(t, p)

	defer p.Destroy(ctx)
	time.Sleep(time.Second)

	var lastPID string
	lastPID = strconv.Itoa(int(p.Workers()[0].Pid()))

	res, err := p.Exec(ctx, &payload.Payload{Body: []byte("hello")})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, lastPID, string(res.Body))

	for i := 0; i < 10; i++ {
		res, err := p.Exec(ctx, &payload.Payload{Body: []byte("hello")})

		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.NotNil(t, res.Body)
		assert.Empty(t, res.Context)

		assert.NotEqual(t, lastPID, string(res.Body))
		lastPID = string(res.Body)
	}
}

// identical to replace but controlled on worker side
func Test_Static_Pool_Destroy_And_Close(t *testing.T) {
	ctx := context.Background()
	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/client.php", "delay", "pipes") },
		pipe.NewPipeFactory(log),
		&pool.Config{
			NumWorkers:      1,
			AllocateTimeout: time.Second,
			DestroyTimeout:  time.Second,
		},
		log,
	)

	assert.NotNil(t, p)
	assert.NoError(t, err)

	p.Destroy(ctx)
	_, err = p.Exec(ctx, &payload.Payload{Body: []byte("100")})
	assert.Error(t, err)
}

// identical to replace but controlled on worker side
func Test_Static_Pool_Destroy_And_Close_While_Wait(t *testing.T) {
	ctx := context.Background()
	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/client.php", "delay", "pipes") },
		pipe.NewPipeFactory(log),
		&pool.Config{
			NumWorkers:      1,
			AllocateTimeout: time.Second,
			DestroyTimeout:  time.Second,
		},
		log,
	)

	assert.NotNil(t, p)
	assert.NoError(t, err)

	go func() {
		_, errP := p.Exec(ctx, &payload.Payload{Body: []byte("100")})
		if errP != nil {
			t.Errorf("error executing payload: error %v", err)
		}
	}()
	time.Sleep(time.Millisecond * 100)

	p.Destroy(ctx)
	_, err = p.Exec(ctx, &payload.Payload{Body: []byte("100")})
	assert.Error(t, err)
}

// identical to replace but controlled on worker side
func Test_Static_Pool_Handle_Dead(t *testing.T) {
	ctx := context.Background()
	p, err := NewPool(
		context.Background(),
		func(cmd string) *exec.Cmd {
			return exec.Command("php", "../../tests/slow-destroy.php", "echo", "pipes")
		},
		pipe.NewPipeFactory(log),
		&pool.Config{
			NumWorkers:      5,
			AllocateTimeout: time.Second * 100,
			DestroyTimeout:  time.Second,
		},
		log,
	)
	assert.NoError(t, err)
	assert.NotNil(t, p)

	time.Sleep(time.Second)
	for i := range p.Workers() {
		p.Workers()[i].State().Transition(fsm.StateErrored)
	}

	_, err = p.Exec(ctx, &payload.Payload{Body: []byte("hello")})
	assert.NoError(t, err)
	p.Destroy(ctx)
}

// identical to replace but controlled on worker side
func Test_Static_Pool_Slow_Destroy(t *testing.T) {
	p, err := NewPool(
		context.Background(),
		func(cmd string) *exec.Cmd {
			return exec.Command("php", "../../tests/slow-destroy.php", "echo", "pipes")
		},
		pipe.NewPipeFactory(log),
		&pool.Config{
			NumWorkers:      5,
			AllocateTimeout: time.Second,
			DestroyTimeout:  time.Second,
		},
		log,
	)

	assert.NoError(t, err)
	assert.NotNil(t, p)

	p.Destroy(context.Background())
}

func Test_StaticPool_ResetTimeout(t *testing.T) {
	ctx := context.Background()

	p, err := NewPool(
		ctx,
		// sleep for the 3 seconds
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/sleep.php", "pipes") },
		pipe.NewPipeFactory(log),
		&pool.Config{
			Debug:           false,
			NumWorkers:      2,
			AllocateTimeout: time.Second * 100,
			DestroyTimeout:  time.Second * 100,
			ResetTimeout:    time.Second * 3,
		},
		log,
	)
	assert.NoError(t, err)
	assert.NotNil(t, p)

	go func() {
		_, _ = p.Exec(ctx, &payload.Payload{Body: []byte("hello")})
	}()

	time.Sleep(time.Second)

	err = p.Reset(ctx)
	assert.NoError(t, err)

	t.Cleanup(func() {
		p.Destroy(ctx)
	})
}

func Test_StaticPool_NoFreeWorkers(t *testing.T) {
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
			Supervisor:      nil,
		},
		log,
	)
	assert.NoError(t, err)
	assert.NotNil(t, p)

	go func() {
		_, _ = p.Exec(ctx, &payload.Payload{Body: []byte("hello")})
	}()

	time.Sleep(time.Second)
	res, err := p.Exec(ctx, &payload.Payload{Body: []byte("hello")})
	assert.Error(t, err)
	assert.Nil(t, res)

	time.Sleep(time.Second)

	p.Destroy(ctx)
}

func Test_StaticPool_QueueSize(t *testing.T) {
	ctx := context.Background()

	p, err := NewPool(
		ctx,
		// sleep for the 3 seconds
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/sleep_short.php", "pipes") },
		pipe.NewPipeFactory(log),
		&pool.Config{
			Debug:           false,
			NumWorkers:      1,
			AllocateTimeout: time.Second,
			DestroyTimeout:  time.Second,
			Supervisor:      nil,
		},
		log,
	)
	assert.NoError(t, err)
	assert.NotNil(t, p)

	for i := 0; i < 10; i++ {
		go func() {
			_, _ = p.Exec(ctx, &payload.Payload{Body: []byte("hello")})
		}()
	}

	time.Sleep(time.Second)
	require.LessOrEqual(t, p.QueueSize(), uint64(10))
	time.Sleep(time.Second * 20)
	require.Less(t, p.QueueSize(), uint64(10))

	p.Destroy(ctx)
}

// identical to replace but controlled on worker side
func Test_Static_Pool_WrongCommand1(t *testing.T) {
	p, err := NewPool(
		context.Background(),
		func(cmd string) *exec.Cmd {
			return exec.Command("phg", "../../tests/slow-destroy.php", "echo", "pipes")
		},
		pipe.NewPipeFactory(log),
		&pool.Config{
			NumWorkers:      5,
			AllocateTimeout: time.Second,
			DestroyTimeout:  time.Second,
		},
		log,
	)

	assert.Error(t, err)
	assert.Nil(t, p)
}

// identical to replace but controlled on worker side
func Test_Static_Pool_WrongCommand2(t *testing.T) {
	p, err := NewPool(
		context.Background(),
		func(cmd string) *exec.Cmd { return exec.Command("php", "", "echo", "pipes") },
		pipe.NewPipeFactory(log),
		&pool.Config{
			NumWorkers:      5,
			AllocateTimeout: time.Second,
			DestroyTimeout:  time.Second,
		},
		log,
	)

	assert.Error(t, err)
	assert.Nil(t, p)
}

func Test_CRC_WithPayload(t *testing.T) {
	ctx := context.Background()
	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/crc_error.php") },
		pipe.NewPipeFactory(log),
		testCfg,
		log,
	)
	assert.Error(t, err)
	data := err.Error()
	assert.Contains(t, data, "warning: some weird php erro")
	require.Nil(t, p)
}

/*
	PTR:

Benchmark_Pool_Echo-32    	   49076	     29926 ns/op	    8016 B/op	      20 allocs/op
Benchmark_Pool_Echo-32    	   47257	     30779 ns/op	    8047 B/op	      20 allocs/op
Benchmark_Pool_Echo-32    	   46737	     29440 ns/op	    8065 B/op	      20 allocs/op
Benchmark_Pool_Echo-32    	   51177	     29074 ns/op	    7981 B/op	      20 allocs/op
Benchmark_Pool_Echo-32    	   51764	     28319 ns/op	    8012 B/op	      20 allocs/op
Benchmark_Pool_Echo-32    	   54054	     30714 ns/op	    7987 B/op	      20 allocs/op
Benchmark_Pool_Echo-32    	   54391	     30689 ns/op	    8055 B/op	      20 allocs/op

VAL:
Benchmark_Pool_Echo-32    	   47936	     28679 ns/op	    7942 B/op	      19 allocs/op
Benchmark_Pool_Echo-32    	   49010	     29830 ns/op	    7970 B/op	      19 allocs/op
Benchmark_Pool_Echo-32    	   46771	     29031 ns/op	    8014 B/op	      19 allocs/op
Benchmark_Pool_Echo-32    	   47760	     30517 ns/op	    7955 B/op	      19 allocs/op
Benchmark_Pool_Echo-32    	   48148	     29816 ns/op	    7950 B/op	      19 allocs/op
Benchmark_Pool_Echo-32    	   52705	     29809 ns/op	    7979 B/op	      19 allocs/op
Benchmark_Pool_Echo-32    	   54374	     27776 ns/op	    7947 B/op	      19 allocs/op
*/
func Benchmark_Pool_Echo(b *testing.B) {
	ctx := context.Background()
	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/client.php", "echo", "pipes") },
		pipe.NewPipeFactory(log),
		testCfg,
		log,
	)
	if err != nil {
		b.Fatal(err)
	}

	bd := make([]byte, 1024)
	c := make([]byte, 1024)

	pld := &payload.Payload{
		Context: c,
		Body:    bd,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		if _, err := p.Exec(ctx, pld); err != nil {
			b.Fail()
		}
	}
}

// Benchmark_Pool_Echo_Batched-32          366996          2873 ns/op        1233 B/op          24 allocs/op
// PTR -> Benchmark_Pool_Echo_Batched-32    	  406839	      2900 ns/op	    1059 B/op	      23 allocs/op
// PTR -> Benchmark_Pool_Echo_Batched-32    	  413312	      2904 ns/op	    1067 B/op	      23 allocs/op
func Benchmark_Pool_Echo_Batched(b *testing.B) {
	ctx := context.Background()
	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/client.php", "echo", "pipes") },
		pipe.NewPipeFactory(log),
		&pool.Config{
			NumWorkers:      uint64(runtime.NumCPU()),
			AllocateTimeout: time.Second * 100,
			DestroyTimeout:  time.Second,
		},
		log,
	)
	assert.NoError(b, err)
	defer p.Destroy(ctx)

	bd := make([]byte, 1024)
	c := make([]byte, 1024)

	pld := &payload.Payload{
		Context: c,
		Body:    bd,
	}

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := p.Exec(ctx, pld); err != nil {
				b.Fail()
				l.Println(err)
			}
		}()
	}

	wg.Wait()
}

// Benchmark_Pool_Echo_Replaced-32    	     104/100	  10900218 ns/op	   52365 B/op	     125 allocs/op
func Benchmark_Pool_Echo_Replaced(b *testing.B) {
	ctx := context.Background()
	p, err := NewPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/client.php", "echo", "pipes") },
		pipe.NewPipeFactory(log),
		&pool.Config{
			NumWorkers:      1,
			MaxJobs:         1,
			AllocateTimeout: time.Second,
			DestroyTimeout:  time.Second,
		},
		log,
	)
	assert.NoError(b, err)
	defer p.Destroy(ctx)
	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		if _, err := p.Exec(ctx, &payload.Payload{Body: []byte("hello")}); err != nil {
			b.Fail()
			l.Println(err)
		}
	}
}

// BenchmarkToStringUnsafe-12    	566317729	         1.91 ns/op	       0 B/op	       0 allocs/op
// BenchmarkToStringUnsafe-32    	1000000000	         0.4434 ns/op	       0 B/op	       0 allocs/op
func BenchmarkToStringUnsafe(b *testing.B) {
	testPayload := []byte("falsflasjlifjwpoihejfoiwejow{}{}{}{}jelfjasjfhwaopiehjtopwhtgohrgouahsgkljasdlfjasl;fjals;jdflkndgouwhetopwqhjtojfalsflasjlifjwpoihejfoiwejow{}{}{}{}jelfjasjfhwaopiehjtopwhtgohrgouahsgkljasdlfjasl;fjals;jdflkndgouwhetopwqhjtojfalsflasjlifjwpoihejfoiwejow{}{}{}{}jelfjasjfhwaopiehjtopwhtgohrgouahsgkljasdlfjasl;fjals;jdflkndgouwhetopwqhjtojfalsflasjlifjwpoihejfoiwejow{}{}{}{}jelfjasjfhwaopiehjtopwhtgohrgouahsgkljasdlfjasl;fjals;jdflkndgouwhetopwqhjtojfalsflasjlifjwpoihejfoiwejow{}{}{}{}jelfjasjfhwaopiehjtopwhtgohrgouahsgkljasdlfjasl;fjals;jdflkndgouwhetopwqhjtojfalsflasjlifjwpoihejfoiwejow{}{}{}{}jelfjasjfhwaopiehjtopwhtgohrgouahsgkljasdlfjasl;fjals;jdflkndgouwhetopwqhjtojfalsflasjlifjwpoihejfoiwejow{}{}{}{}jelfjasjfhwaopiehjtopwhtgohrgouahsgkljasdlfjasl;fjals;jdflkndgouwhetopwqhjtoj")
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		res := utils.AsString(testPayload)
		_ = res
	}
}

// BenchmarkToStringSafe-32    	 8017846	       182.5 ns/op	     896 B/op	       1 allocs/op
// inline BenchmarkToStringSafe-12    	28926276	        46.6 ns/op	     128 B/op	       1 allocs/op
func BenchmarkToStringSafe(b *testing.B) {
	testPayload := []byte("falsflasjlifjwpoihejfoiwejow{}{}{}{}jelfjasjfhwaopiehjtopwhtgohrgouahsgkljasdlfjasl;fjals;jdflkndgouwhetopwqhjtojfalsflasjlifjwpoihejfoiwejow{}{}{}{}jelfjasjfhwaopiehjtopwhtgohrgouahsgkljasdlfjasl;fjals;jdflkndgouwhetopwqhjtojfalsflasjlifjwpoihejfoiwejow{}{}{}{}jelfjasjfhwaopiehjtopwhtgohrgouahsgkljasdlfjasl;fjals;jdflkndgouwhetopwqhjtojfalsflasjlifjwpoihejfoiwejow{}{}{}{}jelfjasjfhwaopiehjtopwhtgohrgouahsgkljasdlfjasl;fjals;jdflkndgouwhetopwqhjtojfalsflasjlifjwpoihejfoiwejow{}{}{}{}jelfjasjfhwaopiehjtopwhtgohrgouahsgkljasdlfjasl;fjals;jdflkndgouwhetopwqhjtojfalsflasjlifjwpoihejfoiwejow{}{}{}{}jelfjasjfhwaopiehjtopwhtgohrgouahsgkljasdlfjasl;fjals;jdflkndgouwhetopwqhjtojfalsflasjlifjwpoihejfoiwejow{}{}{}{}jelfjasjfhwaopiehjtopwhtgohrgouahsgkljasdlfjasl;fjals;jdflkndgouwhetopwqhjtoj")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		res := toStringNotFun(testPayload)
		_ = res
	}
}

func toStringNotFun(data []byte) string {
	return string(data)
}
