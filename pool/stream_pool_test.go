package pool

import (
	"context"
	"os/exec"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/roadrunner-server/api/v2/payload"
	"github.com/roadrunner-server/api/v2/pool"
	"github.com/roadrunner-server/api/v2/worker"
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/sdk/v2/ipc/pipe"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func Test_StreamPool_Echo(t *testing.T) {
	ctx := context.Background()
	p, err := NewStaticPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../tests/client.php", "echo", "pipes") },
		pipe.NewPipeFactory(log),
		testCfg,
		log,
	)
	assert.NoError(t, err)

	defer p.Destroy(ctx)

	assert.NotNil(t, p)

	resp := make(chan *payload.Payload)
	go func() {
		require.NoError(t, p.(pool.Streamer).ExecStream(&payload.Payload{Body: []byte("hello")}, resp, nil))
	}()

	for {
		select {
		case res, ok := <-resp:
			if !ok {
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, res)
			assert.NotNil(t, res.Body)
			assert.Empty(t, res.Context)

			assert.Equal(t, "hello", res.String())
		}
	}
}

func Test_StreamPool_Echo_NilContext(t *testing.T) {
	ctx := context.Background()
	p, err := NewStaticPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../tests/client.php", "echo", "pipes") },
		pipe.NewPipeFactory(log),
		testCfg,
		log,
	)
	assert.NoError(t, err)

	defer p.Destroy(ctx)

	assert.NotNil(t, p)

	resp := make(chan *payload.Payload)
	go func() {
		require.NoError(t, p.(pool.Streamer).ExecStream(&payload.Payload{Body: []byte("hello"), Context: nil}, resp, nil))
	}()

	for {
		select {
		case res, ok := <-resp:
			if !ok {
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, res)
			assert.NotNil(t, res.Body)
			assert.Empty(t, res.Context)

			assert.Equal(t, "hello", res.String())
		}
	}
}

func Test_StreamPool_Echo_Context(t *testing.T) {
	ctx := context.Background()
	p, err := NewStaticPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../tests/client.php", "head", "pipes") },
		pipe.NewPipeFactory(log),
		testCfg,
		log,
	)
	assert.NoError(t, err)

	defer p.Destroy(ctx)

	assert.NotNil(t, p)

	resp := make(chan *payload.Payload)
	go func() {
		require.NoError(t, p.(pool.Streamer).ExecStream(&payload.Payload{Body: []byte("hello"), Context: []byte("world")}, resp, nil))
	}()

	for {
		select {
		case res, ok := <-resp:
			if !ok {
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, res)
			assert.Empty(t, res.Body)
			assert.NotNil(t, res.Context)

			assert.Equal(t, "world", string(res.Context))
		}
	}
}

func Test_StreamPool_JobError(t *testing.T) {
	ctx := context.Background()
	p, err := NewStaticPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../tests/client.php", "error", "pipes") },
		pipe.NewPipeFactory(log),
		testCfg,
		log,
	)
	assert.NoError(t, err)
	assert.NotNil(t, p)

	time.Sleep(time.Second)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	resp := make(chan *payload.Payload)
	go func() {
		defer wg.Done()
		errExec := p.(pool.Streamer).ExecStream(&payload.Payload{Body: []byte("hello")}, resp, nil)
		require.Error(t, errExec)

		require.True(t, errors.Is(errors.SoftJob, errExec))
		assert.Contains(t, errExec.Error(), "hello")
	}()

	wg.Wait()
	p.Destroy(ctx)
}

func Test_StreamPool_Broken_Replace(t *testing.T) {
	ctx := context.Background()

	z, err := zap.NewProduction()
	require.NoError(t, err)

	p, err := NewStaticPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../tests/client.php", "broken", "pipes") },
		pipe.NewPipeFactory(log),
		testCfg,
		z,
	)
	assert.NoError(t, err)
	assert.NotNil(t, p)

	time.Sleep(time.Second)
	wg := &sync.WaitGroup{}
	wg.Add(1)

	resp := make(chan *payload.Payload)
	go func() {
		defer wg.Done()
		require.Error(t, p.(pool.Streamer).ExecStreamWithTTL(ctx, &payload.Payload{Body: []byte("hello"), Context: []byte("world")}, resp, nil))

		for {
			select {
			case res, ok := <-resp:
				if !ok {
					return
				}
				assert.Nil(t, res)
			}
		}
	}()

	wg.Wait()
	p.Destroy(ctx)
}

func Test_StreamPool_Broken_FromOutside(t *testing.T) {
	ctx := context.Background()

	var cfg2 = &Config{
		NumWorkers:      1,
		AllocateTimeout: time.Second * 5,
		DestroyTimeout:  time.Second * 5,
	}

	p, err := NewStaticPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../tests/client.php", "echo", "pipes") },
		pipe.NewPipeFactory(log),
		cfg2,
		nil,
	)
	assert.NoError(t, err)
	assert.NotNil(t, p)
	defer p.Destroy(ctx)
	time.Sleep(time.Second)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	resp := make(chan *payload.Payload)
	go func() {
		defer wg.Done()
		require.NoError(t, p.(pool.Streamer).ExecStream(&payload.Payload{Body: []byte("hello")}, resp, nil))
	}()

	for {
		select {
		case res, ok := <-resp:
			if !ok {
				goto tests
			}
			assert.NoError(t, err)
			assert.NotNil(t, res)
			assert.NotNil(t, res.Body)
			assert.Empty(t, res.Context)

			assert.Equal(t, "hello", res.String())
			assert.Equal(t, 1, len(p.Workers()))
		}
	}

tests:
	wg.Wait()

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
		assert.Equal(t, worker.StateReady, w.State().Value())
	}
}

func Test_StreamPool_Replace_Worker(t *testing.T) {
	ctx := context.Background()
	p, err := NewStaticPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../tests/client.php", "pid", "pipes") },
		pipe.NewPipeFactory(log),
		&Config{
			NumWorkers:      1,
			MaxJobs:         1,
			AllocateTimeout: time.Second,
			DestroyTimeout:  time.Second,
		},
		log,
	)
	assert.NoError(t, err)
	assert.NotNil(t, p)

	defer p.Destroy(ctx)
	// prevent process is not ready
	time.Sleep(time.Second)

	var lastPID string
	lastPID = strconv.Itoa(int(p.Workers()[0].Pid()))

	resp := make(chan *payload.Payload)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t, p.(pool.Streamer).ExecStream(&payload.Payload{Body: []byte("hello")}, resp, nil))
	}()

	for res := range resp {
		assert.Equal(t, lastPID, string(res.Body))
	}
	wg.Wait()

	for i := 0; i < 10; i++ {
		respLoop := make(chan *payload.Payload)
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, p.(pool.Streamer).ExecStream(&payload.Payload{Body: []byte("hello")}, respLoop, nil))
		}()

		for res := range respLoop {
			assert.NoError(t, err)
			assert.NotNil(t, res)
			assert.NotNil(t, res.Body)
			assert.Empty(t, res.Context)

			assert.NotEqual(t, lastPID, string(res.Body))
			lastPID = string(res.Body)
		}

		wg.Wait()
	}
}

func Test_StreamPool_Debug_Worker(t *testing.T) {
	ctx := context.Background()
	p, err := NewStaticPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../tests/client.php", "pid", "pipes") },
		pipe.NewPipeFactory(log),
		&Config{
			Debug:           true,
			AllocateTimeout: time.Second,
			DestroyTimeout:  time.Second,
		},
		log,
	)
	assert.NoError(t, err)
	assert.NotNil(t, p)

	defer p.Destroy(ctx)

	// prevent process is not ready
	time.Sleep(time.Second)
	assert.Len(t, p.Workers(), 0)

	var lastPID string
	resp := make(chan *payload.Payload)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t, p.(pool.Streamer).ExecStream(&payload.Payload{Body: []byte("hello")}, resp, nil))
	}()

	for res := range resp {
		require.NotEqual(t, lastPID, string(res.Body))
	}
	wg.Wait()

	require.Len(t, p.Workers(), 0)

	for i := 0; i < 10; i++ {
		respLoop := make(chan *payload.Payload)
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, p.(pool.Streamer).ExecStream(&payload.Payload{Body: []byte("hello")}, respLoop, nil))
		}()

		for res := range respLoop {
			assert.NoError(t, err)
			assert.NotNil(t, res)
			assert.NotNil(t, res.Body)
			assert.Empty(t, res.Context)

			assert.NotEqual(t, lastPID, string(res.Body))
			lastPID = string(res.Body)
		}

		wg.Wait()
	}
}

// identical to replace but controlled on worker side
func Test_StreamPool_Stop_Worker(t *testing.T) {
	ctx := context.Background()
	p, err := NewStaticPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../tests/client.php", "stop", "pipes") },
		pipe.NewPipeFactory(log),
		&Config{
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

	resp := make(chan *payload.Payload)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t, p.(pool.Streamer).ExecStream(&payload.Payload{Body: []byte("hello")}, resp, nil))
	}()

	for res := range resp {
		require.Equal(t, lastPID, string(res.Body))
	}
	wg.Wait()

	for i := 0; i < 10; i++ {
		respLoop := make(chan *payload.Payload)
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, p.(pool.Streamer).ExecStream(&payload.Payload{Body: []byte("hello")}, respLoop, nil))
		}()

		for res := range respLoop {
			assert.NoError(t, err)
			assert.NotNil(t, res)
			assert.NotNil(t, res.Body)
			assert.Empty(t, res.Context)

			assert.NotEqual(t, lastPID, string(res.Body))
			lastPID = string(res.Body)
		}

		wg.Wait()
	}
}

// identical to replace but controlled on worker side
func Test_Stream_Pool_Destroy_And_Close(t *testing.T) {
	ctx := context.Background()
	p, err := NewStaticPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../tests/client.php", "delay", "pipes") },
		pipe.NewPipeFactory(log),
		&Config{
			NumWorkers:      1,
			AllocateTimeout: time.Second,
			DestroyTimeout:  time.Second,
		},
		log,
	)

	assert.NotNil(t, p)
	assert.NoError(t, err)

	p.Destroy(ctx)
	resp := make(chan *payload.Payload)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.Error(t, p.(pool.Streamer).ExecStream(&payload.Payload{Body: []byte("hello")}, resp, nil))
	}()

	wg.Wait()
}

// identical to replace but controlled on worker side
func Test_Stream_Pool_Destroy_And_Close_While_Wait(t *testing.T) {
	ctx := context.Background()
	p, err := NewStaticPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../tests/client.php", "delay", "pipes") },
		pipe.NewPipeFactory(log),
		&Config{
			NumWorkers:      1,
			AllocateTimeout: time.Second,
			DestroyTimeout:  time.Second,
		},
		log,
	)

	assert.NotNil(t, p)
	assert.NoError(t, err)

	go func() {
		resp := make(chan *payload.Payload)
		go func() {
			require.NoError(t, p.(pool.Streamer).ExecStream(&payload.Payload{Body: []byte("100")}, resp, nil))
		}()
	}()
	time.Sleep(time.Millisecond * 100)

	ctx2, cancel := context.WithTimeout(ctx, time.Second*3)
	defer cancel()
	p.Destroy(ctx2)

	resp := make(chan *payload.Payload)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.Error(t, p.(pool.Streamer).ExecStream(&payload.Payload{Body: []byte("100")}, resp, nil))
	}()

	wg.Wait()
}

// identical to replace but controlled on worker side
func Test_Stream_Pool_Handle_Dead(t *testing.T) {
	ctx := context.Background()
	p, err := NewStaticPool(
		context.Background(),
		func(cmd string) *exec.Cmd { return exec.Command("php", "../tests/slow-destroy.php", "echo", "pipes") },
		pipe.NewPipeFactory(log),
		&Config{
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
		p.Workers()[i].State().Set(worker.StateErrored)
	}

	resp := make(chan *payload.Payload)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.NoError(t, p.(pool.Streamer).ExecStream(&payload.Payload{Body: []byte("hello")}, resp, nil))
	}()

	for range resp {
	}

	wg.Wait()
	p.Destroy(ctx)
}

func Test_StreamPool_NoFreeWorkers(t *testing.T) {
	ctx := context.Background()

	p, err := NewStaticPool(
		ctx,
		// sleep for the 3 seconds
		func(cmd string) *exec.Cmd { return exec.Command("php", "../tests/sleep.php", "pipes") },
		pipe.NewPipeFactory(log),
		&Config{
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

	wg := &sync.WaitGroup{}

	go func() {
		resp := make(chan *payload.Payload)
		go func() {
			ctxNew, cancel := context.WithTimeout(context.Background(), time.Second*5)
			defer cancel()
			_ = p.(pool.Streamer).ExecStreamWithTTL(ctxNew, &payload.Payload{Body: []byte("hello")}, resp, nil)
		}()
	}()

	time.Sleep(time.Second)

	resp2 := make(chan *payload.Payload)
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.Error(t, p.(pool.Streamer).ExecStreamWithTTL(ctx, &payload.Payload{Body: []byte("hello")}, resp2, nil))
	}()

	for range resp2 {
	}

	wg.Wait()
	time.Sleep(time.Second)

	p.Destroy(ctx)
}

func Test_StreamPool_QueueSize(t *testing.T) {
	ctx := context.Background()

	p, err := NewStaticPool(
		ctx,
		// sleep for the 3 seconds
		func(cmd string) *exec.Cmd { return exec.Command("php", "../tests/sleep_short.php", "pipes") },
		pipe.NewPipeFactory(log),
		&Config{
			Debug:           false,
			NumWorkers:      10,
			AllocateTimeout: time.Second * 2,
			DestroyTimeout:  time.Second,
			Supervisor:      nil,
		},
		log,
	)
	assert.NoError(t, err)
	assert.NotNil(t, p)

	for i := 0; i < 10; i++ {
		go func() {
			resp := make(chan *payload.Payload)
			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				require.NoError(t, p.(pool.Streamer).ExecStream(&payload.Payload{Body: []byte("hello")}, resp, nil))
			}()

			for range resp {
			}
			wg.Wait()
		}()
	}

	time.Sleep(time.Second)
	require.LessOrEqual(t, p.(pool.Queuer).QueueSize(), uint64(10))
	time.Sleep(time.Second * 20)
	require.LessOrEqual(t, p.(pool.Queuer).QueueSize(), uint64(10))

	p.Destroy(ctx)
}

/* PTR:
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
func Benchmark_StreamPool_Echo(b *testing.B) {
	ctx := context.Background()
	p, err := NewStaticPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../tests/client.php", "echo", "pipes") },
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
		resp := make(chan *payload.Payload, 1)
		go func() {
			errS := p.(pool.Streamer).ExecStream(pld, resp, nil)
			if errS != nil {
				b.Fail()
			}
		}()
		select {
		case _ = <-resp:
			break
		}
	}
}

// Benchmark_Pool_Echo_Batched-32          366996          2873 ns/op        1233 B/op          24 allocs/op
// PTR -> Benchmark_Pool_Echo_Batched-32    	  406839	      2900 ns/op	    1059 B/op	      23 allocs/op
// PTR -> Benchmark_Pool_Echo_Batched-32    	  413312	      2904 ns/op	    1067 B/op	      23 allocs/op
func Benchmark_StreamPool_Echo_Batched(b *testing.B) {
	ctx := context.Background()
	p, err := NewStaticPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../tests/client.php", "echo", "pipes") },
		pipe.NewPipeFactory(log),
		&Config{
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
			resp := make(chan *payload.Payload, 1)
			go func() {
				errS := p.(pool.Streamer).ExecStream(pld, resp, nil)
				if errS != nil {
					b.Fail()
				}
			}()
			select {
			case _ = <-resp:
				break
			}
		}()
	}

	wg.Wait()
}

// Benchmark_Pool_Echo_Replaced-32    	     104/100	  10900218 ns/op	   52365 B/op	     125 allocs/op
func Benchmark_Stream_Pool_Echo_Replaced(b *testing.B) {
	ctx := context.Background()
	p, err := NewStaticPool(
		ctx,
		func(cmd string) *exec.Cmd { return exec.Command("php", "../tests/client.php", "echo", "pipes") },
		pipe.NewPipeFactory(log),
		&Config{
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
		resp := make(chan *payload.Payload, 1)
		go func() {
			errS := p.(pool.Streamer).ExecStream(&payload.Payload{Body: []byte("hello")}, resp, nil)
			if errS != nil {
				b.Fail()
			}
		}()
		select {
		case _ = <-resp:
			break
		}
	}
}
