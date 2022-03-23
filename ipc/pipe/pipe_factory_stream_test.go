package pipe

import (
	"context"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/roadrunner-server/api/v2/payload"
	"github.com/roadrunner-server/errors"
	workerImpl "github.com/roadrunner-server/sdk/v2/worker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_StreamPipe_Echo(t *testing.T) {
	cmd := exec.Command("php", "../../tests/client.php", "echo", "pipes")
	ctx := context.Background()
	w, err := NewPipeFactory(log).SpawnWorkerWithTimeout(ctx, cmd)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = w.Stop()
		if err != nil {
			t.Errorf("error stopping the Process: error %v", err)
		}
	}()

	go func() {
		if w.Wait() != nil {
			t.Fail()
		}
	}()

	sw := workerImpl.From(w)

	resp := make(chan *payload.Payload)
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		errS := sw.ExecStream(&payload.Payload{Body: []byte("hello")}, resp, nil)
		assert.NoError(t, errS)
	}()

	for res := range resp {
		assert.NotNil(t, res)
		assert.NotNil(t, res.Body)
		assert.Empty(t, res.Context)
		assert.Equal(t, "hello", res.String())
	}

	wg.Wait()
}

func Test_StreamPipe_Echo3(t *testing.T) {
	cmd := exec.Command("php", "../../tests/stream_worker.php")
	ctx := context.Background()
	w, err := NewPipeFactory(log).SpawnWorkerWithTimeout(ctx, cmd)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = w.Stop()
		if err != nil {
			t.Errorf("error stopping the process: error %v", err)
		}
	}()

	go func() {
		if w.Wait() != nil {
			t.Fail()
		}
	}()

	sw := workerImpl.From(w)

	resp := make(chan *payload.Payload)
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		errS := sw.ExecStream(&payload.Payload{Body: []byte("hello")}, resp, make(chan struct{}))
		assert.NoError(t, errS)
	}()

	for res := range resp {
		require.Equal(t, "fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", string(res.Body))
	}

	wg.Wait()
}

func Test_StreamPipe_Broken(t *testing.T) {
	cmd := exec.Command("php", "../../tests/client.php", "broken", "pipes")
	ctx := context.Background()
	w, err := NewPipeFactory(log).SpawnWorkerWithTimeout(ctx, cmd)
	require.NoError(t, err)
	require.NotNil(t, w)

	go func() {
		errW := w.Wait()
		require.Error(t, errW)
	}()

	sw := workerImpl.From(w)

	resp := make(chan *payload.Payload)
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		errS := sw.ExecStream(&payload.Payload{Body: []byte("hello")}, resp, nil)
		assert.Error(t, errS)
	}()

	for res := range resp {
		assert.Nil(t, res)
	}

	wg.Wait()

	time.Sleep(time.Second)
	err = w.Stop()
	assert.NoError(t, err)
}

func Benchmark_StreamPipe_Worker_ExecEcho(b *testing.B) {
	cmd := exec.Command("php", "../../tests/client.php", "echo", "pipes")

	w, _ := NewPipeFactory(log).SpawnWorkerWithTimeout(context.Background(), cmd)
	sw := workerImpl.From(w)

	go func() {
		err := w.Wait()
		if err != nil {
			b.Errorf("error waiting the worker: error %v", err)
		}
	}()
	defer func() {
		err := w.Stop()
		if err != nil {
			b.Errorf("error stopping the worker: error %v", err)
		}
	}()

	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		resp := make(chan *payload.Payload)

		go func() {
			_ = sw.ExecStream(&payload.Payload{Body: []byte("hello")}, resp, nil)
		}()

		for range resp {
		}
	}
}

func Test_StreamEcho(t *testing.T) {
	ctx := context.Background()
	cmd := exec.Command("php", "../../tests/client.php", "echo", "pipes")

	w, err := NewPipeFactory(log).SpawnWorkerWithTimeout(ctx, cmd)
	if err != nil {
		t.Fatal(err)
	}

	sw := workerImpl.From(w)
	go func() {
		assert.NoError(t, sw.Wait())
	}()
	defer func() {
		err = sw.Stop()
		if err != nil {
			t.Errorf("error stopping the Process: error %v", err)
		}
	}()

	resp := make(chan *payload.Payload)
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		errS := sw.ExecStream(&payload.Payload{Body: []byte("hello")}, resp, nil)
		assert.NoError(t, errS)
	}()

	for res := range resp {
		assert.NotNil(t, res)
		assert.NotNil(t, res.Body)
		assert.Empty(t, res.Context)
		assert.Equal(t, "hello", res.String())
	}

	wg.Wait()
}

func Test_StreamBadPayload(t *testing.T) {
	ctx := context.Background()
	cmd := exec.Command("php", "../../tests/client.php", "echo", "pipes")

	w, _ := NewPipeFactory(log).SpawnWorkerWithTimeout(ctx, cmd)

	sw := workerImpl.From(w)

	go func() {
		assert.NoError(t, sw.Wait())
	}()
	defer func() {
		err := sw.Stop()
		if err != nil {
			t.Errorf("error stopping the Process: error %v", err)
		}
	}()

	resp := make(chan *payload.Payload)
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		errS := sw.ExecStream(&payload.Payload{}, resp, nil)
		assert.Error(t, errS)
		assert.Contains(t, errS.Error(), "payload can not be empty")
	}()

	for res := range resp {
		assert.Nil(t, res)
	}

	wg.Wait()
}

func Test_StreamEcho_Slow(t *testing.T) {
	ctx := context.Background()
	cmd := exec.Command("php", "../../tests/slow-client.php", "echo", "pipes", "10", "10")

	w, _ := NewPipeFactory(log).SpawnWorkerWithTimeout(ctx, cmd)
	go func() {
		assert.NoError(t, w.Wait())
	}()
	defer func() {
		err := w.Stop()
		if err != nil {
			t.Errorf("error stopping the Process: error %v", err)
		}
	}()

	sw := workerImpl.From(w)

	resp := make(chan *payload.Payload)
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		errS := sw.ExecStream(&payload.Payload{Body: []byte("hello")}, resp, nil)
		assert.NoError(t, errS)
	}()

	for res := range resp {
		assert.NotNil(t, res)
		assert.NotNil(t, res.Body)
		assert.Empty(t, res.Context)
		assert.Equal(t, "hello", res.String())
	}

	wg.Wait()
}

func Test_StreamBroken(t *testing.T) {
	ctx := context.Background()
	cmd := exec.Command("php", "../../tests/client.php", "broken", "pipes")

	w, err := NewPipeFactory(log).SpawnWorkerWithTimeout(ctx, cmd)
	if err != nil {
		t.Fatal(err)
	}

	sw := workerImpl.From(w)

	resp := make(chan *payload.Payload)
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		errS := sw.ExecStream(&payload.Payload{Body: []byte("hello")}, resp, nil)
		assert.Error(t, errS)
	}()

	for res := range resp {
		assert.Nil(t, res)
	}

	wg.Wait()

	time.Sleep(time.Second * 3)
	assert.Error(t, w.Stop())
}

func Test_StreamError(t *testing.T) {
	ctx := context.Background()
	cmd := exec.Command("php", "../../tests/client.php", "error", "pipes")

	w, _ := NewPipeFactory(log).SpawnWorkerWithTimeout(ctx, cmd)
	go func() {
		assert.NoError(t, w.Wait())
	}()

	defer func() {
		err := w.Stop()
		if err != nil {
			t.Errorf("error stopping the Process: error %v", err)
		}
	}()

	sw := workerImpl.From(w)

	resp := make(chan *payload.Payload)
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		errS := sw.ExecStream(&payload.Payload{Body: []byte("hello")}, resp, nil)
		assert.Error(t, errS)
		require.True(t, errors.Is(errors.SoftJob, errS))
		assert.Contains(t, errS.Error(), "hello")
	}()

	for res := range resp {
		assert.Nil(t, res)
	}

	wg.Wait()
}

func Test_StreamNumExecs(t *testing.T) {
	ctx := context.Background()
	cmd := exec.Command("php", "../../tests/client.php", "echo", "pipes")

	w, _ := NewPipeFactory(log).SpawnWorkerWithTimeout(ctx, cmd)
	go func() {
		assert.NoError(t, w.Wait())
	}()
	defer func() {
		err := w.Stop()
		if err != nil {
			t.Errorf("error stopping the Process: error %v", err)
		}
	}()

	sw := workerImpl.From(w)
	resp := make(chan *payload.Payload)
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		errS := sw.ExecStream(&payload.Payload{Body: []byte("hello")}, resp, nil)
		assert.NoError(t, errS)
	}()

	for range resp {
	}

	wg.Wait()

	assert.Equal(t, uint64(1), w.State().NumExecs())

	resp = make(chan *payload.Payload)
	wg.Add(1)

	go func() {
		defer wg.Done()
		errS := sw.ExecStream(&payload.Payload{Body: []byte("hello")}, resp, nil)
		assert.NoError(t, errS)
	}()

	for range resp {
	}

	wg.Wait()
	assert.Equal(t, uint64(2), w.State().NumExecs())

	resp = make(chan *payload.Payload)
	wg.Add(1)

	go func() {
		defer wg.Done()
		errS := sw.ExecStream(&payload.Payload{Body: []byte("hello")}, resp, nil)
		assert.NoError(t, errS)
	}()

	for range resp {
	}

	wg.Wait()
	assert.Equal(t, uint64(3), w.State().NumExecs())
}

func Test_StreamPipe_Echo2(t *testing.T) {
	cmd := exec.Command("php", "../../tests/client.php", "echo", "pipes")
	w, err := NewPipeFactory(log).SpawnWorker(cmd)
	assert.NoError(t, err)

	sw := workerImpl.From(w)

	go func() {
		if w.Wait() != nil {
			t.Fail()
		}
	}()

	resp := make(chan *payload.Payload)
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		errS := sw.ExecStream(&payload.Payload{Body: []byte("hello")}, resp, nil)
		assert.NoError(t, errS)
	}()

	for res := range resp {
		assert.NotNil(t, res)
		assert.NotNil(t, res.Body)
		assert.Empty(t, res.Context)
		assert.Equal(t, "hello", res.String())
	}

	wg.Wait()

	err = w.Stop()
	assert.NoError(t, err)
}

func Test_StreamPipe_Broken2(t *testing.T) {
	cmd := exec.Command("php", "../../tests/client.php", "broken", "pipes")
	w, err := NewPipeFactory(log).SpawnWorker(cmd)
	assert.NoError(t, err)
	require.NotNil(t, w)

	sw := workerImpl.From(w)
	resp := make(chan *payload.Payload)
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		errS := sw.ExecStream(&payload.Payload{Body: []byte("hello")}, resp, nil)
		assert.Error(t, errS)
	}()

	for res := range resp {
		assert.Nil(t, res)
	}

	wg.Wait()

	time.Sleep(time.Second)
	err = w.Stop()
	assert.Error(t, err)
}

func Benchmark_StreamPipe_Worker_ExecEcho2(b *testing.B) {
	cmd := exec.Command("php", "../../tests/client.php", "echo", "pipes")

	w, _ := NewPipeFactory(log).SpawnWorker(cmd)
	sw := workerImpl.From(w)

	b.ReportAllocs()
	b.ResetTimer()
	go func() {
		err := w.Wait()
		if err != nil {
			b.Errorf("error waiting the worker: error %v", err)
		}
	}()
	defer func() {
		err := w.Stop()
		if err != nil {
			b.Errorf("error stopping the worker: error %v", err)
		}
	}()

	for n := 0; n < b.N; n++ {
		resp := make(chan *payload.Payload)

		go func() {
			_ = sw.ExecStream(&payload.Payload{Body: []byte("hello")}, resp, nil)
		}()

		for range resp {
		}
	}
}

func Benchmark_StreamPipe_Worker_ExecEcho4(b *testing.B) {
	cmd := exec.Command("php", "../../tests/client.php", "echo", "pipes")
	w, err := NewPipeFactory(log).SpawnWorker(cmd)
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		err = w.Stop()
		if err != nil {
			b.Errorf("error stopping the Process: error %v", err)
		}
	}()

	sw := workerImpl.From(w)

	for n := 0; n < b.N; n++ {
		resp := make(chan *payload.Payload)

		go func() {
			_ = sw.ExecStream(&payload.Payload{Body: []byte("hello")}, resp, nil)
		}()

		for range resp {
		}
	}
}

func Benchmark_StreamPipe_Worker_ExecEchoWithoutContext2(b *testing.B) {
	cmd := exec.Command("php", "../../tests/client.php", "echo", "pipes")
	w, err := NewPipeFactory(log).SpawnWorker(cmd)
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		err = w.Stop()
		if err != nil {
			b.Errorf("error stopping the Process: error %v", err)
		}
	}()

	sw := workerImpl.From(w)

	for n := 0; n < b.N; n++ {
		resp := make(chan *payload.Payload)

		go func() {
			_ = sw.ExecStream(&payload.Payload{Body: []byte("hello")}, resp, nil)
		}()

		for range resp {
		}
	}
}

func Test_StreamEcho2(t *testing.T) {
	cmd := exec.Command("php", "../../tests/client.php", "echo", "pipes")

	w, err := NewPipeFactory(log).SpawnWorker(cmd)
	if err != nil {
		t.Fatal(err)
	}

	sw := workerImpl.From(w)

	go func() {
		assert.NoError(t, sw.Wait())
	}()
	defer func() {
		err = sw.Stop()
		if err != nil {
			t.Errorf("error stopping the Process: error %v", err)
		}
	}()

	resp := make(chan *payload.Payload)
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		errS := sw.ExecStream(&payload.Payload{Body: []byte("hello")}, resp, nil)
		assert.NoError(t, errS)
	}()

	for res := range resp {
		assert.NotNil(t, res)
		assert.NotNil(t, res.Body)
		assert.Empty(t, res.Context)
		assert.Equal(t, "hello", res.String())
	}

	wg.Wait()
}

func Test_StreamBadPayload2(t *testing.T) {
	cmd := exec.Command("php", "../../tests/client.php", "echo", "pipes")

	w, _ := NewPipeFactory(log).SpawnWorker(cmd)

	sw := workerImpl.From(w)

	go func() {
		assert.NoError(t, sw.Wait())
	}()
	defer func() {
		err := sw.Stop()
		if err != nil {
			t.Errorf("error stopping the Process: error %v", err)
		}
	}()

	resp := make(chan *payload.Payload)
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		errS := sw.ExecStream(&payload.Payload{}, resp, nil)
		assert.Error(t, errS)
		assert.Contains(t, errS.Error(), "payload can not be empty")
	}()

	for res := range resp {
		assert.Nil(t, res)
	}

	wg.Wait()
}

func Test_StreamEcho_Slow2(t *testing.T) {
	cmd := exec.Command("php", "../../tests/slow-client.php", "echo", "pipes", "10", "10")

	w, _ := NewPipeFactory(log).SpawnWorker(cmd)
	go func() {
		assert.NoError(t, w.Wait())
	}()
	defer func() {
		err := w.Stop()
		if err != nil {
			t.Errorf("error stopping the Process: error %v", err)
		}
	}()

	sw := workerImpl.From(w)

	resp := make(chan *payload.Payload)
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		errS := sw.ExecStream(&payload.Payload{Body: []byte("hello")}, resp, nil)
		assert.NoError(t, errS)
	}()

	for res := range resp {
		assert.NotNil(t, res)
		assert.NotNil(t, res.Body)
		assert.Empty(t, res.Context)
		assert.Equal(t, "hello", res.String())
	}

	wg.Wait()
}

func Test_StreamBroken2(t *testing.T) {
	cmd := exec.Command("php", "../../tests/client.php", "broken", "pipes")

	w, err := NewPipeFactory(log).SpawnWorker(cmd)
	if err != nil {
		t.Fatal(err)
	}

	sw := workerImpl.From(w)
	resp := make(chan *payload.Payload)
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		errS := sw.ExecStream(&payload.Payload{Body: []byte("hello")}, resp, nil)
		assert.Error(t, errS)
	}()

	for res := range resp {
		assert.Nil(t, res)
	}

	wg.Wait()

	time.Sleep(time.Second * 3)
	assert.Error(t, w.Stop())
}

func Test_StreamError2(t *testing.T) {
	cmd := exec.Command("php", "../../tests/client.php", "error", "pipes")

	w, _ := NewPipeFactory(log).SpawnWorker(cmd)
	go func() {
		assert.NoError(t, w.Wait())
	}()

	defer func() {
		err := w.Stop()
		if err != nil {
			t.Errorf("error stopping the Process: error %v", err)
		}
	}()

	sw := workerImpl.From(w)

	resp := make(chan *payload.Payload)
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		errS := sw.ExecStream(&payload.Payload{Body: []byte("hello")}, resp, nil)
		assert.Error(t, errS)
		require.True(t, errors.Is(errors.SoftJob, errS))
		assert.Contains(t, errS.Error(), "hello")
	}()

	for res := range resp {
		assert.Nil(t, res)
	}

	wg.Wait()
}

func Test_StreamNumExecs2(t *testing.T) {
	cmd := exec.Command("php", "../../tests/client.php", "echo", "pipes")

	w, _ := NewPipeFactory(log).SpawnWorker(cmd)
	go func() {
		assert.NoError(t, w.Wait())
	}()
	defer func() {
		err := w.Stop()
		if err != nil {
			t.Errorf("error stopping the Process: error %v", err)
		}
	}()

	sw := workerImpl.From(w)

	resp := make(chan *payload.Payload)
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		errS := sw.ExecStream(&payload.Payload{Body: []byte("hello")}, resp, nil)
		assert.NoError(t, errS)
	}()

	for range resp {
	}

	wg.Wait()

	assert.Equal(t, uint64(1), w.State().NumExecs())

	resp = make(chan *payload.Payload)
	wg.Add(1)

	go func() {
		defer wg.Done()
		errS := sw.ExecStream(&payload.Payload{Body: []byte("hello")}, resp, nil)
		assert.NoError(t, errS)
	}()

	for range resp {
	}

	wg.Wait()
	assert.Equal(t, uint64(2), w.State().NumExecs())

	resp = make(chan *payload.Payload)
	wg.Add(1)

	go func() {
		defer wg.Done()
		errS := sw.ExecStream(&payload.Payload{Body: []byte("hello")}, resp, nil)
		assert.NoError(t, errS)
	}()

	for range resp {
	}

	wg.Wait()
	assert.Equal(t, uint64(3), w.State().NumExecs())
}
