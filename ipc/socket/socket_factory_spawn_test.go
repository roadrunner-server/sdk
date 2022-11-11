package socket

import (
	"context"
	"net"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/roadrunner-server/goridge/v3/pkg/frame"
	"github.com/roadrunner-server/sdk/v3/payload"
	"github.com/roadrunner-server/sdk/v3/pool"
	"github.com/roadrunner-server/sdk/v3/pool/fork_pool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var log = zap.NewNop()

var testCfg = &pool.Config{
	AllocateTimeout: time.Second * 500,
	DestroyTimeout:  time.Second * 500,
}

func Test1(t *testing.T) {
	log, err := zap.NewDevelopment()
	if err != nil {
		t.Fatal(err)
	}

	os.RemoveAll("sock.unix")

	ls, err := net.Listen("unix", "sock.unix")
	assert.NoError(t, err)
	defer func() {
		err = ls.Close()
		assert.NoError(t, err)
	}()

	p, err := fork_pool.NewMasterWorker(
		context.Background(),
		func(cmd string) *exec.Cmd { return exec.Command("php", "../../tests/client.php", "echo", "unix") }, NewSocketServer(ls, time.Minute, log), testCfg, log)
	if err != nil {
		t.Fatal(err)
	}

	np, err := p.Fork(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		np.Wait()
	}()

	_, err = np.ExecWithTTL(context.Background(), &payload.Payload{
		Context: nil,
		Body:    []byte("hello"),
		Codec:   frame.CodecJSON,
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second * 500)

}

func Test_Tcp_Start2(t *testing.T) {
	ls, err := net.Listen("tcp", "127.0.0.1:9007")
	if assert.NoError(t, err) {
		defer func() {
			errC := ls.Close()
			if errC != nil {
				t.Errorf("error closing the listener: error %v", err)
			}
		}()
	} else {
		t.Skip("socket is busy")
	}

	cmd := exec.Command("php", "../../tests/client.php", "echo", "tcp")

	w, err := NewSocketServer(ls, time.Minute, log).SpawnWorker(cmd)
	assert.NoError(t, err)
	assert.NotNil(t, w)

	go func() {
		assert.NoError(t, w.Wait())
	}()

	err = w.Stop()
	if err != nil {
		t.Errorf("error stopping the Process: error %v", err)
	}
}

func Test_Tcp_StartCloseFactory2(t *testing.T) {
	ls, err := net.Listen("tcp", "127.0.0.1:9007")
	if assert.NoError(t, err) {
	} else {
		t.Skip("socket is busy")
	}

	cmd := exec.Command("php", "../../tests/client.php", "echo", "tcp")
	f := NewSocketServer(ls, time.Minute, log)
	defer func() {
		err = ls.Close()
		if err != nil {
			t.Errorf("error closing the listener: error %v", err)
		}
	}()

	w, err := f.SpawnWorker(cmd)
	assert.NoError(t, err)
	assert.NotNil(t, w)

	go func() {
		require.NoError(t, w.Wait())
	}()

	err = w.Stop()
	if err != nil {
		t.Errorf("error stopping the Process: error %v", err)
	}
}

func Test_Tcp_StartError2(t *testing.T) {
	ls, err := net.Listen("tcp", "127.0.0.1:9007")
	if assert.NoError(t, err) {
		defer func() {
			errC := ls.Close()
			if errC != nil {
				t.Errorf("error closing the listener: error %v", err)
			}
		}()
	} else {
		t.Skip("socket is busy")
	}

	cmd := exec.Command("php", "../../tests/client.php", "echo", "pipes")
	err = cmd.Start()
	if err != nil {
		t.Errorf("error executing the command: error %v", err)
	}

	w, err := NewSocketServer(ls, time.Minute, log).SpawnWorker(cmd)
	assert.Error(t, err)
	assert.Nil(t, w)
}

func Test_Tcp_Failboot2(t *testing.T) {
	ls, err := net.Listen("tcp", "127.0.0.1:9007")
	if assert.NoError(t, err) {
		defer func() {
			err3 := ls.Close()
			if err3 != nil {
				t.Errorf("error closing the listener: error %v", err3)
			}
		}()
	} else {
		t.Skip("socket is busy")
	}

	cmd := exec.Command("php", "../../tests/failboot.php")

	w, err2 := NewSocketServer(ls, time.Second*5, log).SpawnWorker(cmd)
	assert.Nil(t, w)
	assert.Error(t, err2)
}

func Test_Tcp_Invalid2(t *testing.T) {
	ls, err := net.Listen("tcp", "127.0.0.1:9007")
	if assert.NoError(t, err) {
		defer func() {
			errC := ls.Close()
			if errC != nil {
				t.Errorf("error closing the listener: error %v", err)
			}
		}()
	} else {
		t.Skip("socket is busy")
	}

	cmd := exec.Command("php", "../../tests/invalid.php")

	w, err := NewSocketServer(ls, time.Second*1, log).SpawnWorker(cmd)
	assert.Error(t, err)
	assert.Nil(t, w)
}

func Test_Tcp_Broken2(t *testing.T) {
	ls, err := net.Listen("tcp", "127.0.0.1:9007")
	if assert.NoError(t, err) {
		defer func() {
			errC := ls.Close()
			if errC != nil {
				t.Errorf("error closing the listener: error %v", err)
			}
		}()
	} else {
		t.Skip("socket is busy")
	}

	cmd := exec.Command("php", "../../tests/client.php", "broken", "tcp")

	w, err := NewSocketServer(ls, time.Minute, log).SpawnWorker(cmd)
	if err != nil {
		t.Fatal(err)
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		errW := w.Wait()
		assert.Error(t, errW)
	}()

	res, err := w.Exec(&payload.Payload{Body: []byte("hello")})
	assert.Error(t, err)
	assert.Nil(t, res)
	wg.Wait()

	time.Sleep(time.Second)
	err2 := w.Stop()
	// write tcp 127.0.0.1:9007->127.0.0.1:34204: use of closed network connection
	// but process exited
	assert.NoError(t, err2)
}

func Test_Tcp_Echo2(t *testing.T) {
	ls, err := net.Listen("tcp", "127.0.0.1:9007")
	if assert.NoError(t, err) {
		defer func() {
			errC := ls.Close()
			if errC != nil {
				t.Errorf("error closing the listener: error %v", err)
			}
		}()
	} else {
		t.Skip("socket is busy")
	}

	cmd := exec.Command("php", "../../tests/client.php", "echo", "tcp")

	w, _ := NewSocketServer(ls, time.Minute, log).SpawnWorker(cmd)
	go func() {
		assert.NoError(t, w.Wait())
	}()
	defer func() {
		err = w.Stop()
		if err != nil {
			t.Errorf("error stopping the Process: error %v", err)
		}
	}()

	res, err := w.Exec(&payload.Payload{Body: []byte("hello")})

	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.NotNil(t, res.Body)
	assert.Empty(t, res.Context)

	assert.Equal(t, "hello", res.String())
}

func Test_Unix_Start2(t *testing.T) {
	ls, err := net.Listen("unix", "sock.unix")
	assert.NoError(t, err)
	defer func() {
		err = ls.Close()
		assert.NoError(t, err)
	}()

	cmd := exec.Command("php", "../../tests/client.php", "echo", "unix")

	w, err := NewSocketServer(ls, time.Minute, log).SpawnWorker(cmd)
	assert.NoError(t, err)
	assert.NotNil(t, w)

	go func() {
		assert.NoError(t, w.Wait())
	}()

	err = w.Stop()
	if err != nil {
		t.Errorf("error stopping the Process: error %v", err)
	}
}

func Test_Unix_Failboot2(t *testing.T) {
	ls, err := net.Listen("unix", "sock.unix")
	assert.NoError(t, err)
	defer func() {
		err = ls.Close()
		assert.NoError(t, err)
	}()

	cmd := exec.Command("php", "../../tests/failboot.php")

	w, err := NewSocketServer(ls, time.Second*5, log).SpawnWorker(cmd)
	assert.Nil(t, w)
	assert.Error(t, err)
}

func Test_Unix_Timeout2(t *testing.T) {
	ls, err := net.Listen("unix", "sock.unix")
	assert.NoError(t, err)
	defer func() {
		err = ls.Close()
		assert.NoError(t, err)
	}()

	cmd := exec.Command("php", "../../tests/slow-client.php", "echo", "unix", "200", "0")

	w, err := NewSocketServer(ls, time.Millisecond*100, log).SpawnWorker(cmd)
	assert.Nil(t, w)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "relay timeout")
}

func Test_Unix_Invalid2(t *testing.T) {
	ls, err := net.Listen("unix", "sock.unix")
	assert.NoError(t, err)
	defer func() {
		err = ls.Close()
		assert.NoError(t, err)
	}()

	cmd := exec.Command("php", "../../tests/invalid.php")

	w, err := NewSocketServer(ls, time.Second*10, log).SpawnWorker(cmd)
	assert.Error(t, err)
	assert.Nil(t, w)
}

func Test_Unix_Broken2(t *testing.T) {
	ls, err := net.Listen("unix", "sock.unix")
	assert.NoError(t, err)
	defer func() {
		errC := ls.Close()
		assert.NoError(t, errC)
	}()

	cmd := exec.Command("php", "../../tests/client.php", "broken", "unix")

	w, err := NewSocketServer(ls, time.Minute, log).SpawnWorker(cmd)
	if err != nil {
		t.Fatal(err)
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		errW := w.Wait()
		assert.Error(t, errW)
	}()

	res, err := w.Exec(&payload.Payload{Body: []byte("hello")})

	assert.Error(t, err)
	assert.Nil(t, res)
	wg.Wait()

	time.Sleep(time.Second)
	err = w.Stop()
	assert.NoError(t, err)
}

func Test_Unix_Echo2(t *testing.T) {
	ls, err := net.Listen("unix", "sock.unix")
	assert.NoError(t, err)
	defer func() {
		err = ls.Close()
		assert.NoError(t, err)
	}()

	cmd := exec.Command("php", "../../tests/client.php", "echo", "unix")

	w, err := NewSocketServer(ls, time.Minute, log).SpawnWorker(cmd)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		assert.NoError(t, w.Wait())
	}()
	defer func() {
		err = w.Stop()
		if err != nil {
			t.Errorf("error stopping the Process: error %v", err)
		}
	}()

	res, err := w.Exec(&payload.Payload{Body: []byte("hello")})

	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.NotNil(t, res.Body)
	assert.Empty(t, res.Context)

	assert.Equal(t, "hello", res.String())
}

func Benchmark_Tcp_SpawnWorker_Stop2(b *testing.B) {
	ls, err := net.Listen("unix", "sock.unix")
	assert.NoError(b, err)
	defer func() {
		err = ls.Close()
		assert.NoError(b, err)
	}()

	f := NewSocketServer(ls, time.Minute, log)
	for n := 0; n < b.N; n++ {
		cmd := exec.Command("php", "../../tests/client.php", "echo", "tcp")

		w, err := f.SpawnWorker(cmd)
		if err != nil {
			b.Fatal(err)
		}
		go func() {
			assert.NoError(b, w.Wait())
		}()

		err = w.Stop()
		if err != nil {
			b.Errorf("error stopping the Process: error %v", err)
		}
	}
}

func Benchmark_Tcp_Worker_ExecEcho2(b *testing.B) {
	ls, err := net.Listen("unix", "sock.unix")
	assert.NoError(b, err)
	defer func() {
		err = ls.Close()
		assert.NoError(b, err)
	}()

	cmd := exec.Command("php", "../../tests/client.php", "echo", "tcp")

	w, err := NewSocketServer(ls, time.Minute, log).SpawnWorker(cmd)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err = w.Stop()
		if err != nil {
			b.Errorf("error stopping the Process: error %v", err)
		}
	}()

	for n := 0; n < b.N; n++ {
		if _, err := w.Exec(&payload.Payload{Body: []byte("hello")}); err != nil {
			b.Fail()
		}
	}
}

func Benchmark_Unix_SpawnWorker_Stop2(b *testing.B) {
	defer func() {
		_ = syscall.Unlink("sock.unix")
	}()
	ls, err := net.Listen("unix", "sock.unix")
	if err == nil {
		defer func() {
			errC := ls.Close()
			if errC != nil {
				b.Errorf("error closing the listener: error %v", err)
			}
		}()
	} else {
		b.Skip("socket is busy")
	}

	f := NewSocketServer(ls, time.Minute, log)
	for n := 0; n < b.N; n++ {
		cmd := exec.Command("php", "../../tests/client.php", "echo", "unix")

		w, err := f.SpawnWorker(cmd)
		if err != nil {
			b.Fatal(err)
		}
		err = w.Stop()
		if err != nil {
			b.Errorf("error stopping the Process: error %v", err)
		}
	}
}

func Benchmark_Unix_Worker_ExecEcho2(b *testing.B) {
	defer func() {
		_ = syscall.Unlink("sock.unix")
	}()
	ls, err := net.Listen("unix", "sock.unix")
	if err == nil {
		defer func() {
			errC := ls.Close()
			if errC != nil {
				b.Errorf("error closing the listener: error %v", err)
			}
		}()
	} else {
		b.Skip("socket is busy")
	}

	cmd := exec.Command("php", "../../tests/client.php", "echo", "unix")

	w, err := NewSocketServer(ls, time.Minute, log).SpawnWorker(cmd)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		err = w.Stop()
		if err != nil {
			b.Errorf("error stopping the Process: error %v", err)
		}
	}()

	for n := 0; n < b.N; n++ {
		if _, err := w.Exec(&payload.Payload{Body: []byte("hello")}); err != nil {
			b.Fail()
		}
	}
}
