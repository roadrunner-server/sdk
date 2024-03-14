package socket

import (
	"context"
	"net"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/sdk/v4/payload"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Tcp_Start(t *testing.T) {
	ctx := context.Background()
	time.Sleep(time.Millisecond * 10) // to ensure free socket

	ls, err := net.Listen("tcp", "127.0.0.1:9007")
	if assert.NoError(t, err) {
		defer func() {
			err = ls.Close()
			if err != nil {
				t.Errorf("error closing the listener: error %v", err)
			}
		}()
	} else {
		t.Skip("socket is busy")
	}

	cmd := exec.Command("php", "../../tests/client.php", "echo", "tcp")

	w, err := NewSocketServer(ls, log).SpawnWorkerWithContext(ctx, cmd)
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

func Test_Tcp_StartCloseFactory(t *testing.T) {
	time.Sleep(time.Millisecond * 10) // to ensure free socket
	ctx := context.Background()
	ls, err := net.Listen("tcp", "127.0.0.1:9007")
	if assert.NoError(t, err) {
	} else {
		t.Skip("socket is busy")
	}

	cmd := exec.Command("php", "../../tests/client.php", "echo", "tcp")
	f := NewSocketServer(ls, log)
	defer func() {
		err = ls.Close()
		if err != nil {
			t.Errorf("error closing the listener: error %v", err)
		}
	}()

	w, err := f.SpawnWorkerWithContext(ctx, cmd)
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

func Test_Tcp_StartError(t *testing.T) {
	time.Sleep(time.Millisecond * 10) // to ensure free socket
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()

	ls, err := net.Listen("tcp", "127.0.0.1:9007")
	if assert.NoError(t, err) {
		defer func() {
			err = ls.Close()
			if err != nil {
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

	serv := NewSocketServer(ls, log)
	time.Sleep(time.Second * 2)
	w, err := serv.SpawnWorkerWithContext(ctx, cmd)
	assert.Error(t, err)
	assert.Nil(t, w)
}

func Test_Tcp_Failboot(t *testing.T) {
	time.Sleep(time.Millisecond * 10) // to ensure free socket
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

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

	w, err2 := NewSocketServer(ls, log).SpawnWorkerWithContext(ctx, cmd)
	assert.Nil(t, w)
	assert.Error(t, err2)
}

func Test_Tcp_Timeout(t *testing.T) {
	time.Sleep(time.Millisecond * 10) // to ensure free socket
	ctx, cancel := context.WithTimeout(context.Background(), time.Microsecond)
	defer cancel()

	ls, err := net.Listen("tcp", "127.0.0.1:9007")
	if assert.NoError(t, err) {
		defer func() {
			err = ls.Close()
			if err != nil {
				t.Errorf("error closing the listener: error %v", err)
			}
		}()
	} else {
		t.Skip("socket is busy")
	}

	cmd := exec.Command("php", "../../tests/slow-client.php", "echo", "tcp", "200", "0")

	w, err := NewSocketServer(ls, log).SpawnWorkerWithContext(ctx, cmd)
	assert.Nil(t, w)
	assert.Error(t, err)
	assert.True(t, errors.Is(errors.TimeOut, err))
}

func Test_Tcp_Invalid(t *testing.T) {
	time.Sleep(time.Millisecond * 10) // to ensure free socket
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ls, err := net.Listen("tcp", "127.0.0.1:9007")
	if assert.NoError(t, err) {
		defer func() {
			err = ls.Close()
			if err != nil {
				t.Errorf("error closing the listener: error %v", err)
			}
		}()
	} else {
		t.Skip("socket is busy")
	}

	cmd := exec.Command("php", "../../tests/invalid.php")

	w, err := NewSocketServer(ls, log).SpawnWorkerWithContext(ctx, cmd)
	assert.Error(t, err)
	assert.Nil(t, w)
}

func Test_Tcp_Broken(t *testing.T) {
	time.Sleep(time.Millisecond * 10) // to ensure free socket
	ctx := context.Background()
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

	w, err := NewSocketServer(ls, log).SpawnWorkerWithContext(ctx, cmd)
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

	res, err := w.Exec(context.Background(), &payload.Payload{Body: []byte("hello")})
	assert.Error(t, err)
	assert.Nil(t, res)
	wg.Wait()

	time.Sleep(time.Second)
	err2 := w.Stop()
	// write tcp 127.0.0.1:9007->127.0.0.1:34204: use of closed network connection
	// but process is stopped
	assert.NoError(t, err2)
}

func Test_Tcp_Echo(t *testing.T) {
	time.Sleep(time.Millisecond * 10) // to ensure free socket
	ctx := context.Background()
	ls, err := net.Listen("tcp", "127.0.0.1:9007")
	if assert.NoError(t, err) {
		defer func() {
			err = ls.Close()
			if err != nil {
				t.Errorf("error closing the listener: error %v", err)
			}
		}()
	} else {
		t.Skip("socket is busy")
	}

	cmd := exec.Command("php", "../../tests/client.php", "echo", "tcp")

	w, _ := NewSocketServer(ls, log).SpawnWorkerWithContext(ctx, cmd)
	go func() {
		assert.NoError(t, w.Wait())
	}()
	defer func() {
		err = w.Stop()
		if err != nil {
			t.Errorf("error stopping the Process: error %v", err)
		}
	}()

	res, err := w.Exec(context.Background(), &payload.Payload{Body: []byte("hello")})

	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.NotNil(t, res.Body)
	assert.Empty(t, res.Context)

	assert.Equal(t, "hello", res.String())
}

func Test_Unix_Start(t *testing.T) {
	ctx := context.Background()
	ls, err := net.Listen("unix", "sock.unix")
	if err == nil {
		defer func() {
			err = ls.Close()
			if err != nil {
				t.Errorf("error closing the listener: error %v", err)
			}
		}()
	} else {
		t.Skip("socket is busy")
	}

	cmd := exec.Command("php", "../../tests/client.php", "echo", "unix")

	w, err := NewSocketServer(ls, log).SpawnWorkerWithContext(ctx, cmd)
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

func Test_Unix_Failboot(t *testing.T) {
	ls, err := net.Listen("unix", "sock.unix")
	if err == nil {
		defer func() {
			err = ls.Close()
			if err != nil {
				t.Errorf("error closing the listener: error %v", err)
			}
		}()
	} else {
		t.Skip("socket is busy")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	cmd := exec.Command("php", "../../tests/failboot.php")

	w, err := NewSocketServer(ls, log).SpawnWorkerWithContext(ctx, cmd)
	assert.Nil(t, w)
	assert.Error(t, err)
}

func Test_Unix_Timeout(t *testing.T) {
	ls, err := net.Listen("unix", "sock.unix")
	if err == nil {
		defer func() {
			err = ls.Close()
			if err != nil {
				t.Errorf("error closing the listener: error %v", err)
			}
		}()
	} else {
		t.Skip("socket is busy")
	}

	cmd := exec.Command("php", "../../tests/slow-client.php", "echo", "unix", "200", "0")
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()

	w, err := NewSocketServer(ls, log).SpawnWorkerWithContext(ctx, cmd)
	assert.Nil(t, w)
	assert.Error(t, err)
	assert.True(t, errors.Is(errors.TimeOut, err))
}

func Test_Unix_Invalid(t *testing.T) {
	ls, err := net.Listen("unix", "sock.unix")
	if err == nil {
		defer func() {
			err = ls.Close()
			if err != nil {
				t.Errorf("error closing the listener: error %v", err)
			}
		}()
	} else {
		t.Skip("socket is busy")
	}

	cmd := exec.Command("php", "../../tests/invalid.php")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	w, err := NewSocketServer(ls, log).SpawnWorkerWithContext(ctx, cmd)
	assert.Error(t, err)
	assert.Nil(t, w)
}

func Test_Unix_Broken(t *testing.T) {
	ctx := context.Background()
	ls, err := net.Listen("unix", "sock.unix")
	if err == nil {
		defer func() {
			errC := ls.Close()
			if errC != nil {
				t.Errorf("error closing the listener: error %v", err)
			}
		}()
	} else {
		t.Skip("socket is busy")
	}

	cmd := exec.Command("php", "../../tests/client.php", "broken", "unix")
	w, err := NewSocketServer(ls, log).SpawnWorkerWithContext(ctx, cmd)
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

	res, err := w.Exec(context.Background(), &payload.Payload{Body: []byte("hello")})

	assert.Error(t, err)
	assert.Nil(t, res)

	time.Sleep(time.Second)
	err = w.Stop()
	assert.NoError(t, err)

	wg.Wait()
}

func Test_Unix_Echo(t *testing.T) {
	ctx := context.Background()
	ls, err := net.Listen("unix", "sock.unix")
	if err == nil {
		defer func() {
			err = ls.Close()
			if err != nil {
				t.Errorf("error closing the listener: error %v", err)
			}
		}()
	} else {
		t.Skip("socket is busy")
	}

	cmd := exec.Command("php", "../../tests/client.php", "echo", "unix")

	w, err := NewSocketServer(ls, log).SpawnWorkerWithContext(ctx, cmd)
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

	res, err := w.Exec(context.Background(), &payload.Payload{Body: []byte("hello")})

	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.NotNil(t, res.Body)
	assert.Empty(t, res.Context)

	assert.Equal(t, "hello", res.String())
}

func Benchmark_Tcp_SpawnWorker_Stop(b *testing.B) {
	ctx := context.Background()
	ls, err := net.Listen("tcp", "127.0.0.1:9007")
	if err == nil {
		defer func() {
			err = ls.Close()
			if err != nil {
				b.Errorf("error closing the listener: error %v", err)
			}
		}()
	} else {
		b.Skip("socket is busy")
	}

	f := NewSocketServer(ls, log)
	for n := 0; n < b.N; n++ {
		cmd := exec.Command("php", "../../tests/client.php", "echo", "tcp")

		w, err := f.SpawnWorkerWithContext(ctx, cmd)
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

func Benchmark_Tcp_Worker_ExecEcho(b *testing.B) {
	ctx := context.Background()
	ls, err := net.Listen("tcp", "127.0.0.1:9007")
	if err == nil {
		defer func() {
			err = ls.Close()
			if err != nil {
				b.Errorf("error closing the listener: error %v", err)
			}
		}()
	} else {
		b.Skip("socket is busy")
	}

	cmd := exec.Command("php", "../../tests/client.php", "echo", "tcp")

	w, err := NewSocketServer(ls, log).SpawnWorkerWithContext(ctx, cmd)
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
		if _, err := w.Exec(context.Background(), &payload.Payload{Body: []byte("hello")}); err != nil {
			b.Fail()
		}
	}
}

func Benchmark_Unix_SpawnWorker_Stop(b *testing.B) {
	ctx := context.Background()
	ls, err := net.Listen("unix", "sock.unix")
	if err == nil {
		defer func() {
			err = ls.Close()
			if err != nil {
				b.Errorf("error closing the listener: error %v", err)
			}
		}()
	} else {
		b.Skip("socket is busy")
	}

	f := NewSocketServer(ls, log)
	for n := 0; n < b.N; n++ {
		cmd := exec.Command("php", "../../tests/client.php", "echo", "unix")

		w, err := f.SpawnWorkerWithContext(ctx, cmd)
		if err != nil {
			b.Fatal(err)
		}
		err = w.Stop()
		if err != nil {
			b.Errorf("error stopping the Process: error %v", err)
		}
	}
}

func Benchmark_Unix_Worker_ExecEcho(b *testing.B) {
	ctx := context.Background()
	ls, err := net.Listen("unix", "sock.unix")
	if err == nil {
		defer func() {
			err = ls.Close()
			if err != nil {
				b.Errorf("error closing the listener: error %v", err)
			}
		}()
	} else {
		b.Skip("socket is busy")
	}

	cmd := exec.Command("php", "../../tests/client.php", "echo", "unix")

	w, err := NewSocketServer(ls, log).SpawnWorkerWithContext(ctx, cmd)
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
		if _, err := w.Exec(context.Background(), &payload.Payload{Body: []byte("hello")}); err != nil {
			b.Fail()
		}
	}
}
