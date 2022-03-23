package socket

import (
	"context"
	"net"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/roadrunner-server/api/v2/payload"
	workerImpl "github.com/roadrunner-server/sdk/v2/worker"
	"github.com/stretchr/testify/assert"
)

func Test_TcpStream_Broken(t *testing.T) {
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

	w, err := NewSocketServer(ls, time.Second*10, log).SpawnWorkerWithTimeout(ctx, cmd)
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

	sw := workerImpl.From(w)
	resp := make(chan *payload.Payload)
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
	err2 := w.Stop()
	// write tcp 127.0.0.1:9007->127.0.0.1:34204: use of closed network connection
	// but process is stopped
	assert.NoError(t, err2)
}

func Test_TcpStream_Echo(t *testing.T) {
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

	w, _ := NewSocketServer(ls, time.Minute, log).SpawnWorkerWithTimeout(ctx, cmd)
	go func() {
		assert.NoError(t, w.Wait())
	}()
	defer func() {
		err = w.Stop()
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

func Test_UnixStream_Broken(t *testing.T) {
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
	w, err := NewSocketServer(ls, time.Minute, log).SpawnWorkerWithTimeout(ctx, cmd)
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

	sw := workerImpl.From(w)
	resp := make(chan *payload.Payload)
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

	wg.Wait()
}

func Test_UnixStream_Echo(t *testing.T) {
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

	w, err := NewSocketServer(ls, time.Minute, log).SpawnWorkerWithTimeout(ctx, cmd)
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

func Test_TcpStream_Broken2(t *testing.T) {
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

	sw := workerImpl.From(w)
	resp := make(chan *payload.Payload)
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
	err2 := w.Stop()
	// write tcp 127.0.0.1:9007->127.0.0.1:34204: use of closed network connection
	// but process exited
	assert.NoError(t, err2)
}

func Test_TcpStream_Echo2(t *testing.T) {
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

func Test_UnixStream_Broken2(t *testing.T) {
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

	sw := workerImpl.From(w)
	resp := make(chan *payload.Payload)
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

func Test_UnixStream_Echo2(t *testing.T) {
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
