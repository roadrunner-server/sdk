package internal

import (
	"os"
	"sync"

	"github.com/goccy/go-json"
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/goridge/v3/pkg/frame"
	"github.com/roadrunner-server/goridge/v3/pkg/relay"
)

type StopCommand struct {
	Stop bool `json:"stop"`
}

type pidCommand struct {
	Pid int `json:"pid"`
}

type forkCommand struct {
	Fork int `json:"fork"`
}

var fPool = sync.Pool{New: func() any {
	return frame.NewFrame()
}}

func getFrame() *frame.Frame {
	return fPool.Get().(*frame.Frame)
}

func putFrame(f *frame.Frame) {
	f.Reset()
	fPool.Put(f)
}

func SendControl(rl relay.Relay, payload any) error {
	fr := getFrame()
	defer putFrame(fr)

	fr.WriteVersion(fr.Header(), frame.Version1)
	fr.WriteFlags(fr.Header(), frame.CONTROL, frame.CodecJSON)

	if data, ok := payload.([]byte); ok {
		// check if payload no more that 4Gb
		if uint32(len(data)) > ^uint32(0) {
			return errors.Str("payload is more that 4gb")
		}

		fr.WritePayloadLen(fr.Header(), uint32(len(data)))
		fr.WritePayload(data)
		fr.WriteCRC(fr.Header())

		err := rl.Send(fr)
		if err != nil {
			return err
		}
		return nil
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return errors.Errorf("invalid payload: %s", err)
	}

	fr.WritePayloadLen(fr.Header(), uint32(len(data)))
	fr.WritePayload(data)
	fr.WriteCRC(fr.Header())

	// we don't need a copy here, because frame copy the data before send
	err = rl.Send(fr)
	if err != nil {
		return err
	}

	return nil
}

func Pid(rl relay.Relay) (int64, error) {
	err := SendControl(rl, pidCommand{Pid: os.Getpid()})
	if err != nil {
		return 0, err
	}

	fr := getFrame()
	defer putFrame(fr)

	err = rl.Receive(fr)
	if err != nil {
		return 0, err
	}

	if fr == nil {
		return 0, errors.Str("nil frame received")
	}

	flags := fr.ReadFlags()

	if flags&frame.CONTROL == 0 {
		return 0, errors.Str("unexpected response, header is missing, no CONTROL flag")
	}

	link := &pidCommand{}
	err = json.Unmarshal(fr.Payload(), link)
	if err != nil {
		return 0, err
	}

	if link.Pid <= 0 {
		return 0, errors.Str("pid should be greater than 0")
	}

	return int64(link.Pid), nil
}

func Fork(rl relay.Relay) (int, error) {
	err := SendControl(rl, forkCommand{Fork: os.Getpid()})
	if err != nil {
		return 0, err
	}

	fr := getFrame()
	defer putFrame(fr)

	err = rl.Receive(fr)
	if err != nil {
		return 0, err
	}

	if fr == nil {
		return 0, errors.Str("nil frame received")
	}

	flags := fr.ReadFlags()

	if flags&frame.CONTROL == 0 {
		return 0, errors.Str("unexpected response, header is missing, no CONTROL flag")
	}

	// we sent a forkCommand, but receiving just a PID (child)
	link := &pidCommand{}
	err = json.Unmarshal(fr.Payload(), link)
	if err != nil {
		return 0, err
	}

	if link.Pid == 0 {
		return 0, errors.Str("pid should be greater than 0")
	}

	return link.Pid, nil
}
