package pubsub

import (
	"github.com/goccy/go-json"
)

// Msg represents a single message with payload bound to a particular topic
type Msg struct {
	// Topic (channel in terms of redis)
	Topic_ string `json:"topic"`
	// Payload (on some decode stages might be represented as base64 string)
	Payload_ []byte `json:"payload"`
}

func (m *Msg) MarshalBinary() (data []byte, err error) {
	return json.Marshal(m)
}

func (m *Msg) Topic() string {
	return m.Topic_
}

func (m *Msg) Payload() []byte {
	return m.Payload_
}
