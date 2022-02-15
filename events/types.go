package events

import (
	"fmt"
)

type event struct {
	// event typ
	typ fmt.Stringer
	// plugin
	plugin string
	// message
	message string
}

// NewEvent initializes new event
func NewEvent(t fmt.Stringer, plugin string, message string) *event {
	if t.String() == "" || plugin == "" {
		return nil
	}

	return &event{
		typ:     t,
		plugin:  plugin,
		message: message,
	}
}

func (r *event) Type() fmt.Stringer {
	return r.typ
}

func (r *event) Message() string {
	return r.message
}

func (r *event) Plugin() string {
	return r.plugin
}
