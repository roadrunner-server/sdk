package pool

import (
	"os/exec"
)

const (
	// StopRequest can be sent by worker to indicate that restart is required.
	StopRequest = `{"stop":true}`
)

type Command func(cmd string) *exec.Cmd
