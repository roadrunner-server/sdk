package pool

import (
	"os/exec"
)

// Command is a function that returns a new exec.Cmd instance for the given command string.
type Command func(cmd string) *exec.Cmd
