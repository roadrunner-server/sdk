package worker

import (
	"testing"

	"github.com/roadrunner-server/api/v2/worker"
	"github.com/stretchr/testify/assert"
)

func Test_NewState(t *testing.T) {
	st := NewWorkerState(worker.StateErrored)

	assert.Equal(t, "errored", st.String())

	assert.Equal(t, "inactive", NewWorkerState(worker.StateInactive).String())
	assert.Equal(t, "ready", NewWorkerState(worker.StateReady).String())
	assert.Equal(t, "working", NewWorkerState(worker.StateWorking).String())
	assert.Equal(t, "stopped", NewWorkerState(worker.StateStopped).String())
	assert.Equal(t, "undefined", NewWorkerState(1000).String())
}

func Test_IsActive(t *testing.T) {
	assert.False(t, NewWorkerState(worker.StateInactive).IsActive())
	assert.True(t, NewWorkerState(worker.StateReady).IsActive())
	assert.True(t, NewWorkerState(worker.StateWorking).IsActive())
	assert.False(t, NewWorkerState(worker.StateStopped).IsActive())
	assert.False(t, NewWorkerState(worker.StateErrored).IsActive())
}
