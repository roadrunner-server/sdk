package fsm

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func Test_NewState(t *testing.T) {
	log, err := zap.NewDevelopment()
	assert.NoError(t, err)
	st := NewFSM(StateErrored, log)

	assert.Equal(t, "errored", st.String())

	assert.Equal(t, "inactive", NewFSM(StateInactive, log).String())
	assert.Equal(t, "ready", NewFSM(StateReady, log).String())
	assert.Equal(t, "working", NewFSM(StateWorking, log).String())
	assert.Equal(t, "stopped", NewFSM(StateStopped, log).String())
	assert.Equal(t, "undefined", NewFSM(1000, log).String())
}

func Test_IsActive(t *testing.T) {
	log, err := zap.NewDevelopment()
	assert.NoError(t, err)
	assert.False(t, NewFSM(StateInactive, log).IsActive())
	assert.True(t, NewFSM(StateReady, log).IsActive())
	assert.True(t, NewFSM(StateWorking, log).IsActive())
	assert.False(t, NewFSM(StateStopped, log).IsActive())
	assert.False(t, NewFSM(StateErrored, log).IsActive())
}
