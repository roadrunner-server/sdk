package fsm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_NewState(t *testing.T) {
	st := NewFSM(StateErrored)

	assert.Equal(t, "errored", st.String())

	assert.Equal(t, "inactive", NewFSM(StateInactive).String())
	assert.Equal(t, "ready", NewFSM(StateReady).String())
	assert.Equal(t, "working", NewFSM(StateWorking).String())
	assert.Equal(t, "stopped", NewFSM(StateStopped).String())
	assert.Equal(t, "undefined", NewFSM(1000).String())
}

func Test_IsActive(t *testing.T) {
	assert.False(t, NewFSM(StateInactive).IsActive())
	assert.True(t, NewFSM(StateReady).IsActive())
	assert.True(t, NewFSM(StateWorking).IsActive())
	assert.False(t, NewFSM(StateStopped).IsActive())
	assert.False(t, NewFSM(StateErrored).IsActive())
}
