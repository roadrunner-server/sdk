package fsm

// All worker states
const (
	// StateInactive - no associated process
	StateInactive int64 = iota
	// StateReady - ready for job.
	StateReady
	// StateWorking - working on given payload.
	StateWorking
	// StateInvalid - indicates that WorkerProcess is being disabled and will be removed.
	StateInvalid
	// StateStopping - process is being softly stopped.
	StateStopping
	// StateStopped - process has been terminated.
	StateStopped
	// StateDestroyed State of worker, when no need to allocate new one
	StateDestroyed
	// StateMaxJobsReached State of worker, when it reached executions limit
	StateMaxJobsReached
	// StateErrored - error StateImpl (can't be used).
	StateErrored
	// StateIdleTTLReached - worker idle TTL was reached
	StateIdleTTLReached
	// StateTTLReached - worker TTL was reached
	StateTTLReached
	// StateMaxMemoryReached - worker max memory limit was reached
	StateMaxMemoryReached
	// StateExecTTLReached - worker execution TTL was reached
	StateExecTTLReached
)
