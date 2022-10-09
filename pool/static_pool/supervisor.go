package static_pool //nolint:stylecheck

import (
	"time"

	"github.com/roadrunner-server/sdk/v3/events"
	"github.com/roadrunner-server/sdk/v3/state/process"
	"github.com/roadrunner-server/sdk/v3/worker/fsm"
	"go.uber.org/zap"
)

const (
	MB = 1024 * 1024

	// NSEC_IN_SEC nanoseconds in second
	NSEC_IN_SEC int64 = 1000000000 //nolint:stylecheck
)

func (sp *Pool) Start() {
	go func() {
		watchTout := time.NewTicker(sp.cfg.Supervisor.WatchTick)
		defer watchTout.Stop()

		for {
			select {
			case <-sp.stopCh:
				return
			// stop here
			case <-watchTout.C:
				sp.mu.Lock()
				sp.control()
				sp.mu.Unlock()
			}
		}
	}()
}

func (sp *Pool) Stop() {
	sp.stopCh <- struct{}{}
}

func (sp *Pool) control() {
	now := time.Now()

	// MIGHT BE OUTDATED
	// It's a copy of the Workers pointers
	workers := sp.Workers()

	for i := 0; i < len(workers); i++ {
		// if worker not in the Ready OR working state
		// skip such worker
		switch workers[i].State().CurrentState() {
		case
			fsm.StateInvalid,
			fsm.StateErrored,
			fsm.StateDestroyed,
			fsm.StateInactive,
			fsm.StateStopped,
			fsm.StateStopping,
			fsm.StateMaxJobsReached,
			fsm.StateKilling:

			// stop the bad worker
			if workers[i] != nil {
				_ = workers[i].Stop()
			}
			continue
		}

		s, err := process.WorkerProcessState(workers[i])
		if err != nil {
			// worker not longer valid for supervision
			continue
		}

		if sp.cfg.Supervisor.TTL != 0 && now.Sub(workers[i].Created()).Seconds() >= sp.cfg.Supervisor.TTL.Seconds() {
			/*
				worker at this point might be in the middle of request execution:

				---> REQ ---> WORKER -----------------> RESP (at this point we should not set the Ready state) ------> | ----> Worker gets between supervisor checks and get killed in the ww.Release
											 ^
				                           TTL Reached, state - invalid                                                |
																														-----> Worker Stopped here
			*/
			workers[i].State().Transition(fsm.StateInvalid)
			sp.log.Debug("ttl", zap.String("reason", "ttl is reached"), zap.Int64("pid", workers[i].Pid()), zap.String("internal_event_name", events.EventTTL.String()))
			continue
		}

		if sp.cfg.Supervisor.MaxWorkerMemory != 0 && s.MemoryUsage >= sp.cfg.Supervisor.MaxWorkerMemory*MB {
			/*
				worker at this point might be in the middle of request execution:

				---> REQ ---> WORKER -----------------> RESP (at this point we should not set the Ready state) ------> | ----> Worker gets between supervisor checks and get killed in the ww.Release
											 ^
				                           TTL Reached, state - invalid                                                |
																														-----> Worker Stopped here
			*/
			workers[i].State().Transition(fsm.StateInvalid)
			sp.log.Debug("memory_limit", zap.String("reason", "max memory is reached"), zap.Int64("pid", workers[i].Pid()), zap.String("internal_event_name", events.EventMaxMemory.String()))
			continue
		}

		// firs we check maxWorker idle
		if sp.cfg.Supervisor.IdleTTL != 0 {
			// then check for the worker state
			if !workers[i].State().Compare(fsm.StateReady) {
				continue
			}

			/*
				Calculate idle time
				If worker in the StateReady, we read it LastUsed timestamp as UnixNano uint64
				2. For example maxWorkerIdle is equal to 5sec, then, if (time.Now - LastUsed) > maxWorkerIdle
				we are guessing that worker overlap idle time and has to be killed
			*/

			// 1610530005534416045 lu
			// lu - now = -7811150814 - nanoseconds
			// 7.8 seconds
			// get last used unix nano
			lu := workers[i].State().LastUsed()
			// worker not used, skip
			if lu == 0 {
				continue
			}

			// convert last used to unixNano and sub time.now to seconds
			// negative number, because lu always in the past, except for the `back to the future` :)
			res := ((int64(lu) - now.UnixNano()) / NSEC_IN_SEC) * -1

			// maxWorkerIdle more than diff between now and last used
			// for example:
			// After exec worker goes to the rest
			// And resting for the 5 seconds
			// IdleTTL is 1 second.
			// After the control check, res will be 5, idle is 1
			// 5 - 1 = 4, more than 0, YOU ARE FIRED (removed). Done.
			if int64(sp.cfg.Supervisor.IdleTTL.Seconds())-res <= 0 {
				/*
					worker at this point might be in the middle of request execution:

					---> REQ ---> WORKER -----------------> RESP (at this point we should not set the Ready state) ------> | ----> Worker gets between supervisor checks and get killed in the ww.Release
												 ^
					                           TTL Reached, state - invalid                                                |
																															-----> Worker Stopped here
				*/

				workers[i].State().Transition(fsm.StateInvalid)
				sp.log.Debug("idle_ttl", zap.String("reason", "idle ttl is reached"), zap.Int64("pid", workers[i].Pid()), zap.String("internal_event_name", events.EventTTL.String()))
			}
		}
	}
}
