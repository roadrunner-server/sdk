package pool

import (
	"context"
	"sync"
	"time"

	"github.com/roadrunner-server/api/v2/payload"
	"github.com/roadrunner-server/api/v2/pool"
	"github.com/roadrunner-server/api/v2/worker"
	"github.com/roadrunner-server/sdk/v2/events"
	"github.com/roadrunner-server/sdk/v2/state/process"
	"go.uber.org/zap"
)

const (
	MB = 1024 * 1024
)

// NSEC_IN_SEC nanoseconds in second
const NSEC_IN_SEC int64 = 1000000000 //nolint:stylecheck

type supervised struct {
	cfg    *SupervisorConfig
	pool   pool.Pool
	log    *zap.Logger
	stopCh chan struct{}
	mu     *sync.RWMutex
}

func supervisorWrapper(pool pool.Pool, log *zap.Logger, cfg *SupervisorConfig) *supervised {
	sp := &supervised{
		cfg:    cfg,
		pool:   pool,
		log:    log,
		mu:     &sync.RWMutex{},
		stopCh: make(chan struct{}),
	}

	return sp
}

func (sp *supervised) ExecWithTTL(ctx context.Context, pld *payload.Payload) (*payload.Payload, error) {
	if sp.cfg.ExecTTL == 0 {
		sp.log.Warn("incorrect supervisor ExecWithTTL method usage. ExecTTL should be set. Fallback to the pool.Exec method")
		return sp.pool.Exec(pld)
	}

	ctx, cancel := context.WithTimeout(ctx, sp.cfg.ExecTTL)
	defer cancel()

	res, err := sp.pool.ExecWithTTL(ctx, pld)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (sp *supervised) Exec(rqs *payload.Payload) (*payload.Payload, error) {
	// to be compatible with the old behavior where we only used an Exec method
	if sp.cfg.ExecTTL != 0 {
		return sp.ExecWithTTL(context.Background(), rqs)
	}
	return sp.pool.Exec(rqs)
}

func (sp *supervised) Reset(ctx context.Context) error {
	return sp.pool.Reset(ctx)
}

func (sp *supervised) GetConfig() interface{} {
	return sp.pool.GetConfig()
}

func (sp *supervised) Workers() (workers []worker.BaseProcess) {
	return sp.pool.Workers()
}

func (sp *supervised) QueueSize() uint64 {
	queuer, ok := sp.pool.(pool.Queuer)
	if ok {
		return queuer.QueueSize()
	}

	sp.log.Warn("not implemented")
	return 0
}

func (sp *supervised) RemoveWorker(worker worker.BaseProcess) error {
	return sp.pool.RemoveWorker(worker)
}

func (sp *supervised) Destroy(ctx context.Context) {
	sp.Stop()
	sp.mu.Lock()
	sp.pool.Destroy(ctx)
	sp.mu.Unlock()
}

func (sp *supervised) Start() {
	go func() {
		watchTout := time.NewTicker(sp.cfg.WatchTick)
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

func (sp *supervised) Stop() {
	sp.stopCh <- struct{}{}
}

func (sp *supervised) control() {
	now := time.Now()

	// MIGHT BE OUTDATED
	// It's a copy of the Workers pointers
	workers := sp.pool.Workers()

	for i := 0; i < len(workers); i++ {
		// if worker not in the Ready OR working state
		// skip such worker
		switch workers[i].State().Value() {
		case
			worker.StateInvalid,
			worker.StateErrored,
			worker.StateDestroyed,
			worker.StateInactive,
			worker.StateStopped,
			worker.StateStopping,
			worker.StateKilling:

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

		if sp.cfg.TTL != 0 && now.Sub(workers[i].Created()).Seconds() >= sp.cfg.TTL.Seconds() {
			/*
				worker at this point might be in the middle of request execution:

				---> REQ ---> WORKER -----------------> RESP (at this point we should not set the Ready state) ------> | ----> Worker gets between supervisor checks and get killed in the ww.Release
											 ^
				                           TTL Reached, state - invalid                                                |
																														-----> Worker Stopped here
			*/
			workers[i].State().Set(worker.StateInvalid)
			sp.log.Debug("ttl", zap.String("reason", "ttl is reached"), zap.Int64("pid", workers[i].Pid()), zap.String("internal_event_name", events.EventTTL.String()))
			continue
		}

		if sp.cfg.MaxWorkerMemory != 0 && s.MemoryUsage >= sp.cfg.MaxWorkerMemory*MB {
			/*
				worker at this point might be in the middle of request execution:

				---> REQ ---> WORKER -----------------> RESP (at this point we should not set the Ready state) ------> | ----> Worker gets between supervisor checks and get killed in the ww.Release
											 ^
				                           TTL Reached, state - invalid                                                |
																														-----> Worker Stopped here
			*/
			workers[i].State().Set(worker.StateInvalid)
			sp.log.Debug("memory_limit", zap.String("reason", "max memory is reached"), zap.Int64("pid", workers[i].Pid()), zap.String("internal_event_name", events.EventMaxMemory.String()))
			continue
		}

		// firs we check maxWorker idle
		if sp.cfg.IdleTTL != 0 {
			// then check for the worker state
			if workers[i].State().Value() != worker.StateReady {
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
			if int64(sp.cfg.IdleTTL.Seconds())-res <= 0 {
				/*
					worker at this point might be in the middle of request execution:

					---> REQ ---> WORKER -----------------> RESP (at this point we should not set the Ready state) ------> | ----> Worker gets between supervisor checks and get killed in the ww.Release
												 ^
					                           TTL Reached, state - invalid                                                |
																															-----> Worker Stopped here
				*/

				workers[i].State().Set(worker.StateInvalid)
				sp.log.Debug("idle_ttl", zap.String("reason", "idle ttl is reached"), zap.Int64("pid", workers[i].Pid()), zap.String("internal_event_name", events.EventTTL.String()))
			}
		}
	}
}
