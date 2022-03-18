package pool

import (
	"context"

	"github.com/roadrunner-server/api/v2/payload"
	"github.com/roadrunner-server/api/v2/pool"
)

func (sp *supervised) ExecStream(p *payload.Payload, resp chan *payload.Payload) error {
	return sp.pool.(pool.Streamer).ExecStream(p, resp)
}

func (sp *supervised) ExecStreamWithTTL(ctx context.Context, p *payload.Payload, resp chan *payload.Payload) error {
	if sp.cfg.ExecTTL == 0 {
		sp.log.Warn("incorrect supervisor ExecWithTTL method usage. ExecTTL should be set. Fallback to the pool.Exec method")
		return sp.pool.(pool.Streamer).ExecStream(p, resp)
	}

	ctx, cancel := context.WithTimeout(ctx, sp.cfg.ExecTTL)
	defer cancel()

	err := sp.pool.(pool.Streamer).ExecStreamWithTTL(ctx, p, resp)
	if err != nil {
		return err
	}

	return nil
}
