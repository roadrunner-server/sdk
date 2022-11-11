package fork_pool //nolint:stylecheck

import (
	"go.uber.org/zap"
)

type Options func(p *MasterWorker)

func WithLogger(z *zap.Logger) Options {
	return func(p *MasterWorker) {
		p.log = z
	}
}
