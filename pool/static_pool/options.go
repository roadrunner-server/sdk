package static_pool //nolint:stylecheck

import (
	"go.uber.org/zap"
)

type Options func(p *Pool)

func WithLogger(z *zap.Logger) Options {
	return func(p *Pool) {
		p.log = z
	}
}

func WithQueueSize(l uint64) Options {
	return func(p *Pool) {
		p.maxQueueSize = l
	}
}
