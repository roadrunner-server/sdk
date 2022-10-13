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
