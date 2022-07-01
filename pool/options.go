package pool

import (
	"go.uber.org/zap"
)

func WithLogger(z *zap.Logger) Options {
	return func(p *Pool) {
		p.log = z
	}
}
