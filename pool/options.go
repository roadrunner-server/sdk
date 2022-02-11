package pool

import (
	"go.uber.org/zap"
)

func WithLogger(z *zap.Logger) Options {
	return func(p *StaticPool) {
		p.log = z
	}
}

func WithCustomErrEncoder(errEnc ErrorEncoder) Options {
	return func(p *StaticPool) {
		p.errEncoder = errEnc
	}
}

func UseParallelAlloc() Options {
	return func(p *StaticPool) {
		p.parallelAlloc = true
	}
}
