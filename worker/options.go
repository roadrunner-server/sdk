package worker

import (
	"math/rand"

	"go.uber.org/zap"
)

const (
	maxExecsPercentJitter uint64 = 15
)

type Options func(p *Process)

func WithLog(z *zap.Logger) Options {
	return func(p *Process) {
		p.log = z
	}
}

func WithMaxExecs(maxExecs uint64) Options {
	return func(p *Process) {
		p.maxExecs = calculateMaxExecsJitter(maxExecs, maxExecsPercentJitter)
	}
}

func calculateMaxExecsJitter(maxExecs, jitter uint64) uint64 {
	if maxExecs == 0 {
		return 0
	}

	percent := rand.Intn(int(jitter)) //nolint:gosec

	if percent == 0 {
		return maxExecs
	}

	result := (float64(maxExecs) * float64(percent)) / 100.0

	return maxExecs + uint64(result)
}
