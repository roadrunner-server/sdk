package static_pool //nolint:stylecheck

import "github.com/roadrunner-server/sdk/v4/payload"

type PExec struct {
	pld *payload.Payload
	err error
}

func newPExec(pld *payload.Payload, err error) *PExec {
	return &PExec{
		pld: pld,
		err: err,
	}
}

func (p *PExec) Payload() *payload.Payload {
	return p.pld
}

func (p *PExec) Body() []byte {
	return p.pld.Body
}

func (p *PExec) Context() []byte {
	return p.pld.Context
}

func (p *PExec) Error() error {
	return p.err
}
