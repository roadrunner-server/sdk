package payload

import (
	"unsafe"
)

// Payload carries binary header and body to stack and
// back to the server.
type Payload struct {
	// Context represent payload context, might be omitted.
	Context []byte
	// body contains binary payload to be processed by WorkerProcess.
	Body []byte
	// Type of codec used to decode/encode payload.
	Codec byte
	// Flags
	Flags byte
}

// String returns payload body as string
func (p *Payload) String() string {
	if len(p.Body) == 0 {
		return ""
	}

	return unsafe.String(unsafe.SliceData(p.Body), len(p.Body))
}
