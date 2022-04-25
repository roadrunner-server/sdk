package utils

type ContextKey struct {
	name string
}

func (ck *ContextKey) String() string {
	return ck.name
}

var (
	// OtelTracerNameKey is OpenTelemetry context key
	OtelTracerNameKey = &ContextKey{name: "tracer_name"} //nolint:gochecknoglobals

	// PsrContextKey is a context key. It can be used in the http attributes
	// immutable
	PsrContextKey = &ContextKey{"psr_attributes"} //nolint:gochecknoglobals
)
