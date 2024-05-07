package heimdall

type entity interface {
	Checkpoint | Milestone | Span
}
