package event

type Resendable interface {
	PossibleDuplicate() bool
}
