package observer

import "fmt"

type InterrogationErrorID int

const (
	InterrogationErrorPing InterrogationErrorID = iota + 1
	InterrogationErrorENRDecode
	InterrogationErrorIncompatibleForkID
	InterrogationErrorBlacklistedClientID
	InterrogationErrorKeygen
	InterrogationErrorFindNode
	InterrogationErrorFindNodeTimeout
)

type InterrogationError struct {
	id         InterrogationErrorID
	wrappedErr error
}

func NewInterrogationError(id InterrogationErrorID, wrappedErr error) *InterrogationError {
	instance := InterrogationError{
		id,
		wrappedErr,
	}
	return &instance
}

func (e *InterrogationError) Unwrap() error {
	return e.wrappedErr
}

func (e *InterrogationError) Error() string {
	switch e.id {
	case InterrogationErrorPing:
		return fmt.Sprintf("ping-pong failed: %v", e.wrappedErr)
	case InterrogationErrorENRDecode:
		return e.wrappedErr.Error()
	case InterrogationErrorIncompatibleForkID:
		return fmt.Sprintf("incompatible ENR fork ID %v", e.wrappedErr)
	case InterrogationErrorBlacklistedClientID:
		return fmt.Sprintf("incompatible client ID %v", e.wrappedErr)
	case InterrogationErrorKeygen:
		return fmt.Sprintf("keygen failed: %v", e.wrappedErr)
	case InterrogationErrorFindNode:
		return fmt.Sprintf("FindNode request failed: %v", e.wrappedErr)
	case InterrogationErrorFindNodeTimeout:
		return fmt.Sprintf("FindNode request timeout: %v", e.wrappedErr)
	default:
		return "<unhandled InterrogationErrorID>"
	}
}
