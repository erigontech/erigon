package common

import "errors"

var ErrStopped = errors.New("stopped")
var ErrUnwind = errors.New("unwound")

func Stopped(ch <-chan struct{}) error {
	if ch == nil {
		return nil
	}
	select {
	case <-ch:
		return ErrStopped
	default:
	}
	return nil
}

func SafeClose(ch chan struct{}) {
	if ch == nil {
		return
	}
	select {
	case <-ch:
		// Channel was already closed
	default:
		close(ch)
	}
}
