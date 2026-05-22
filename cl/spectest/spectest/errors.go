package spectest

import (
	"errors"
	"fmt"
)

var ErrorHandlerNotFound = errors.New("handler not found")

func ErrHandlerNotFound(handler string) error {
	return fmt.Errorf("%w: %s", ErrorHandlerNotFound, handler)
}

var ErrorHandlerNotImplemented = errors.New("handler not implemented")

func ErrHandlerNotImplemented(handler string) error {
	return fmt.Errorf("%w: %s", ErrorHandlerNotImplemented, handler)
}
