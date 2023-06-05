package requests

import "errors"

var (
	// ErrBadRequest for http bad requests
	ErrBadRequest = errors.New("bad request")
)
