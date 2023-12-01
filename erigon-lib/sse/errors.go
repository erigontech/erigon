package sse

import "errors"

var (
	ErrInvalidUTF8Bytes   = errors.New("invalid utf8 bytes")
	ErrInvalidContentType = errors.New("invalid content type")
)
