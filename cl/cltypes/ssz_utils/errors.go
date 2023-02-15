package ssz_utils

import "errors"

var (
	ErrLowBufferSize    = errors.New("ssz(DecodeSSZ): bad encoding size")
	ErrBadDynamicLength = errors.New("ssz(DecodeSSZ): bad dynamic length")
	ErrBadOffset        = errors.New("ssz(DecodeSSZ): invalid offset")
	ErrBufferNotRounded = errors.New("ssz(DecodeSSZ): badly rounded operator")
	ErrTooBigList       = errors.New("ssz(DecodeSSZ): list too big")
)
