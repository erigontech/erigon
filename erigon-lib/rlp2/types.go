package rlp

import (
	"fmt"

	"github.com/holiman/uint256"
)

func Bytes(dst *[]byte, src []byte) error {
	if len(*dst) < len(src) {
		(*dst) = make([]byte, len(src))
	}
	copy(*dst, src)
	return nil
}
func BytesExact(dst *[]byte, src []byte) error {
	if len(*dst) != len(src) {
		return fmt.Errorf("%w: BytesExact no match", ErrDecode)
	}
	copy(*dst, src)
	return nil
}

func Uint256(dst *uint256.Int, src []byte) error {
	if len(src) > 32 {
		return fmt.Errorf("%w: uint256 must not be more than 32 bytes long, got %d", ErrParse, len(src))
	}
	if len(src) > 0 && src[0] == 0 {
		return fmt.Errorf("%w: integer encoding for RLP must not have leading zeros: %x", ErrParse, src)
	}
	dst.SetBytes(src)
	return nil
}

func Uint64(dst *uint64, src []byte) error {
	var r uint64
	for _, b := range src {
		r = (r << 8) | uint64(b)
	}
	(*dst) = r
	return nil
}

func IsEmpty(dst *bool, src []byte) error {
	if len(src) == 0 {
		(*dst) = true
	} else {
		(*dst) = false
	}
	return nil
}
func BlobLength(dst *int, src []byte) error {
	(*dst) = len(src)
	return nil
}

func Skip(dst *int, src []byte) error {
	return nil
}
