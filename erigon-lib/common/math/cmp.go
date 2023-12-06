package math

import (
	"golang.org/x/exp/constraints"
)

// Min generic min implementation.
//
// TODO replace usages with builtin generic min/max in Go 1.21+
func Min[T constraints.Ordered](a, b T) T {
	if b < a {
		return b
	}

	return a
}

// Max generic max implementation.
//
// TODO replace usages with builtin generic min/max in Go 1.21+
func Max[T constraints.Ordered](a, b T) T {
	if b > a {
		return b
	}

	return a
}
