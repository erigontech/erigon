package utils

import (
	"math"

	"github.com/thomaso-mirodin/intmath/u64"
)

func IsPowerOf2(n uint64) bool {
	return n != 0 && (n&(n-1)) == 0
}

func PowerOf2(n uint64) uint64 {
	if n >= 64 {
		panic("integer overflow")
	}
	return 1 << n
}

var squareRootTable = map[uint64]uint64{
	4:       2,
	16:      4,
	64:      8,
	256:     16,
	1024:    32,
	4096:    64,
	16384:   128,
	65536:   256,
	262144:  512,
	1048576: 1024,
	4194304: 2048,
}

func IntegerSquareRoot(n uint64) uint64 {
	if v, ok := squareRootTable[n]; ok {
		return v
	}
	if n >= 1<<52 {
		return u64.Sqrt(n)
	}

	return uint64(math.Sqrt(float64(n)))
}

func Max64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func Min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
