package utils

func IsPowerOf2(n uint64) bool {
	return n != 0 && (n&(n-1)) == 0
}

func PowerOf2(n uint64) uint64 {
	if n >= 64 {
		panic("integer overflow")
	}
	return 1 << n
}
