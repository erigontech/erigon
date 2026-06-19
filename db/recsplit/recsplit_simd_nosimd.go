//go:build !goexperiment.simd || !amd64

package recsplit

func findSplitVec(bucket []uint64, salt uint64, fanout, unit uint16, count []uint16) uint64 {
	return findSplit(bucket, salt, fanout, unit, count)
}

func findBijectionVec(bucket []uint64, salt uint64) uint64 {
	return findBijection(bucket, salt)
}
