//go:build !amd64

package crypto

func prefetchKeccakBucket(*keccakBucket) {}
