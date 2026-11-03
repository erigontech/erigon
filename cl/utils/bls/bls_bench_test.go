package bls_test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/stretchr/testify/require"
)

// aggregateBenchmarkSizes adds the 32768-key case only outside short mode: building its inputs takes seconds.
func aggregateBenchmarkSizes(sizes ...int) []int {
	if testing.Short() {
		return sizes
	}
	return append(sizes, 32768)
}

func BenchmarkVerifyAggregate(b *testing.B) {
	message := aggregateMessage
	sizes := aggregateBenchmarkSizes(1, 128, 2048)
	_, publicKeys, signatures := generateAggregateInputs(b, slices.Max(sizes))

	bls.SetEnabledCaching(true)
	b.Cleanup(func() { bls.SetEnabledCaching(false) })
	for _, size := range sizes {
		b.Run(fmt.Sprintf("keys=%d", size), func(b *testing.B) {
			signature, err := bls.AggregateSignatures(signatures[:size])
			require.NoError(b, err)
			keys := publicKeys[:size]
			valid, err := bls.VerifyAggregate(signature, message, keys)
			require.NoError(b, err)
			require.True(b, valid)

			b.ReportAllocs()
			for b.Loop() {
				valid, err = bls.VerifyAggregate(signature, message, keys)
				if err != nil || !valid {
					b.Fatalf("VerifyAggregate() = %v, %v", valid, err)
				}
			}
		})
	}
}

func BenchmarkSignatureVerifyAggregate(b *testing.B) {
	message := aggregateMessage
	sizes := aggregateBenchmarkSizes(1, 128, 513, 2048)
	publicKeys, _, signatures := generateAggregateInputs(b, slices.Max(sizes))
	for _, size := range sizes {
		b.Run(fmt.Sprintf("keys=%d", size), func(b *testing.B) {
			signature := aggregateSignature(b, signatures[:size])
			keys := publicKeys[:size]

			b.ReportAllocs()
			for b.Loop() {
				if !signature.VerifyAggregate(message, keys) {
					b.Fatal("Signature.VerifyAggregate() = false")
				}
			}
		})
	}
}
