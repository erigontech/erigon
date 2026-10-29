package bls_test

import (
	"fmt"
	"testing"

	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/stretchr/testify/require"
)

func BenchmarkVerifyAggregate(b *testing.B) {
	message := []byte("aggregate verification benchmark")
	_, publicKeys, signatures := generateAggregateInputs(b, 32768, message)
	sizes := []int{1, 128, 2048, 32768}

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
	message := []byte("aggregate verification benchmark")
	publicKeys, _, signatures := generateAggregateInputs(b, 32768, message)
	for _, size := range []int{1, 128, 513, 2048, 32768} {
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
