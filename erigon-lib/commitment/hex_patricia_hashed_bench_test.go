package commitment

import (
	"encoding/hex"
	"math/rand"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common/length"
)

func Benchmark_HexPatriciaHahsed_ReviewKeys(b *testing.B) {
	ms := NewMockState(&testing.T{})
	hph := NewHexPatriciaHashed(length.Addr, ms.branchFn, ms.accountFn, ms.storageFn)
	hph.SetTrace(false)

	builder := NewUpdateBuilder()

	rnd := rand.New(rand.NewSource(133777))
	keysCount := rnd.Int31n(10_000_0)

	// generate updates
	for i := int32(0); i < keysCount; i++ {
		key := make([]byte, length.Addr)

		for j := 0; j < len(key); j++ {
			key[j] = byte(rnd.Intn(256))
		}
		builder.Balance(hex.EncodeToString(key), rnd.Uint64())
	}

	pk, hk, _ := builder.Build()

	b.Run("review_keys", func(b *testing.B) {
		for i, j := 0, 0; i < b.N; i, j = i+1, j+1 {
			if j >= len(pk) {
				j = 0
			}

			hph.ReviewKeys(pk[j:j+1], hk[j:j+1])
		}
	})
}
