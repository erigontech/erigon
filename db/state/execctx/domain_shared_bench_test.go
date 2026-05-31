// Copyright 2026 The Erigon Authors
// SPDX-License-Identifier: LGPL-3.0-or-later

package execctx_test

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
)

// BenchmarkSharedDomains_DomainPut measures the per-call overhead of
// SharedDomains.DomainPut on the hot path that newPayload exercises
// (per-tx state writes — account/storage updates fan out to thousands
// per block). Two variants:
//
//   - /serial    : trie variant is plain HexPatricia → parallelCommitment=false,
//     changesetMu Lock/Unlock skipped on hot path.
//   - /parallel  : trie variant is ConcurrentHexPatricia → parallelCommitment=true,
//     changesetMu is acquired/released on every domainPut.
//
// The delta between the two reports the cost of the lock when parallel
// commitment is NOT actually running (the default config), validating
// the optimization that gates Lock/Unlock on `sd.parallelCommitment`.
func BenchmarkSharedDomains_DomainPut(b *testing.B) {
	for _, mode := range []struct {
		name    string
		variant commitment.TrieVariant
	}{
		{"serial", commitment.VariantHexPatriciaTrie},
		{"parallel", commitment.VariantConcurrentHexPatricia},
	} {
		b.Run(mode.name, func(b *testing.B) {
			db := newTestDb(b, 16)
			ctx := context.Background()
			rwTx, err := db.BeginTemporalRw(ctx)
			if err != nil {
				b.Fatal(err)
			}
			defer rwTx.Rollback() //nolint:errcheck

			doms, err := execctx.NewSharedDomainsWithTrieVariant(ctx, rwTx, log.New(), mode.variant)
			if err != nil {
				b.Fatal(err)
			}
			defer doms.Close()

			// Pre-fill the key buffer; each iteration writes a unique key/value
			// to exercise the real DomainPut path (not the prev-value-equal
			// early-return).
			k := make([]byte, 20)
			v := make([]byte, 8)

			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				binary.LittleEndian.PutUint64(k[:8], uint64(i))
				binary.LittleEndian.PutUint64(v, uint64(i))
				if err := doms.DomainPut(kv.AccountsDomain, rwTx, k, v, uint64(i), nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
