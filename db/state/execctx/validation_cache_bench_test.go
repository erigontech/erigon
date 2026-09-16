// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package execctx_test

import (
	"encoding/binary"
	"testing"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/stretchr/testify/require"
)

func BenchmarkValidationCache(b *testing.B) {
	for _, local := range []bool{false, true} {
		name := "shared-unwind"
		if local {
			name = "local-unwind"
		}
		b.Run(name, func(b *testing.B) {
			for _, tc := range []struct {
				name         string
				unwind, read bool
			}{
				{name: "extend"}, {name: "discard-fork", unwind: true}, {name: "read-discard-fork", unwind: true, read: true},
			} {
				b.Run(tc.name, func(b *testing.B) {
					db := benchSeedDb(b)
					tx, err := db.BeginTemporalRo(b.Context())
					require.NoError(b, err)
					defer tx.Rollback()
					sc := newSmallStateCache()
					b.Cleanup(sc.Close)
					canonical, err := execctx.NewSharedDomains(b.Context(), tx, log.New())
					require.NoError(b, err)
					defer canonical.Close()
					canonical.BindStateCache(sc)
					keys := make([][]byte, 256)
					for i := range keys {
						keys[i] = make([]byte, 20)
						binary.BigEndian.PutUint64(keys[i][12:], uint64(i)+1)
						_, _, err := canonical.GetLatest(kv.AccountsDomain, tx, keys[i])
						require.NoError(b, err)
					}
					var opts []execctx.SharedDomainOption
					if local {
						opts = append(opts, execctx.WithLocalCacheUnwind())
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						candidate, err := execctx.NewSharedDomains(b.Context(), tx, log.New(), opts...)
						if err != nil {
							b.Fatal(err)
						}
						candidate.BindStateCache(sc)
						if tc.unwind {
							candidate.Unwind(16, nil)
						}
						if tc.read {
							for _, key := range keys {
								if _, _, err := candidate.GetLatest(kv.AccountsDomain, tx, key); err != nil {
									b.Fatal(err)
								}
							}
						}
						candidate.Close()
						for _, key := range keys {
							_, _, err := canonical.GetLatest(kv.AccountsDomain, tx, key)
							if err != nil {
								b.Fatal(err)
							}
						}
					}
				})
			}
		})
	}
}
