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

package stagedsync

import (
	"context"
	"encoding/binary"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
)

func BenchmarkPrefetchedBranchRead(b *testing.B) {
	ctx := context.Background()
	db := temporaltest.NewTestDB(b, datadir.New(b.TempDir()), temporaltest.WithStepSize(16))
	tx, err := db.BeginTemporalRw(ctx) //nolint:gocritic
	require.NoError(b, err)
	defer tx.Rollback()
	doms, err := execctx.NewSharedDomains(ctx, tx, log.New())
	require.NoError(b, err)
	defer doms.Close()

	p := &branchPrefetcher{}
	keys := make([][]byte, 8192)
	data := make([]byte, 300)
	for i := range keys {
		keys[i] = binary.BigEndian.AppendUint64([]byte{0x00}, uint64(i)*0x9e3779b97f4a7c15)
		p.put(keys[i], data, 1)
	}
	p.freeze()
	r := &asOfStateReader{sd: doms, roTx: tx, prefetched: p}

	var next atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := next.Add(1) * 977
		for pb.Next() {
			if got, _, _ := r.Read(kv.CommitmentDomain, keys[i%uint64(len(keys))], 16); len(got) == 0 {
				b.Fatal("record not served")
			}
			i++
		}
	})
}
