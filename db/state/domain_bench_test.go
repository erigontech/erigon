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

package state

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

// BenchmarkAbsentKeyGetLatestFromFiles reads absent 20-byte keys over several domain files with in-heap existence filters.
func BenchmarkAbsentKeyGetLatestFromFiles(b *testing.B) {
	for _, keysPerFile := range []int{4_000, 200_000} {
		b.Run(fmt.Sprintf("keysPerFile=%d", keysPerFile), func(b *testing.B) {
			benchAbsentKeyGetLatestFromFiles(b, 12, keysPerFile)
		})
	}
}

func benchAbsentKeyGetLatestFromFiles(b *testing.B, files, keysPerFile int) {
	prev := domainExistenceForceInMem
	domainExistenceForceInMem = []string{kv.AccountsDomain.String()}
	b.Cleanup(func() { domainExistenceForceInMem = prev })
	db, d := testDbAndDomain(b, log.New())
	tx, err := db.BeginRw(b.Context())
	require.NoError(b, err)
	defer tx.Rollback()
	rnd := rand.New(rand.NewPCG(1, 2))
	randKey := func() []byte {
		k := make([]byte, 20)
		for i := range k {
			k[i] = byte(rnd.Uint32())
		}
		return k
	}
	domainTx := d.beginForTests()
	writer := domainTx.NewWriter(db)
	for step := range files {
		for range keysPerFile {
			require.NoError(b, writer.PutWithPrev(randKey(), []byte("v"), uint64(step)*d.stepSize+1, nil))
		}
	}
	require.NoError(b, writer.Flush(b.Context(), tx))
	writer.Close()
	domainTx.Close()
	for step := range files {
		require.NoError(b, d.collateBuildIntegrate(b.Context(), kv.Step(step), tx, background.NewProgressSet()))
	}
	dt := d.beginForTests()
	defer dt.Close()
	require.Len(b, dt.files, files)
	absent := make([][]byte, 1<<18)
	for i := range absent {
		absent[i] = randKey()
	}
	for i := 0; b.Loop(); i++ {
		if _, found, _, _, err := dt.getLatestFromFiles(absent[i%len(absent)], nil, kv.NoStepBound); err != nil || found {
			b.Fatal(found, err)
		}
	}
	b.ReportMetric(float64(files), "files/lookup")
}
