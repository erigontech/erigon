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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/statecfg"
)

func benchmarkDomainCollateStep(b *testing.B, steps int, step kv.Step) {
	const stepSize, keys = 16, 4000
	db, d := testDbAndDomainOfStep(b, statecfg.Schema.StorageDomain, stepSize, log.New())
	writeDupSortStepBacklog(b, db, d, stepSize, steps, keys, 11)
	roTx, err := db.BeginRo(b.Context())
	require.NoError(b, err)
	defer roTx.Rollback()
	for b.Loop() {
		coll, err := d.collate(b.Context(), step, uint64(step)*stepSize, uint64(step+1)*stepSize, roTx)
		require.NoError(b, err)
		coll.Close()
	}
}

func BenchmarkDomain_CollateSingleStep(b *testing.B) {
	benchmarkDomainCollateStep(b, 1, 0)
}

func BenchmarkDomain_CollateOldestStepOfBacklog(b *testing.B) {
	benchmarkDomainCollateStep(b, 256, 0)
}

func BenchmarkDomain_CollateNewestStepOfBacklog(b *testing.B) {
	benchmarkDomainCollateStep(b, 256, 255)
}
