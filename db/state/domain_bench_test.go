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
	"github.com/erigontech/erigon/db/state/statecfg"
)

func BenchmarkDomain_CollateOldestStepOfBacklog(b *testing.B) {
	const stepSize, steps, keys = 16, 256, 4000
	db, d := testDbAndDomainOfStep(b, statecfg.Schema.StorageDomain, stepSize, log.New())
	writeDupSortStepBacklog(b, db, d, stepSize, steps, keys, 11)
	roTx, err := db.BeginRo(b.Context())
	require.NoError(b, err)
	defer roTx.Rollback()
	for b.Loop() {
		coll, err := d.collate(b.Context(), 0, 0, stepSize, roTx)
		require.NoError(b, err)
		coll.Close()
	}
}
