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

package clique

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	memdb "github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

func TestSnapshotStoreUpdatesLatestSnapshot(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ConsensusDB)
	latest := &Snapshot{Number: 2, Hash: common.Hash{2}}
	require.NoError(t, latest.store(db))

	number, err := lastSnapshot(db, log.New())
	require.NoError(t, err)
	require.Equal(t, latest.Number, number)

	require.NoError(t, (&Snapshot{Number: 1, Hash: common.Hash{1}}).store(db))
	number, err = lastSnapshot(db, log.New())
	require.NoError(t, err)
	require.Equal(t, latest.Number, number)
}
