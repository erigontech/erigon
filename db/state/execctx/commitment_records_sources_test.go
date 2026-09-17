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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

func TestReadCommitmentRecordsMergesMemAndCacheSources(t *testing.T) {
	db := newTestDb(t, 16)
	rwTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()
	sd, err := execctx.NewSharedDomains(t.Context(), rwTx, log.New())
	require.NoError(t, err)
	defer sd.Close()

	branchCache := rwTx.AggTx().(commitment.BranchCacheProvider).BranchCache()
	require.NotNil(t, branchCache)
	branchCache.SetEdgeRecords(true)

	nodeKey := nibbles.EncodeKeyV3([]byte{0x0a, 0x0c})
	childKey := func(nibble byte) []byte {
		return nibbles.ChildKeyV3(nodeKey, nibble)
	}

	cached := []byte("from-cache")
	branchCache.Put(childKey(2), cached, 1, 31)

	pending := []byte("from-mem")
	require.NoError(t, sd.DomainPut(kv.CommitmentDomain, rwTx, childKey(5), pending, 48, nil))

	wanted := uint16(1)<<2 | uint16(1)<<5
	records, present, step, err := sd.ReadCommitmentRecords(rwTx, nodeKey, wanted, true, nil)
	require.NoError(t, err)
	require.Equal(t, wanted, present, "both sources must resolve under one read")
	require.Equal(t, cached, records[2])
	require.Equal(t, pending, records[5])
	require.EqualValues(t, 3, step, "the node reports the newest of the records it served")

	narrow, present, _, err := sd.ReadCommitmentRecords(rwTx, nodeKey, uint16(1)<<5, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint16(1)<<5, present, "a masked-out sibling must not be resolved")
	require.Nil(t, narrow[2])
	require.Equal(t, pending, narrow[5])
}
