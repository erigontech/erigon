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

package rawdb_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/rawdb"
)

func TestShadowStateRootStorageUsesBlockHash(t *testing.T) {
	_, tx := mdbxtest.NewTestTx(t)
	defer tx.Rollback()

	firstHash := common.Hash{1}
	secondHash := common.Hash{2}
	firstRoot := []byte{3}
	secondRoot := []byte{4}

	require.NoError(t, rawdb.WriteShadowStateRoot(tx, firstHash, 7, firstRoot))
	require.NoError(t, rawdb.WriteShadowStateRoot(tx, secondHash, 7, secondRoot))

	got, err := rawdb.ReadShadowStateRoot(tx, firstHash, 7)
	require.NoError(t, err)
	require.Equal(t, firstRoot, got)
	got, err = rawdb.ReadShadowStateRoot(tx, secondHash, 7)
	require.NoError(t, err)
	require.Equal(t, secondRoot, got)
}

func TestCommitmentDomainStoppedMarker(t *testing.T) {
	_, tx := mdbxtest.NewTestTx(t)
	defer tx.Rollback()

	stopped, err := rawdb.ReadCommitmentDomainStopped(tx, kv.CommitmentBinDomain)
	require.NoError(t, err)
	require.False(t, stopped)
	require.NoError(t, rawdb.WriteCommitmentDomainStopped(tx, kv.CommitmentBinDomain))
	stopped, err = rawdb.ReadCommitmentDomainStopped(tx, kv.CommitmentBinDomain)
	require.NoError(t, err)
	require.True(t, stopped)
	stopped, err = rawdb.ReadCommitmentDomainStopped(tx, kv.CommitmentDomain)
	require.NoError(t, err)
	require.False(t, stopped)
}
