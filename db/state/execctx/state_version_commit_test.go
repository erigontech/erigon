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
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
)

func TestSharedDomainsCommitAdvancesStateVersionOnce(t *testing.T) {
	ctx := t.Context()
	db := newTestDb(t, 16)
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	sd, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	projected, err := sd.ProjectedStateVersion()
	require.NoError(t, err)

	require.NoError(t, sd.Commit(ctx, rwTx))
	require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
		committed, err := rawdb.GetStateVersion(tx)
		require.NoError(t, err)
		require.Equal(t, projected, committed)
		return nil
	}))
}

func TestSharedDomainsCommitRejectsAnotherStateVersionWriter(t *testing.T) {
	ctx := t.Context()
	db := newTestDb(t, 16)
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	sd, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	require.NoError(t, sd.InitBlockOverlay(rwTx, t.TempDir()))
	_, err = rawdb.IncrementStateVersion(sd.BlockOverlay())
	require.NoError(t, err)

	err = sd.Commit(ctx, rwTx)
	require.ErrorContains(t, err, "unexpected state version after flush")
}

func TestSharedDomainsCommitRejectsStaleBaseStateVersion(t *testing.T) {
	ctx := t.Context()
	db := newTestDb(t, 16)
	baseTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer baseTx.Rollback()

	sd, err := execctx.NewSharedDomains(ctx, baseTx, log.New())
	require.NoError(t, err)
	defer sd.Close()

	advanceTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer advanceTx.Rollback()
	_, err = rawdb.IncrementStateVersion(advanceTx)
	require.NoError(t, err)
	require.NoError(t, advanceTx.Commit())

	commitTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer commitTx.Rollback()
	err = sd.Commit(ctx, commitTx)
	require.ErrorContains(t, err, "state version changed since SharedDomains was created")
}
