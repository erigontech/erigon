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

package integrity_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/integrity"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestCheckCommitmentRootFailsWithoutVisibleCommitmentFile(t *testing.T) {
	ctx, logger := t.Context(), log.New()
	const stepSize = uint64(10)
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs, temporaltest.WithStepSize(stepSize))
	agg := db.(state.HasAgg).Agg().(*state.Aggregator)

	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	sd, err := execctx.NewSharedDomains(ctx, tx, logger)
	require.NoError(t, err)
	defer sd.Close()
	for txNum := uint64(1); txNum <= 2*stepSize; txNum++ {
		acc := accounts.NewAccount()
		acc.Balance = *uint256.NewInt(txNum)
		addr := common.BigToAddress(uint256.NewInt(txNum).ToBig())
		require.NoError(t, sd.DomainPut(kv.AccountsDomain, tx, addr[:], accounts.SerialiseV3(&acc), txNum, nil))
	}
	require.NoError(t, sd.Flush(ctx, tx))
	require.NoError(t, tx.Commit())
	require.NoError(t, agg.BuildFiles(db, 2*stepSize, unboundedFinalityCtx))

	agg.CloseFilesNoReopen()
	entries, err := os.ReadDir(dirs.SnapDomain)
	require.NoError(t, err)
	for _, e := range entries {
		if strings.Contains(e.Name(), "-commitment.") {
			require.NoError(t, dir.RemoveFile(filepath.Join(dirs.SnapDomain, e.Name())))
		}
	}
	require.NoError(t, agg.ReloadFiles())

	require.ErrorIs(t, integrity.CheckCommitmentRoot(ctx, db, nil, true, logger), integrity.ErrIntegrity)
}

func TestCheckCommitmentRootPassesOnEmptyDatadir(t *testing.T) {
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	require.NoError(t, integrity.CheckCommitmentRoot(t.Context(), db, nil, true, log.New()))
}
