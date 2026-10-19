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

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestComputeCommitmentPausesColdIO(t *testing.T) {
	t.Parallel()
	db := newTestDb(t, 16)
	ctx := t.Context()
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	sd, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	at := rwTx.AggTx().(*state.AggregatorRoTx)

	addr := make([]byte, length.Addr)
	addr[0] = 0x42
	acc := accounts.Account{Nonce: 1, Balance: *uint256.NewInt(1), CodeHash: accounts.EmptyCodeHash}
	require.NoError(t, sd.DomainPut(kv.AccountsDomain, rwTx, addr, accounts.SerialiseV3(&acc), 1, nil))

	var pausedDuringCompute bool
	_, err = sd.ComputeCommitment(ctx, rwTx, false, 0, 1, "", func(*commitment.CommitProgress) {
		pausedDuringCompute = at.ColdIOPaused()
	})
	require.NoError(t, err)
	require.True(t, pausedDuringCompute)
	require.False(t, at.ColdIOPaused())
}
