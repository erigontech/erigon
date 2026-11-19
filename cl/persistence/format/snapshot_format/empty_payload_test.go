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

package snapshot_format_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/format/snapshot_format"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

type noExecutionData struct{}

func (noExecutionData) Transactions(uint64, common.Hash) (*solid.TransactionsSSZ, error) {
	return nil, errors.New("no execution block")
}
func (noExecutionData) Withdrawals(uint64, common.Hash) (*solid.ListSSZ[*cltypes.Withdrawal], error) {
	return nil, errors.New("no execution block")
}
func (noExecutionData) SetBeaconChainConfig(*clparams.BeaconChainConfig) {}
func (noExecutionData) CacheBody(uint64, [][]byte, []*types.Withdrawal)  {}

// A genesis block's payload is empty; reading it back must not ask the EL for a block it never had.
func TestReadBlockFromSnapshotKeepsAnEmptyPayloadEmpty(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	blk := cltypes.NewSignedBeaconBlock(cfg, clparams.DenebVersion)
	body := blk.Block.Body
	body.SyncAggregate = cltypes.NewSyncAggregateWithSize(int(cfg.SyncCommitteeSize) / 8)
	body.ExecutionPayload.Extra = solid.NewExtraData()
	body.ExecutionPayload.Transactions = &solid.TransactionsSSZ{}
	body.ExecutionPayload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44)
	want, err := blk.HashSSZ()
	require.NoError(t, err)

	var b bytes.Buffer
	_, err = snapshot_format.WriteBlockForSnapshot(&b, blk, nil)
	require.NoError(t, err)
	got, err := snapshot_format.ReadBlockFromSnapshot(&b, noExecutionData{}, cfg)
	require.NoError(t, err)

	gotRoot, err := got.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, want, gotRoot)
}
