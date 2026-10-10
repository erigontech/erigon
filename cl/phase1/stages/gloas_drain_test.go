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

package stages

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/common"
)

func TestDrainPendingGloasPayloadsRequeuesWithoutVerdictWhenNewPayloadIsInterrupted(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	blockRoot := common.HexToHash("0x1234")
	fc := &forkchoice.ForkChoiceStore{}
	ctx, cancel := context.WithCancel(context.Background())
	engine := &testExecutionEngine{supportInsertion: true}
	engine.newPayloadFn = func(context.Context, *cltypes.Eth1Block) (execution_client.PayloadStatus, error) {
		cancel()
		return execution_client.PayloadStatusNone, ctx.Err()
	}
	payload := cltypes.NewEth1Block(clparams.GloasVersion, &cfg)
	payload.BlockHash = common.HexToHash("0x9abc")
	fc.RequeuePendingELPayload(forkchoice.PendingELPayload{
		Block: &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{
			Slot:       1,
			ParentRoot: common.HexToHash("0x5678"),
			Body: &cltypes.BeaconBody{
				Version: clparams.GloasVersion,
				SignedExecutionPayloadBid: &cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{
					BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48),
				}},
			},
		}},
		Envelope: &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{
			BeaconBlockRoot: blockRoot,
			Payload:         payload,
		}},
	})
	stageCfg := &Cfg{beaconCfg: &cfg, executionClient: engine, gloasPayloadValidator: engine, forkChoice: fc}
	store := &verdictRecordingStore{ForkChoiceStore: fc, verdicts: map[common.Hash]execution_client.PayloadStatus{}}

	drainPendingGloasPayloads(ctx, stageCfg, store)

	require.Equal(t, 1, engine.newPayloadCalls)
	require.Empty(t, store.verdicts, "an interrupted NewPayload is not a verdict")
	require.Len(t, fc.DrainPendingELPayloadsLimit(10), 1)
}

type verdictRecordingStore struct {
	*forkchoice.ForkChoiceStore
	verdicts map[common.Hash]execution_client.PayloadStatus
}

func (s *verdictRecordingStore) MarkPayloadStatusAndGasLimitIfRetained(root, _ common.Hash, status execution_client.PayloadStatus, _ uint64) (execution_client.PayloadStatus, bool) {
	s.verdicts[root] = status
	return status, true
}
