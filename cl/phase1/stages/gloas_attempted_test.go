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

// A root that received a verdict in this cycle is requeued by a later drain pass without
// another NewPayload, so the retry phases share one attempt per root per cycle.
func TestDrainPendingGloasPayloadsSkipsRootsAttemptedThisCycle(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	blockRoot := common.HexToHash("0x1234")
	fc := &forkchoice.ForkChoiceStore{}
	engine := &testExecutionEngine{supportInsertion: true, payloadStatus: execution_client.PayloadStatusNotValidated}
	payload := cltypes.NewEth1Block(clparams.GloasVersion, &cfg)
	payload.BlockHash = common.HexToHash("0x9abc")
	pending := forkchoice.PendingELPayload{
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
	}
	fc.RequeuePendingELPayload(pending)
	stageCfg := &Cfg{beaconCfg: &cfg, executionClient: engine, gloasPayloadValidator: engine, forkChoice: fc}
	attempted := map[common.Hash]struct{}{}

	drainPendingGloasPayloads(context.Background(), stageCfg, attempted)
	require.Equal(t, 1, engine.newPayloadCalls)
	require.Contains(t, attempted, blockRoot)

	// The NotValidated result was requeued; the same cycle must not retry it.
	drainPendingGloasPayloads(context.Background(), stageCfg, attempted)
	require.Equal(t, 1, engine.newPayloadCalls)
	require.Len(t, fc.DrainPendingELPayloadsLimit(10), 1)
}

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
	attempted := map[common.Hash]struct{}{}

	drainPendingGloasPayloads(ctx, stageCfg, attempted)

	require.Equal(t, 1, engine.newPayloadCalls)
	require.Empty(t, attempted)
	_, recorded := fc.GetRecentExecutionPayloadStatusByRoot(blockRoot)
	require.False(t, recorded)
	require.Len(t, fc.DrainPendingELPayloadsLimit(10), 1)
}
