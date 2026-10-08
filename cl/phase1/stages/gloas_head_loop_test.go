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
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/common"
)

// headLoopTestStore serves a sequence of heads whose persisted payloads have no EL status.
type headLoopTestStore struct {
	*forkchoice.ForkChoiceStore
	heads     []common.Hash
	blocks    map[common.Hash]*cltypes.SignedBeaconBlock
	envelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope
	verdicts  map[common.Hash]execution_client.PayloadStatus
}

func (s *headLoopTestStore) GetRecentExecutionPayloadStatus(common.Hash) (execution_client.PayloadStatus, bool) {
	return execution_client.PayloadStatusNone, false
}

func (s *headLoopTestStore) MarkPayloadStatusAndGasLimitIfRetained(root, _ common.Hash, status execution_client.PayloadStatus, _ uint64) (execution_client.PayloadStatus, bool) {
	s.verdicts[root] = status
	return status, true
}

func (s *headLoopTestStore) GetHead(*state.CachingBeaconState) (common.Hash, uint64, error) {
	head := s.heads[0]
	if len(s.heads) > 1 {
		s.heads = s.heads[1:]
	}
	return head, 0, nil
}

func (s *headLoopTestStore) HasEnvelope(root common.Hash) bool {
	_, ok := s.envelopes[root]
	return ok
}

func (s *headLoopTestStore) GetBlock(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	block, ok := s.blocks[root]
	return block, ok
}

func (s *headLoopTestStore) ReadEnvelopeFromDisk(root common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	return s.envelopes[root], nil
}

func newHeadLoopTestStore(cfg *clparams.BeaconChainConfig, heads ...common.Hash) *headLoopTestStore {
	store := &headLoopTestStore{
		ForkChoiceStore: &forkchoice.ForkChoiceStore{},
		heads:           heads,
		blocks:          map[common.Hash]*cltypes.SignedBeaconBlock{},
		envelopes:       map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{},
		verdicts:        map[common.Hash]execution_client.PayloadStatus{},
	}
	for i, root := range heads {
		payload := cltypes.NewEth1Block(clparams.GloasVersion, cfg)
		payload.BlockHash = common.Hash{0xe0, byte(i)}
		store.blocks[root] = &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{
			Slot: uint64(i + 1),
			Body: &cltypes.BeaconBody{
				Version: clparams.GloasVersion,
				SignedExecutionPayloadBid: &cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{
					BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48),
				}},
			},
		}}
		store.envelopes[root] = &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{
			BeaconBlockRoot: root,
			Payload:         payload,
		}}
	}
	return store
}

func TestVerifyGloasHeadPayloadsFollowsTheHeadWhileVerdictsMoveIt(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	first, second := common.HexToHash("0x1"), common.HexToHash("0x2")
	// Verifying the first head exposes the second; the second verdict leaves the head put.
	store := newHeadLoopTestStore(&cfg, first, second, second, second)
	engine := &testExecutionEngine{supportInsertion: true, payloadStatus: execution_client.PayloadStatusValidated}
	stageCfg := &Cfg{beaconCfg: &cfg, executionClient: engine, gloasPayloadValidator: engine, forkChoice: store.ForkChoiceStore}
	attempted := map[common.Hash]struct{}{}

	verifyGloasHeadPayloads(context.Background(), stageCfg, store, attempted)

	require.Equal(t, 2, engine.newPayloadCalls)
	require.Equal(t, map[common.Hash]execution_client.PayloadStatus{first: execution_client.PayloadStatusValidated, second: execution_client.PayloadStatusValidated}, store.verdicts)
	require.Contains(t, attempted, first)
	require.Contains(t, attempted, second)
}

func TestVerifyGloasHeadPayloadsSkipsAHeadAttemptedThisCycle(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	head := common.HexToHash("0x1")
	store := newHeadLoopTestStore(&cfg, head)
	engine := &testExecutionEngine{supportInsertion: true, payloadStatus: execution_client.PayloadStatusValidated}
	stageCfg := &Cfg{beaconCfg: &cfg, executionClient: engine, gloasPayloadValidator: engine, forkChoice: store.ForkChoiceStore}

	verifyGloasHeadPayloads(context.Background(), stageCfg, store, map[common.Hash]struct{}{head: {}})

	require.Zero(t, engine.newPayloadCalls)
}

func TestVerifyGloasHeadPayloadsRecordsNoVerdictWhenNewPayloadIsInterrupted(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	head := common.HexToHash("0x1")
	store := newHeadLoopTestStore(&cfg, head)
	ctx, cancel := context.WithCancel(context.Background())
	engine := &testExecutionEngine{supportInsertion: true}
	engine.newPayloadFn = func(context.Context, *cltypes.Eth1Block) (execution_client.PayloadStatus, error) {
		cancel()
		return execution_client.PayloadStatusNone, ctx.Err()
	}
	stageCfg := &Cfg{beaconCfg: &cfg, executionClient: engine, gloasPayloadValidator: engine, forkChoice: store.ForkChoiceStore}
	attempted := map[common.Hash]struct{}{}

	verifyGloasHeadPayloads(ctx, stageCfg, store, attempted)

	require.Equal(t, 1, engine.newPayloadCalls)
	require.Empty(t, attempted)
	require.Empty(t, store.verdicts)
}
