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

package forkchoice_test

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/network/services"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
)

// consensus-spec test keys: validator i signs with the scalar i+1.
func specTestKey(t *testing.T, validatorIndex uint64) *bls.PrivateKey {
	t.Helper()
	var scalar [32]byte
	binary.BigEndian.PutUint64(scalar[24:], validatorIndex+1)
	key, err := bls.NewPrivateKeyFromBytes(scalar[:])
	require.NoError(t, err)
	return key
}

func sign(t *testing.T, validatorIndex uint64, signingRoot []byte) []byte {
	t.Helper()
	return specTestKey(t, validatorIndex).Sign(signingRoot).Bytes()
}

// reorgScenario is the ex-ante fixture: block C (slot 3) wins by proposer boost over the late block B
// (slot 2), which holds fewer votes than the boost is worth.
type reorgScenario struct {
	store      *forkchoice.ForkChoiceStore
	syncedData *synced_data.SyncedDataManager
	headState  *state.CachingBeaconState
	rootB      common.Hash
	rootC      common.Hash
}

func newReorgScenario(t *testing.T) *reorgScenario {
	t.Helper()
	cfg := &clparams.MainnetBeaconConfig
	decode := func(enc []byte) (*cltypes.SignedBeaconBlock, common.Hash) {
		block := cltypes.NewSignedBeaconBlock(cfg, clparams.DenebVersion)
		require.NoError(t, utils.DecodeSSZSnappy(block, enc, int(clparams.AltairVersion)))
		root, err := block.Block.HashSSZ()
		require.NoError(t, err)
		return block, root
	}
	blockA, _ := decode(block3aEncoded)
	blockC, rootC := decode(blockc2Encoded)
	blockB, rootB := decode(blockd4Encoded)
	attB := &solid.Attestation{}
	require.NoError(t, utils.DecodeSSZSnappy(attB, attestationEncoded, int(clparams.AltairVersion)))
	require.Equal(t, rootB, attB.Data.BeaconBlockRoot)

	anchor := state.New(cfg)
	require.NoError(t, utils.DecodeSSZSnappy(anchor, anchorStateEncoded, int(clparams.AltairVersion)))
	syncedData := synced_data.NewSyncedDataManager(cfg, true)
	store := newUntickedForkChoiceStoreWithSyncedData(t, anchor, nil, syncedData)
	store.OnTick(0)
	store.OnTick(12 * 1000)
	require.NoError(t, store.OnBlock(t.Context(), blockA, false, true, false))
	store.OnTick(36 * 1000)
	require.NoError(t, store.OnBlock(t.Context(), blockC, false, true, false))
	require.NoError(t, store.OnBlock(t.Context(), blockB, false, true, false))

	headState, err := store.GetStateAtBlockRoot(rootC)
	require.NoError(t, err)
	require.NoError(t, syncedData.OnHeadState(headState))

	committeeB, err := headState.GetBeaconCommitee(attB.Data.Slot, attB.Data.CommitteeIndex)
	require.NoError(t, err)
	store.ProcessAttestingIndicies(attB, committeeB[:3])

	head, _, err := store.GetHead(nil)
	require.NoError(t, err)
	require.Equal(t, rootC, head, "block C must win by proposer boost")
	return &reorgScenario{store: store, syncedData: syncedData, headState: headState, rootB: rootB, rootC: rootC}
}

func (s *reorgScenario) headAfterTickToSlot4(t *testing.T) common.Hash {
	t.Helper()
	s.store.OnTick(48 * 1000)
	head, _, err := s.store.GetHead(nil)
	require.NoError(t, err)
	return head
}

func (s *reorgScenario) slotThreeAggregate(t *testing.T) *services.SignedAggregateAndProofForGossip {
	t.Helper()
	cfg := &clparams.MainnetBeaconConfig
	const slot = 3
	committee, err := s.headState.GetBeaconCommitee(slot, 0)
	require.NoError(t, err)
	aggregatorIndex := committee[0]

	checkpoint := s.store.JustifiedCheckpoint()
	bits := make([]byte, len(committee)/8+1)
	for i := range committee {
		bits[i/8] |= 1 << (i % 8)
	}
	bits[len(committee)/8] |= 1 << (len(committee) % 8)
	aggregate := &solid.Attestation{
		AggregationBits: solid.BitlistFromBytes(bits, 2048),
		Data: &solid.AttestationData{
			Slot:            slot,
			BeaconBlockRoot: s.rootC,
			Source:          checkpoint,
			Target:          checkpoint,
		},
	}
	aggregate.SetBeaconConfig(cfg)
	signed := &cltypes.SignedAggregateAndProof{
		Message: &cltypes.AggregateAndProof{AggregatorIndex: aggregatorIndex, Aggregate: aggregate},
	}
	signed.SetVersion(clparams.Phase0Version)

	_, attestationRoot, _, err := services.AggregateMessageSignature(s.headState, signed, committee)
	require.NoError(t, err)
	memberSignatures := make([][]byte, 0, len(committee))
	for _, member := range committee {
		memberSignatures = append(memberSignatures, sign(t, member, attestationRoot))
	}
	aggregateSignature, err := bls.AggregateSignatures(memberSignatures)
	require.NoError(t, err)
	copy(aggregate.Signature[:], aggregateSignature)

	_, selectionRoot, _, err := services.AggregateAndProofSignature(s.headState, signed.Message)
	require.NoError(t, err)
	copy(signed.Message.SelectionProof[:], sign(t, aggregatorIndex, selectionRoot))
	require.True(t, state.IsAggregator(cfg, uint64(len(committee)), signed.Message.SelectionProof))

	_, aggregatorRoot, _, err := services.AggregatorSignature(s.headState, signed)
	require.NoError(t, err)
	copy(signed.Signature[:], sign(t, aggregatorIndex, aggregatorRoot))
	return &services.SignedAggregateAndProofForGossip{SignedAggregateAndProof: signed}
}

type votesAppliedForkChoice struct {
	*forkchoice.ForkChoiceStore
	applied chan struct{}
}

func (f *votesAppliedForkChoice) ProcessAttestingIndicies(attestation *solid.Attestation, indices []uint64) {
	f.ForkChoiceStore.ProcessAttestingIndicies(attestation, indices)
	close(f.applied)
}

func TestSlotVotesFromGossipAggregatesKeepHeadOnNodeWithoutValidators(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig

	t.Run("without the aggregate the head flips back to the late block", func(t *testing.T) {
		scenario := newReorgScenario(t)
		require.Equal(t, scenario.rootB, scenario.headAfterTickToSlot4(t))
	})

	t.Run("the aggregate keeps the head on the boosted block", func(t *testing.T) {
		scenario := newReorgScenario(t)
		aggregate := scenario.slotThreeAggregate(t)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		verifier := services.NewBatchSignatureVerifier(ctx, nil)
		verifier.Start()
		opPool := pool.NewOperationsPool(cfg)
		forkChoice := &votesAppliedForkChoice{ForkChoiceStore: scenario.store, applied: make(chan struct{})}
		clock := eth_clock.NewEthereumClock(uint64(time.Now().Unix())-4*cfg.SecondsPerSlot, common.Hash{}, cfg)
		service := services.NewAggregateAndProofService(ctx, scenario.syncedData, forkChoice, clock, cfg, opPool, false, verifier)

		require.NoError(t, service.ProcessMessage(ctx, nil, aggregate))
		select {
		case <-forkChoice.applied:
		case <-time.After(5 * time.Second):
			t.Fatal("fork choice did not apply the aggregate votes")
		}

		require.Equal(t, scenario.rootC, scenario.headAfterTickToSlot4(t))
	})
}
