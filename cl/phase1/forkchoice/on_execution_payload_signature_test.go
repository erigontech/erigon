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

package forkchoice

import (
	"strconv"
	"testing"

	"github.com/hashicorp/golang-lru/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	state2 "github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
)

// The envelope uses another key, so success with one stub call proves only fork-choice verification ran.
func TestOnExecutionPayloadValidatedPathVerifiesBuilderSignatureOnce(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	signAdmissionEnvelopeWithKey(t, cfg, blockState, envelope, otherSigningKey)
	store, _ := newEnvelopeApplicationTestStore(t, cfg, blockState, block)

	calls := stubBLSVerify(t)

	err := store.OnExecutionPayload(t.Context(), envelope, false, true)

	require.Equal(t, 1, *calls)
	require.NoError(t, err)
}

func TestValidateExecutionPayloadEnvelopeForConsensusVerifiesBuilderSignatureOnce(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	signAdmissionEnvelopeWithKey(t, cfg, blockState, envelope, otherSigningKey)
	engine := execution_client.NewMockExecutionEngine(gomock.NewController(t))
	engine.EXPECT().
		NewPayload(gomock.Any(), envelope.Message.Payload, &block.Block.ParentRoot, []common.Hash{}, []hexutil.Bytes{}).
		Return(execution_client.PayloadStatusValidated, nil)
	store, _ := newEnvelopeApplicationTestStore(t, cfg, blockState, block)
	store.engine = engine

	calls := stubBLSVerify(t)

	err := store.ValidateExecutionPayloadEnvelopeForConsensus(t.Context(), envelope)

	require.Equal(t, 1, *calls)
	require.NoError(t, err)
}

func TestValidateExecutionPayloadEnvelopeForConsensusRejectsInvalidBuilderSignature(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	signAdmissionEnvelopeWithKey(t, cfg, blockState, envelope, otherSigningKey)
	engine := execution_client.NewMockExecutionEngine(gomock.NewController(t))
	store, _ := newEnvelopeApplicationTestStore(t, cfg, blockState, block)
	store.engine = engine

	err := store.ValidateExecutionPayloadEnvelopeForConsensus(t.Context(), envelope)

	require.ErrorContains(t, err, "invalid builder signature: invalid signature")
}

func TestOnExecutionPayloadRejectsInvalidBuilderSignature(t *testing.T) {
	for _, validatePayload := range []bool{false, true} {
		t.Run("validatePayload="+strconv.FormatBool(validatePayload), func(t *testing.T) {
			cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
			signAdmissionEnvelopeWithKey(t, cfg, blockState, envelope, otherSigningKey)
			store, graph := newEnvelopeApplicationTestStore(t, cfg, blockState, block)

			err := store.OnExecutionPayload(t.Context(), envelope, false, validatePayload)

			require.ErrorIs(t, err, ErrInvalidExecutionPayloadEnvelope)
			if validatePayload {
				require.ErrorContains(t, err, "invalid builder signature: invalid signature")
			} else {
				require.ErrorContains(t, err, "invalid envelope signature")
			}
			require.False(t, graph.HasEnvelope(envelope.Message.BeaconBlockRoot))
		})
	}
}

func TestOnExecutionPayloadKeepsBlockStateRoot(t *testing.T) {
	cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
	store, _ := newEnvelopeApplicationTestStore(t, cfg, blockState, block)
	blockState.SetPreviousStateRoot(block.Block.StateRoot)
	before, err := blockState.HashSSZ()
	require.NoError(t, err)
	beforePreviousRoot := blockState.PeekPreviousStateRoot()

	require.NoError(t, store.OnExecutionPayload(t.Context(), envelope, false, true))

	after, err := blockState.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Equal(t, beforePreviousRoot, blockState.PeekPreviousStateRoot())
}

func TestOnExecutionPayloadSelfBuildUsesProposerKey(t *testing.T) {
	for _, validatePayload := range []bool{false, true} {
		for _, signer := range []struct {
			name    string
			key     string
			wantErr bool
		}{
			{name: "proposer", key: admissionSigningKey},
			{name: "builder", key: otherSigningKey, wantErr: true},
		} {
			t.Run("validatePayload="+strconv.FormatBool(validatePayload)+"/"+signer.name, func(t *testing.T) {
				cfg, blockState, block, envelope := validAdmissionCancellationFixture(t)
				configureSelfBuildEnvelope(t, cfg, blockState, block, envelope, signer.key)
				store, graph := newEnvelopeApplicationTestStore(t, cfg, blockState, block)

				err := store.OnExecutionPayload(t.Context(), envelope, false, validatePayload)

				if signer.wantErr {
					require.ErrorIs(t, err, ErrInvalidExecutionPayloadEnvelope)
					require.False(t, graph.HasEnvelope(envelope.Message.BeaconBlockRoot))
					return
				}
				require.NoError(t, err)
				require.True(t, graph.HasEnvelope(envelope.Message.BeaconBlockRoot))
			})
		}
	}
}

func newEnvelopeApplicationTestStore(
	t testing.TB,
	cfg *clparams.BeaconChainConfig,
	blockState *state2.CachingBeaconState,
	block *cltypes.SignedBeaconBlock,
) (*ForkChoiceStore, *persistingEnvelopeForkGraph) {
	t.Helper()
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	graph := &persistingEnvelopeForkGraph{dataAvailabilityForkGraph: dataAvailabilityForkGraph{
		state: blockState,
		block: block,
	}}
	store := newPayloadVoteTestStore(t, common.Hash(root), false, false)
	store.beaconCfg = cfg
	store.forkGraph = graph
	store.finalizedCheckpoint.Store(solid.Checkpoint{})
	gasLimits, err := lru.New[common.Hash, uint64](1)
	require.NoError(t, err)
	store.executionPayloadGasLimit = gasLimits
	return store, graph
}

func stubBLSVerify(t testing.TB) *int {
	t.Helper()
	calls := new(int)
	original := blsVerify
	blsVerify = func([]byte, []byte, []byte) (bool, error) {
		(*calls)++
		return true, nil
	}
	t.Cleanup(func() { blsVerify = original })
	return calls
}

func configureSelfBuildEnvelope(
	t testing.TB,
	cfg *clparams.BeaconChainConfig,
	blockState *state2.CachingBeaconState,
	block *cltypes.SignedBeaconBlock,
	envelope *cltypes.SignedExecutionPayloadEnvelope,
	signingKey string,
) {
	t.Helper()
	builderKey, err := bls.NewPrivateKeyFromIKM([]byte(otherSigningKey))
	require.NoError(t, err)
	blockState.GetBuilders().Get(0).Pubkey = common.Bytes48(bls.CompressPublicKey(builderKey.PublicKey()))

	bid := block.Block.Body.GetSignedExecutionPayloadBid().Message
	bid.BuilderIndex = clparams.BuilderIndexSelfBuild
	blockState.SetLatestExecutionPayloadBid(bid)
	envelope.Message.BuilderIndex = clparams.BuilderIndexSelfBuild
	rebindAdmissionBlockRoot(t, blockState, block, envelope)
	signAdmissionEnvelopeWithKey(t, cfg, blockState, envelope, signingKey)
}
