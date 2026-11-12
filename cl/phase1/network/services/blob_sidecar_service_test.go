// Copyright 2024 The Erigon Authors
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

package services

import (
	"context"
	_ "embed"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
)

//go:embed test_data/blob_sidecar_service_blob.ssz_snappy
var blobSidecarServiceBlob []byte

//go:embed test_data/blob_sidecar_service_block.ssz_snappy
var blobSidecarServiceBlock []byte

//go:embed test_data/blob_sidecar_service_state.ssz_snappy
var blobSidecarServiceState []byte

func getObjectsForBlobSidecarServiceTests(t *testing.T) (*state.CachingBeaconState, *cltypes.SignedBeaconBlock, *cltypes.BlobSidecar) {
	stateObj := state.New(&clparams.MainnetBeaconConfig)
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	blob := cltypes.Blob{}
	require.NoError(t, utils.DecodeSSZSnappy(stateObj, blobSidecarServiceState, int(clparams.DenebVersion)))
	require.NoError(t, utils.DecodeSSZSnappy(block, blobSidecarServiceBlock, int(clparams.DenebVersion)))
	require.NoError(t, utils.DecodeSSZSnappy(&blob, blobSidecarServiceBlob, int(clparams.DenebVersion)))
	var proof common.Bytes48
	proofStr := "0xb5a64254a75a8dd4e5fde529e2f657de268fcb9eedff43363c946f40bbf36ef16ee13a890504a7a8c4f689085146ad51"
	proofBytes := common.Hex2Bytes(proofStr[2:])
	copy(proof[:], proofBytes)
	sidecar := &cltypes.BlobSidecar{
		Index:                    uint64(0),
		SignedBlockHeader:        block.SignedBeaconBlockHeader(),
		Blob:                     blob,
		KzgCommitment:            common.Bytes48(*block.Block.Body.BlobKzgCommitments.Get(0)),
		KzgProof:                 proof,
		CommitmentInclusionProof: solid.NewHashVector(cltypes.CommitmentBranchSize),
	}
	return stateObj, block, sidecar
}

func setupBlobSidecarService(t *testing.T, ctrl *gomock.Controller, test bool) (BlobSidecarsService, *synced_data.SyncedDataManager, *eth_clock.MockEthereumClock, *mock_services.ForkChoiceStorageMock) {
	cfg := &clparams.MainnetBeaconConfig
	syncedDataManager := synced_data.NewSyncedDataManager(cfg, true)
	ethClock := eth_clock.NewMockEthereumClock(ctrl)
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
	emitters := beaconevents.NewEventEmitter()
	blockService := NewBlobSidecarService(t.Context(), cfg, forkchoiceMock, syncedDataManager, ethClock, emitters, test)
	return blockService, syncedDataManager, ethClock, forkchoiceMock
}

func TestBlobServiceUnsynced(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blobService, _, _, _ := setupBlobSidecarService(t, ctrl, true)

	ctx := t.Context()
	require.Error(t, blobService.ProcessMessage(ctx, nil, &cltypes.BlobSidecar{}))
}

func TestBlobServiceInvalidIndex(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blobService, syncedData, _, _ := setupBlobSidecarService(t, ctrl, true)
	stateObj, _, _ := getObjectsForBlobSidecarServiceTests(t)
	require.NoError(t, syncedData.OnHeadState(stateObj))

	ctx := t.Context()
	require.Error(t, blobService.ProcessMessage(ctx, nil, &cltypes.BlobSidecar{
		Index: 99999,
	}))
}

func TestBlobServiceInvalidSubnet(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blobService, syncedData, _, _ := setupBlobSidecarService(t, ctrl, true)
	stateObj, _, _ := getObjectsForBlobSidecarServiceTests(t)
	require.NoError(t, syncedData.OnHeadState(stateObj))
	sn := uint64(99999)

	ctx := t.Context()
	require.Error(t, blobService.ProcessMessage(ctx, &sn, &cltypes.BlobSidecar{
		Index: 0,
	}))
}

func TestBlobServiceBadTimings(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blobService, syncedData, ethClock, _ := setupBlobSidecarService(t, ctrl, false)
	stateObj, _, blobSidecar := getObjectsForBlobSidecarServiceTests(t)
	require.NoError(t, syncedData.OnHeadState(stateObj))
	sn := uint64(0)

	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(false).AnyTimes()

	ctx := t.Context()
	require.Error(t, blobService.ProcessMessage(ctx, &sn, blobSidecar))
}

func TestBlobServiceAlreadyHave(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blobService, syncedData, ethClock, fcu := setupBlobSidecarService(t, ctrl, false)
	stateObj, _, blobSidecar := getObjectsForBlobSidecarServiceTests(t)
	require.NoError(t, syncedData.OnHeadState(stateObj))
	sn := uint64(0)
	sidecarRoot, err := blobSidecar.SignedBlockHeader.Header.HashSSZ()
	require.NoError(t, err)

	fcu.Headers[sidecarRoot] = blobSidecar.SignedBlockHeader.Header.Copy()

	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()

	ctx := t.Context()
	require.Error(t, blobService.ProcessMessage(ctx, &sn, blobSidecar))
}

func TestBlobServiceDontHaveParentRoot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blobService, syncedData, ethClock, _ := setupBlobSidecarService(t, ctrl, false)
	stateObj, _, blobSidecar := getObjectsForBlobSidecarServiceTests(t)
	require.NoError(t, syncedData.OnHeadState(stateObj))
	sn := uint64(0)

	// fcu.Headers[blobSidecar.SignedBlockHeader.Header.ParentRoot] = blobSidecar.SignedBlockHeader.Header.Copy()

	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()

	ctx := t.Context()
	require.Error(t, blobService.ProcessMessage(ctx, &sn, blobSidecar))
}

func TestBlobServiceInvalidSidecarSlot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blobService, syncedData, ethClock, fcu := setupBlobSidecarService(t, ctrl, false)
	stateObj, _, blobSidecar := getObjectsForBlobSidecarServiceTests(t)
	require.NoError(t, syncedData.OnHeadState(stateObj))
	sn := uint64(0)

	fcu.Headers[blobSidecar.SignedBlockHeader.Header.ParentRoot] = blobSidecar.SignedBlockHeader.Header.Copy()

	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()

	ctx := t.Context()
	require.Error(t, blobService.ProcessMessage(ctx, &sn, blobSidecar))
}

func TestBlobServiceSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blobService, syncedData, ethClock, fcu := setupBlobSidecarService(t, ctrl, true)
	stateObj, _, blobSidecar := getObjectsForBlobSidecarServiceTests(t)
	require.NoError(t, syncedData.OnHeadState(stateObj))
	sn := uint64(0)

	fcu.Headers[blobSidecar.SignedBlockHeader.Header.ParentRoot] = blobSidecar.SignedBlockHeader.Header.Copy()
	fcu.Headers[blobSidecar.SignedBlockHeader.Header.ParentRoot].Slot--
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()

	require.NoError(t, blobService.ProcessMessage(context.Background(), &sn, blobSidecar))
}

type blobProposerForkChoice struct {
	*mock_services.ForkChoiceStorageMock
	stored []*cltypes.BlobSidecar
}

func (f *blobProposerForkChoice) AddPreverifiedBlobSidecar(sidecar *cltypes.BlobSidecar) error {
	f.stored = append(f.stored, sidecar)
	return nil
}

func setupBlobProposerTest(t *testing.T, version clparams.StateVersion, wrongProposer bool) (*blobSidecarService, *blobProposerForkChoice, *cltypes.BlobSidecar) {
	t.Helper()
	cfg := forkBoundaryConfig(version + 1)
	parentState := state.New(&cfg)
	parentState.SetVersion(version)
	require.NoError(t, parentState.SetSlot(1))
	keys := make([]*bls.PrivateKey, 2)
	for i := range keys {
		key, err := bls.GenerateKey()
		require.NoError(t, err)
		keys[i] = key
		balance, exitEpoch := cfg.MaxEffectiveBalance, cfg.FarFutureEpoch
		validator := solid.NewValidatorFromParameters(common.Bytes48(bls.CompressPublicKey(key.PublicKey())), common.Hash{}, balance, false, 0, 0, exitEpoch, exitEpoch)
		require.NoError(t, parentState.AddValidator(validator, balance))
	}
	_, block, sidecar := getObjectsForBlobSidecarServiceTests(t)
	proof, err := block.Block.Body.KzgCommitmentMerkleProof(int(sidecar.Index))
	require.NoError(t, err)
	for i, hash := range proof {
		sidecar.CommitmentInclusionProof.Set(i, hash)
	}
	header := sidecar.SignedBlockHeader.Header
	parentProposer, err := parentState.GetBeaconProposerIndexForSlot(parentState.Slot())
	require.NoError(t, err)
	var expected uint64
	for header.Slot = parentState.Slot() + 1; ; header.Slot++ {
		require.Less(t, header.Slot+1, cfg.SlotsPerEpoch)
		expected, err = parentState.GetBeaconProposerIndexForSlot(header.Slot)
		require.NoError(t, err)
		nextProposer, err := parentState.GetBeaconProposerIndexForSlot(header.Slot + 1)
		require.NoError(t, err)
		if expected != parentProposer && expected != nextProposer {
			break
		}
	}
	header.ParentRoot = common.Hash{1}
	proposerIndex := expected
	if wrongProposer {
		proposerIndex = 1 - expected
	}
	header.ProposerIndex = proposerIndex
	domain, err := fork.ComputeDomainAtEpoch(&cfg, cfg.DomainBeaconProposer, header.Slot/cfg.SlotsPerEpoch, parentState.GenesisValidatorsRoot())
	require.NoError(t, err)
	signingRoot, err := fork.ComputeSigningRoot(header, domain)
	require.NoError(t, err)
	copy(sidecar.SignedBlockHeader.Signature[:], keys[proposerIndex].Sign(signingRoot[:]).Bytes())
	syncedData := synced_data.NewSyncedDataManager(&cfg, true)
	require.NoError(t, syncedData.OnHeadState(parentState))
	fc := &blobProposerForkChoice{ForkChoiceStorageMock: mock_services.NewForkChoiceStorageMock(t)}
	fc.Headers[header.ParentRoot] = &cltypes.BeaconBlockHeader{Slot: parentState.Slot()}
	fc.StateAtBlockRootVal[header.ParentRoot] = parentState
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().GetCurrentSlot().Return(header.Slot).AnyTimes()
	service := NewBlobSidecarService(t.Context(), &cfg, fc, syncedData, clock, beaconevents.NewEventEmitter(), false).(*blobSidecarService)
	return service, fc, sidecar
}

func TestBlobServiceExpectedProposer(t *testing.T) {
	for _, version := range []clparams.StateVersion{clparams.DenebVersion, clparams.ElectraVersion} {
		t.Run(version.String(), func(t *testing.T) {
			for _, tc := range []struct {
				name          string
				wrongProposer bool
				missingState  bool
				invalidProof  bool
				invalidSig    bool
				wantError     string
				wantIgnore    bool
			}{
				{name: "unexpected proposer", wrongProposer: true, wantError: "does not match expected proposer"},
				{name: "expected proposer"},
				{name: "unavailable parent state", missingState: true, wantError: "view parent state", wantIgnore: true},
				{name: "invalid proof with unavailable state", missingState: true, invalidProof: true, wantError: "blob KZG proof verification failed"},
				{name: "invalid signature with unavailable state", missingState: true, invalidSig: true, wantError: "signature not valid"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					service, fc, sidecar := setupBlobProposerTest(t, version, tc.wrongProposer)
					if tc.missingState {
						delete(fc.StateAtBlockRootVal, sidecar.SignedBlockHeader.Header.ParentRoot)
					}
					if tc.invalidProof {
						sidecar.KzgProof = common.Bytes48{}
					}
					if tc.invalidSig {
						sidecar.SignedBlockHeader.Header.Root[0] ^= 1
					}
					events := make(chan *beaconevents.EventStream, 1)
					subscription := service.emitters.Operation().Subscribe(events)
					defer subscription.Unsubscribe()
					subnet := uint64(0)

					err := service.ProcessMessage(t.Context(), &subnet, sidecar)

					if tc.wantError == "" {
						require.NoError(t, err)
						require.Equal(t, []*cltypes.BlobSidecar{sidecar}, fc.stored)
						require.Len(t, events, 1)
						return
					}
					require.ErrorContains(t, err, tc.wantError)
					if tc.wantIgnore {
						require.ErrorIs(t, err, ErrIgnore)
					} else {
						require.NotErrorIs(t, err, ErrIgnore)
					}
					require.Empty(t, fc.stored)
					require.Empty(t, events)
				})
			}
		})
	}
}
