package services

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/merkle_tree"
	state2 "github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
)

var forkBoundaryGenesisValidatorsRoot = common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")

// forkBoundaryConfig activates every fork before newFork at genesis and newFork at epoch 2.
func forkBoundaryConfig(newFork clparams.StateVersion) clparams.BeaconChainConfig {
	cfg := clparams.MainnetBeaconConfig
	epochs := []*uint64{&cfg.AltairForkEpoch, &cfg.BellatrixForkEpoch, &cfg.CapellaForkEpoch, &cfg.DenebForkEpoch, &cfg.ElectraForkEpoch, &cfg.FuluForkEpoch, &cfg.GloasForkEpoch}
	for i, epoch := range epochs {
		switch {
		case clparams.StateVersion(i+1) < newFork:
			*epoch = 0
		case clparams.StateVersion(i+1) == newFork:
			*epoch = 2
		}
	}
	return cfg
}

// newPreForkHeadState returns the last state before cfg's epoch-2 fork, still carrying the previous fork, with one
// validator whose key it returns.
func newPreForkHeadState(t *testing.T, cfg *clparams.BeaconChainConfig, newFork clparams.StateVersion) (*state2.CachingBeaconState, *bls.PrivateKey) {
	t.Helper()
	oldFork := newFork - 1
	headState := state2.New(cfg)
	headState.SetVersion(oldFork)
	require.NoError(t, headState.SetSlot(2*cfg.SlotsPerEpoch-1))
	headState.SetGenesisValidatorsRoot(forkBoundaryGenesisValidatorsRoot)
	headState.SetFork(&cltypes.Fork{
		PreviousVersion: utils.Uint32ToBytes4(cfg.GetForkVersionByVersion(oldFork - 1)),
		CurrentVersion:  utils.Uint32ToBytes4(cfg.GetForkVersionByVersion(oldFork)),
	})
	key, err := bls.GenerateKey()
	require.NoError(t, err)
	pubKey := common.Bytes48(bls.CompressPublicKey(key.PublicKey()))
	require.NoError(t, headState.AddValidator(solid.NewValidatorFromParameters(pubKey, common.Hash{}, 0, false, 0, 0, cfg.FarFutureEpoch, cfg.FarFutureEpoch), 0))
	return headState, key
}

// When the first slot of a fork has no block, messages for that slot are checked against the last state of the
// previous fork. Messages from the previous fork's last slot can also arrive after the head has crossed the fork.
func TestGossipSignatureDomainsUseMessageEpochForkVersion(t *testing.T) {
	cfg := forkBoundaryConfig(clparams.GloasVersion)
	preForkHeadState, key := newPreForkHeadState(t, &cfg, clparams.GloasVersion)
	pubKey := common.Bytes48(bls.CompressPublicKey(key.PublicKey()))
	gloasSlot := cfg.GloasForkEpoch * cfg.SlotsPerEpoch
	postForkHeadState, err := preForkHeadState.Copy()
	require.NoError(t, err)
	require.NoError(t, postForkHeadState.SetSlot(gloasSlot))
	postForkHeadState.SetVersion(clparams.GloasVersion)
	postForkHeadState.SetFork(&cltypes.Fork{
		PreviousVersion: utils.Uint32ToBytes4(cfg.GetForkVersionByVersion(clparams.FuluVersion)),
		CurrentVersion:  utils.Uint32ToBytes4(cfg.GetForkVersionByVersion(clparams.GloasVersion)),
		Epoch:           cfg.GloasForkEpoch,
	})

	for _, boundary := range []struct {
		name      string
		slot      uint64
		version   clparams.StateVersion
		headState *state2.CachingBeaconState
	}{
		{name: "first slot of the new fork", slot: gloasSlot, version: clparams.GloasVersion, headState: preForkHeadState},
		{name: "last slot of the previous fork", slot: gloasSlot - 1, version: clparams.FuluVersion, headState: postForkHeadState},
	} {
		t.Run(boundary.name, func(t *testing.T) {
			headState := boundary.headState
			slot, epoch := boundary.slot, boundary.slot/cfg.SlotsPerEpoch
			domain := func(domainType [4]byte) []byte {
				domain, err := fork.ComputeDomain(domainType[:], utils.Uint32ToBytes4(cfg.GetForkVersionByVersion(boundary.version)), forkBoundaryGenesisValidatorsRoot)
				require.NoError(t, err)
				return domain
			}
			sha := func(root, domain []byte) []byte {
				h := crypto.Sha256(root, domain)
				return h[:]
			}
			signingRoot := func(obj interface{ HashSSZ() ([32]byte, error) }, domainType [4]byte) []byte {
				root, err := fork.ComputeSigningRoot(obj, domain(domainType))
				require.NoError(t, err)
				return root[:]
			}

			blockRoot := common.HexToHash("0x2222222222222222222222222222222222222222222222222222222222222222")
			contribution := &cltypes.Contribution{Slot: slot, BeaconBlockRoot: blockRoot, AggregationBits: []byte{1}}
			contributionAndProof := &cltypes.SignedContributionAndProof{Message: &cltypes.ContributionAndProof{Contribution: contribution}}
			attestationData := &solid.AttestationData{
				Slot:            slot,
				BeaconBlockRoot: blockRoot,
				Target:          solid.Checkpoint{Epoch: epoch, Root: blockRoot},
			}
			aggregate := &cltypes.SignedAggregateAndProof{Message: &cltypes.AggregateAndProof{
				Aggregate: &solid.Attestation{AggregationBits: solid.BitlistFromBytes([]byte{1}, 2048), Data: attestationData},
			}}
			selectionRoot := merkle_tree.Uint64Root(slot)

			for _, tt := range []struct {
				name string
				sign func() ([]byte, []byte, []byte, error)
				want []byte
			}{
				{
					name: "sync committee message",
					sign: func() ([]byte, []byte, []byte, error) {
						return verifySyncCommitteeMessageSignature(headState, &cltypes.SyncCommitteeMessage{Slot: slot, BeaconBlockRoot: blockRoot})
					},
					want: sha(blockRoot[:], domain(cfg.DomainSyncCommittee)),
				},
				{
					name: "sync contribution aggregate",
					sign: func() ([]byte, []byte, []byte, error) {
						return verifySyncContributionProofAggregatedSignature(headState, contribution, []common.Bytes48{pubKey})
					},
					want: sha(blockRoot[:], domain(cfg.DomainSyncCommittee)),
				},
				{
					name: "sync contribution selection proof",
					sign: func() ([]byte, []byte, []byte, error) {
						return verifySyncContributionSelectionProof(headState, contributionAndProof.Message)
					},
					want: signingRoot(&cltypes.SyncAggregatorSelectionData{Slot: slot}, cfg.DomainSyncCommitteeSelectionProof),
				},
				{
					name: "sync contribution aggregator",
					sign: func() ([]byte, []byte, []byte, error) {
						return verifyAggregatorSignatureForSyncContribution(headState, contributionAndProof)
					},
					want: signingRoot(contributionAndProof.Message, cfg.DomainContributionAndProof),
				},
				{
					name: "aggregate selection proof",
					sign: func() ([]byte, []byte, []byte, error) {
						return AggregateAndProofSignature(headState, aggregate.Message)
					},
					want: sha(selectionRoot[:], domain(cfg.DomainSelectionProof)),
				},
				{
					name: "aggregator",
					sign: func() ([]byte, []byte, []byte, error) {
						return AggregatorSignature(headState, aggregate)
					},
					want: signingRoot(aggregate.Message, cfg.DomainAggregateAndProof),
				},
				{
					name: "aggregate attestation",
					sign: func() ([]byte, []byte, []byte, error) {
						return AggregateMessageSignature(headState, aggregate, []uint64{0})
					},
					want: signingRoot(attestationData, cfg.DomainBeaconAttester),
				},
			} {
				t.Run(tt.name, func(t *testing.T) {
					_, root, _, err := tt.sign()
					require.NoError(t, err)
					require.Equal(t, tt.want, root)
				})
			}
		})
	}
}

// signedHeaderAtFork returns a block header for the first slot of cfg's epoch-2 fork, signed for that fork.
func signedHeaderAtFork(t *testing.T, cfg *clparams.BeaconChainConfig, newFork clparams.StateVersion, key *bls.PrivateKey, parentRoot common.Hash) *cltypes.SignedBeaconBlockHeader {
	t.Helper()
	header := &cltypes.SignedBeaconBlockHeader{Header: &cltypes.BeaconBlockHeader{Slot: 2 * cfg.SlotsPerEpoch, ParentRoot: parentRoot}}
	domain, err := fork.ComputeDomain(cfg.DomainBeaconProposer[:], utils.Uint32ToBytes4(cfg.GetForkVersionByVersion(newFork)), forkBoundaryGenesisValidatorsRoot)
	require.NoError(t, err)
	root, err := fork.ComputeSigningRoot(header.Header, domain)
	require.NoError(t, err)
	copy(header.Signature[:], key.Sign(root[:]).Bytes())
	return header
}

func TestDataColumnSidecarProposerSignatureUsesHeaderEpochForkVersion(t *testing.T) {
	cfg := forkBoundaryConfig(clparams.FuluVersion)
	headState, key := newPreForkHeadState(t, &cfg, clparams.FuluVersion)
	syncedData := synced_data.NewSyncedDataManager(&cfg, true)
	require.NoError(t, syncedData.OnHeadState(headState))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	service := NewDataColumnSidecarService(ctx, &cfg, nil, nil, syncedData, nil, nil).(*dataColumnSidecarService)

	valid, err := service.verifyProposerSignature(0, signedHeaderAtFork(t, &cfg, clparams.FuluVersion, key, common.Hash{}))
	require.NoError(t, err)
	require.True(t, valid)
}

func TestBlobSidecarProposerSignatureUsesHeaderEpochForkVersion(t *testing.T) {
	cfg := forkBoundaryConfig(clparams.ElectraVersion)
	headState, key := newPreForkHeadState(t, &cfg, clparams.ElectraVersion)
	syncedData := synced_data.NewSyncedDataManager(&cfg, true)
	require.NoError(t, syncedData.OnHeadState(headState))
	parentRoot := common.HexToHash("0x3333333333333333333333333333333333333333333333333333333333333333")
	forkChoice := mock_services.NewForkChoiceStorageMock(t)
	forkChoice.Headers[parentRoot] = &cltypes.BeaconBlockHeader{Slot: headState.Slot()}
	service := NewBlobSidecarService(&cfg, forkChoice, syncedData, nil, nil, false).(*blobSidecarService)

	require.NoError(t, service.verifySidecarsSignature(signedHeaderAtFork(t, &cfg, clparams.ElectraVersion, key, parentRoot)))
}
