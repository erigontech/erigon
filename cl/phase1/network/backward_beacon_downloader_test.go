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

package network

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/sentinel/communication"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/sentinel/peers"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

// makeGloasBlock creates a GLOAS SignedBeaconBlock with the given bid hashes.
func makeGloasBlock(slot uint64, blockHash, parentBlockHash common.Hash) *cltypes.SignedBeaconBlock {
	blk := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	blk.Block.Slot = slot
	blk.Block.Body.SyncAggregate = cltypes.NewSyncAggregate()
	bid := blk.Block.Body.GetSignedExecutionPayloadBid()
	bid.Message.BlockHash = blockHash
	bid.Message.ParentBlockHash = parentBlockHash
	return blk
}

// makeDenebBlock creates a pre-GLOAS (Deneb) SignedBeaconBlock.
func makeDenebBlock(slot uint64) *cltypes.SignedBeaconBlock {
	blk := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	blk.Block.Slot = slot
	blk.Block.Body.SyncAggregate = cltypes.NewSyncAggregate()
	return blk
}

func hash(b byte) common.Hash {
	var h common.Hash
	h[0] = b
	return h
}

type beaconBlockBodyReaderFunc func(context.Context, kv.Tx, common.Hash) (*cltypes.SignedBeaconBlock, error)

func (f beaconBlockBodyReaderFunc) ReadBlockByRoot(ctx context.Context, tx kv.Tx, root common.Hash) (*cltypes.SignedBeaconBlock, error) {
	return f(ctx, tx, root)
}

func acceptGloasSuccessor(*cltypes.SignedBeaconBlock) error { return nil }

func TestNewGloasSuccessorValidatorRejectsMalformedSuccessor(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	anchorState := state.New(cfg)
	anchorState.SetVersion(clparams.GloasVersion)
	require.NoError(t, anchorState.SetSlot(10))
	anchorRoot, err := anchorState.BlockRoot()
	require.NoError(t, err)

	validator := NewGloasSuccessorValidator(anchorState, anchorRoot)
	require.ErrorContains(t, validator(nil), "missing GLOAS successor")

	wrongParent := makeGloasBlock(11, hash(0xaa), hash(0xbb))
	require.ErrorContains(t, validator(wrongParent), "parent root mismatch")

	malformed := makeGloasBlock(11, hash(0xaa), hash(0xbb))
	malformed.Block.ParentRoot = anchorRoot
	require.ErrorContains(t, validator(malformed), "transition GLOAS successor")
	require.Equal(t, uint64(10), anchorState.Slot())
}

func TestValidateBackfilledGloasEnvelopeSignature(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	envelopeSlot := 2 * cfg.SlotsPerEpoch
	envelopeEpoch := state.GetEpochAtSlot(cfg, envelopeSlot)
	cfg.GloasForkEpoch = envelopeEpoch

	for _, tc := range []struct {
		name         string
		builderIndex uint64
		depositEpoch uint64
		signingKey   string
		wantErr      bool
	}{
		{name: "self build proposer key", builderIndex: clparams.BuilderIndexSelfBuild, signingKey: backfillProposerKey},
		{name: "self build other key", builderIndex: clparams.BuilderIndexSelfBuild, signingKey: backfillOtherKey, wantErr: true},
		{name: "known builder key", depositEpoch: envelopeEpoch - 1, signingKey: backfillBuilderKey},
		{name: "known builder other key", depositEpoch: envelopeEpoch - 1, signingKey: backfillOtherKey, wantErr: true},
		{name: "builder registered in envelope epoch", depositEpoch: envelopeEpoch, signingKey: backfillOtherKey},
		{name: "unknown builder index", builderIndex: 1, depositEpoch: envelopeEpoch - 1, signingKey: backfillOtherKey},
	} {
		t.Run(tc.name, func(t *testing.T) {
			anchorState, block, envelope := backfilledGloasEnvelopeFixture(t, cfg, envelopeSlot, tc.builderIndex, tc.depositEpoch, tc.signingKey)

			err := validateBackfilledGloasEnvelopeSignature(anchorState, block, envelope)

			if tc.wantErr {
				require.ErrorContains(t, err, "invalid signature")
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestGloasEnvelopeSignatureVerifiable(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	envelopeSlot := 2 * cfg.SlotsPerEpoch
	envelopeEpoch := state.GetEpochAtSlot(cfg, envelopeSlot)
	cfg.GloasForkEpoch = envelopeEpoch

	for _, tc := range []struct {
		name         string
		builderIndex uint64
		depositEpoch uint64
		signingKey   string
		setValidator bool
		mutate       func(*state.CachingBeaconState, *cltypes.SignedBeaconBlock)
		want         bool
	}{
		{name: "self build", builderIndex: clparams.BuilderIndexSelfBuild, signingKey: backfillProposerKey, setValidator: true, want: true},
		{name: "self build proposer outside validator set", builderIndex: clparams.BuilderIndexSelfBuild, signingKey: backfillProposerKey, setValidator: true, mutate: func(_ *state.CachingBeaconState, block *cltypes.SignedBeaconBlock) { block.Block.ProposerIndex = 1 }},
		{name: "known builder", depositEpoch: envelopeEpoch - 1, signingKey: backfillBuilderKey, setValidator: true, want: true},
		{name: "builder deposited in envelope epoch", depositEpoch: envelopeEpoch, signingKey: backfillOtherKey, setValidator: true},
		{name: "builder deposited after envelope epoch", depositEpoch: envelopeEpoch + 1, signingKey: backfillOtherKey, setValidator: true},
		{name: "builder index outside registry", builderIndex: 1, depositEpoch: envelopeEpoch - 1, signingKey: backfillOtherKey, setValidator: true},
		{name: "nil builders list", depositEpoch: envelopeEpoch - 1, signingKey: backfillBuilderKey, setValidator: true, mutate: func(anchorState *state.CachingBeaconState, _ *cltypes.SignedBeaconBlock) {
			anchorState.SetBuilders(nil)
		}},
		{name: "no validator", builderIndex: clparams.BuilderIndexSelfBuild, signingKey: backfillProposerKey},
	} {
		t.Run(tc.name, func(t *testing.T) {
			anchorState, block, envelope := backfilledGloasEnvelopeFixture(t, cfg, envelopeSlot, tc.builderIndex, tc.depositEpoch, tc.signingKey)
			if tc.mutate != nil {
				tc.mutate(anchorState, block)
			}
			downloader := NewBackwardBeaconDownloader(t.Context(), nil, nil, nil, nil, cfg)
			t.Cleanup(downloader.reqInterval.Stop)
			if tc.setValidator {
				downloader.SetGloasEnvelopeSignatureValidator(anchorState)
			}

			require.Equal(t, tc.want, downloader.GloasEnvelopeSignatureVerifiable(block, envelope))
		})
	}
}

func TestSetGloasEnvelopeSignatureValidatorWiresRangeFilter(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	envelopeSlot := 2 * cfg.SlotsPerEpoch
	cfg.GloasForkEpoch = state.GetEpochAtSlot(cfg, envelopeSlot)
	anchorState, block, validEnvelope := backfilledGloasEnvelopeFixture(t, cfg, envelopeSlot, clparams.BuilderIndexSelfBuild, 0, backfillProposerKey)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	invalidEnvelope := *validEnvelope
	signBackfilledGloasEnvelope(t, cfg, anchorState, &invalidEnvelope, backfillOtherKey)

	downloader := NewBackwardBeaconDownloader(t.Context(), nil, nil, nil, nil, cfg)
	t.Cleanup(downloader.reqInterval.Stop)
	downloader.SetGloasEnvelopeSignatureValidator(anchorState)

	envelopes := map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{root: &invalidEnvelope}
	downloader.discardInvalidGloasEnvelopes([]*cltypes.SignedBeaconBlock{block}, envelopes)
	require.NotContains(t, envelopes, common.Hash(root))

	envelopes[root] = validEnvelope
	downloader.discardInvalidGloasEnvelopes([]*cltypes.SignedBeaconBlock{block}, envelopes)
	require.Same(t, validEnvelope, envelopes[root])
}

const (
	backfillProposerKey = "01234567890123456789012345678901"
	backfillBuilderKey  = "builder-key-material-000000000001"
	backfillOtherKey    = "abcdefghijklmnopqrstuvwxyzABCDEF"
)

func backfilledGloasEnvelopeFixture(
	t *testing.T,
	cfg *clparams.BeaconChainConfig,
	envelopeSlot uint64,
	builderIndex uint64,
	depositEpoch uint64,
	signingKey string,
) (*state.CachingBeaconState, *cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope) {
	t.Helper()

	anchorState := state.New(cfg)
	anchorState.SetVersion(clparams.GloasVersion)
	require.NoError(t, anchorState.SetSlot(envelopeSlot+cfg.SlotsPerEpoch))
	anchorState.SetGenesisValidatorsRoot(common.HexToHash("0x01"))
	proposerKey, err := bls.NewPrivateKeyFromIKM([]byte(backfillProposerKey))
	require.NoError(t, err)
	proposerPubkey := common.Bytes48(bls.CompressPublicKey(proposerKey.PublicKey()))
	require.NoError(t, anchorState.AddValidator(solid.NewValidatorFromParameters(
		proposerPubkey, common.Hash{}, cfg.MaxEffectiveBalance, false, 0, 0, cfg.FarFutureEpoch, cfg.FarFutureEpoch,
	), cfg.MaxEffectiveBalance))
	builderKey, err := bls.NewPrivateKeyFromIKM([]byte(backfillBuilderKey))
	require.NoError(t, err)
	builderPubkey := common.Bytes48(bls.CompressPublicKey(builderKey.PublicKey()))
	builders := solid.NewStaticListSSZ[*cltypes.Builder](64, 73)
	builders.Append(&cltypes.Builder{Pubkey: builderPubkey, DepositEpoch: depositEpoch})
	anchorState.SetBuilders(builders)

	parentRoot := common.HexToHash("0x11")
	requests := cltypes.NewExecutionRequestsWithVersion(cfg, clparams.GloasVersion)
	requestsRoot, err := requests.HashSSZ()
	require.NoError(t, err)
	requestsHash := cltypes.ComputeExecutionRequestHash(cltypes.GetExecutionRequestsList(cfg, requests))
	payload := cltypes.NewEth1Block(clparams.GloasVersion, cfg)
	payload.SlotNumber = envelopeSlot
	payload.Extra = solid.NewExtraData()
	payload.Transactions = &solid.TransactionsSSZ{}
	payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44)
	payload.BlockAccessList = solid.NewByteListSSZ(cfg.MaxBytesPerTransaction)
	payload.BlockHash, err = payload.ComputeBlockHash(&parentRoot, requestsHash, nil)
	require.NoError(t, err)

	block := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
	block.Block.Slot = envelopeSlot
	block.Block.ProposerIndex = 0
	bid := block.Block.Body.GetSignedExecutionPayloadBid().Message
	bid.ParentBlockHash = payload.ParentHash
	bid.ParentBlockRoot = parentRoot
	bid.BlockHash = payload.BlockHash
	bid.PrevRandao = payload.PrevRandao
	bid.GasLimit = payload.GasLimit
	bid.BuilderIndex = builderIndex
	bid.Slot = envelopeSlot
	bid.ExecutionRequestsRoot = requestsRoot
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)

	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{
		Payload:               payload,
		ExecutionRequests:     requests,
		BuilderIndex:          builderIndex,
		BeaconBlockRoot:       blockRoot,
		ParentBeaconBlockRoot: parentRoot,
	}}
	signBackfilledGloasEnvelope(t, cfg, anchorState, envelope, signingKey)
	require.NoError(t, ValidateDownloadedGloasEnvelope(cfg, block, envelope))
	return anchorState, block, envelope
}

func signBackfilledGloasEnvelope(
	t *testing.T,
	cfg *clparams.BeaconChainConfig,
	anchorState *state.CachingBeaconState,
	envelope *cltypes.SignedExecutionPayloadEnvelope,
	keyMaterial string,
) {
	t.Helper()
	privateKey, err := bls.NewPrivateKeyFromIKM([]byte(keyMaterial))
	require.NoError(t, err)
	domain, err := fork.ComputeDomain(cfg.DomainBeaconBuilder[:], utils.Uint32ToBytes4(uint32(cfg.GloasForkVersion)), anchorState.GenesisValidatorsRoot())
	require.NoError(t, err)
	signingRoot, err := fork.ComputeSigningRoot(envelope.Message, domain)
	require.NoError(t, err)
	copy(envelope.Signature[:], privateKey.Sign(signingRoot[:]).Bytes())
}

// TestDetermineGloasFullRoots_EmptyBatch verifies that an empty batch returns no roots.
func TestDetermineGloasFullRoots_EmptyBatch(t *testing.T) {
	roots := determineGloasFullRoots(nil, nil)
	assert.Empty(t, roots)

	roots = determineGloasFullRoots([]*cltypes.SignedBeaconBlock{}, nil)
	assert.Empty(t, roots)
}

// TestDetermineGloasFullRoots_AllPreGloas verifies that pre-GLOAS blocks are ignored.
func TestDetermineGloasFullRoots_AllPreGloas(t *testing.T) {
	responses := []*cltypes.SignedBeaconBlock{
		makeDenebBlock(100),
		makeDenebBlock(101),
		makeDenebBlock(102),
	}
	roots := determineGloasFullRoots(responses, nil)
	assert.Empty(t, roots)
}

// TestDetermineGloasFullRoots_SingleBlock_NilLookahead verifies that a single GLOAS block
// with no prevBatchTopBlock (first batch ever) is treated optimistically as FULL.
func TestDetermineGloasFullRoots_SingleBlock_NilLookahead(t *testing.T) {
	blk := makeGloasBlock(100, hash(0xAA), hash(0x00))
	responses := []*cltypes.SignedBeaconBlock{blk}

	roots := determineGloasFullRoots(responses, nil)
	require.Len(t, roots, 1)

	expected, err := blk.Block.HashSSZ()
	require.NoError(t, err)
	assert.Equal(t, expected, roots[0])
}

// TestDetermineGloasFullRoots_InBatch_Full verifies that a GLOAS block is identified as FULL
// when the next block's bid.ParentBlockHash matches this block's bid.BlockHash.
func TestDetermineGloasFullRoots_InBatch_Full(t *testing.T) {
	// blk0 is FULL: blk1.ParentBlockHash == blk0.BlockHash
	blk0 := makeGloasBlock(100, hash(0xAA), hash(0x00))
	blk1 := makeGloasBlock(101, hash(0xBB), hash(0xAA)) // ParentBlockHash = blk0.BlockHash
	responses := []*cltypes.SignedBeaconBlock{blk0, blk1}

	roots := determineGloasFullRoots(responses, nil)
	// blk0 is FULL, blk1 is highest with nil prevBatchTopBlock → optimistic
	require.Len(t, roots, 2)

	root0, err := blk0.Block.HashSSZ()
	require.NoError(t, err)
	root1, err := blk1.Block.HashSSZ()
	require.NoError(t, err)
	assert.Contains(t, roots, root0)
	assert.Contains(t, roots, root1)
}

// TestDetermineGloasFullRoots_InBatch_Empty verifies that a GLOAS block is identified as EMPTY
// when the next block's bid.ParentBlockHash does NOT match this block's bid.BlockHash.
func TestDetermineGloasFullRoots_InBatch_Empty(t *testing.T) {
	// blk0 is EMPTY: blk1.ParentBlockHash != blk0.BlockHash
	blk0 := makeGloasBlock(100, hash(0xAA), hash(0x00))
	blk1 := makeGloasBlock(101, hash(0xBB), hash(0xCC)) // ParentBlockHash != blk0.BlockHash
	responses := []*cltypes.SignedBeaconBlock{blk0, blk1}

	roots := determineGloasFullRoots(responses, nil)
	// blk0 is EMPTY, blk1 is highest with nil prevBatchTopBlock → optimistic
	require.Len(t, roots, 1)

	root1, err := blk1.Block.HashSSZ()
	require.NoError(t, err)
	assert.Equal(t, root1, roots[0])
}

// TestDetermineGloasFullRoots_CrossBatch_Full verifies the cross-batch lookahead:
// the highest block in a batch is confirmed FULL by prevBatchTopBlock.
func TestDetermineGloasFullRoots_CrossBatch_Full(t *testing.T) {
	blk := makeGloasBlock(100, hash(0xAA), hash(0x00))
	// prevBatchTopBlock is from the previous (higher-slot) batch; its ParentBlockHash = blk.BlockHash
	prevTop := makeGloasBlock(101, hash(0xBB), hash(0xAA))
	responses := []*cltypes.SignedBeaconBlock{blk}

	roots := determineGloasFullRoots(responses, prevTop)
	require.Len(t, roots, 1)

	expected, err := blk.Block.HashSSZ()
	require.NoError(t, err)
	assert.Equal(t, expected, roots[0])
}

// TestDetermineGloasFullRoots_CrossBatch_Empty verifies the cross-batch lookahead:
// the highest block is identified as EMPTY when prevBatchTopBlock doesn't reference it.
func TestDetermineGloasFullRoots_CrossBatch_Empty(t *testing.T) {
	blk := makeGloasBlock(100, hash(0xAA), hash(0x00))
	// prevBatchTopBlock's ParentBlockHash != blk.BlockHash → blk is EMPTY
	prevTop := makeGloasBlock(101, hash(0xBB), hash(0xCC))
	responses := []*cltypes.SignedBeaconBlock{blk}

	roots := determineGloasFullRoots(responses, prevTop)
	assert.Empty(t, roots)
}

// TestDetermineGloasFullRoots_Mixed verifies a batch with both FULL and EMPTY blocks.
func TestDetermineGloasFullRoots_Mixed(t *testing.T) {
	// Chain: blk0(FULL) → blk1(EMPTY) → blk2(FULL) → blk3(highest, optimistic)
	//   blk1.ParentBlockHash == blk0.BlockHash → blk0 FULL
	//   blk2.ParentBlockHash != blk1.BlockHash → blk1 EMPTY
	//   blk3.ParentBlockHash == blk2.BlockHash → blk2 FULL
	//   blk3 is highest, prevBatchTopBlock=nil → blk3 optimistic
	blk0 := makeGloasBlock(100, hash(0x10), hash(0x00))
	blk1 := makeGloasBlock(101, hash(0x20), hash(0x10)) // parent = blk0.hash → blk0 FULL
	blk2 := makeGloasBlock(102, hash(0x30), hash(0xFF)) // parent != blk1.hash → blk1 EMPTY
	blk3 := makeGloasBlock(103, hash(0x40), hash(0x30)) // parent = blk2.hash → blk2 FULL
	responses := []*cltypes.SignedBeaconBlock{blk0, blk1, blk2, blk3}

	roots := determineGloasFullRoots(responses, nil)
	require.Len(t, roots, 3) // blk0, blk2, blk3(optimistic)

	root0, _ := blk0.Block.HashSSZ()
	root1, _ := blk1.Block.HashSSZ()
	root2, _ := blk2.Block.HashSSZ()
	root3, _ := blk3.Block.HashSSZ()
	assert.Contains(t, roots, root0)
	assert.NotContains(t, roots, root1)
	assert.Contains(t, roots, root2)
	assert.Contains(t, roots, root3)
}

// TestDetermineGloasFullRoots_MixedVersions verifies that pre-GLOAS blocks in a mixed
// batch are ignored and GLOAS blocks are processed correctly.
func TestDetermineGloasFullRoots_MixedVersions(t *testing.T) {
	deneb := makeDenebBlock(99)
	gloasFull := makeGloasBlock(100, hash(0xAA), hash(0x00))
	// lookahead confirms gloasFull is FULL
	lookahead := makeGloasBlock(101, hash(0xBB), hash(0xAA))
	responses := []*cltypes.SignedBeaconBlock{deneb, gloasFull, lookahead}

	roots := determineGloasFullRoots(responses, nil)
	// gloasFull FULL, lookahead optimistic (highest with nil prevBatchTopBlock)
	require.Len(t, roots, 2)

	rootFull, _ := gloasFull.Block.HashSSZ()
	rootLookahead, _ := lookahead.Block.HashSSZ()
	assert.Contains(t, roots, rootFull)
	assert.Contains(t, roots, rootLookahead)
}

func TestBackwardBeaconDownloaderHTTPPreferredEmptyResponseFallsBack(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	downloader := &BackwardBeaconDownloader{
		reqInterval:     time.NewTicker(time.Hour),
		httpFallbackURL: server.URL,
		beaconCfg:       &clparams.MainnetBeaconConfig,
	}
	defer downloader.reqInterval.Stop()
	downloader.slotToDownload.Store(63)
	downloader.httpPreferred.Store(true)

	blocks, err := downloader.fetchBlockRange(ctx)
	if err == nil {
		t.Fatalf("fetchBlockRange returned nil error for %d blocks, want fallback instead of empty HTTP success", len(blocks))
	}
	if downloader.httpPreferred.Load() {
		t.Fatal("httpPreferred remained true after empty HTTP response")
	}
}

type emptyResponseBanRecordingSentinel struct {
	sentinelproto.SentinelClient
	banned chan string
}

func (s *emptyResponseBanRecordingSentinel) SendRequest(_ context.Context, _ *sentinelproto.RequestData, _ ...grpc.CallOption) (*sentinelproto.ResponseData, error) {
	return &sentinelproto.ResponseData{Peer: &sentinelproto.Peer{Pid: "empty-peer"}}, nil
}

func (s *emptyResponseBanRecordingSentinel) BanPeer(_ context.Context, peer *sentinelproto.Peer, _ ...grpc.CallOption) (*sentinelproto.EmptyMessage, error) {
	select {
	case s.banned <- peer.Pid:
	default:
	}
	return &sentinelproto.EmptyMessage{}, nil
}

func TestSendBlockRequestDoesNotBanOnEmptyResponse(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	sentinel := &emptyResponseBanRecordingSentinel{banned: make(chan string, 1)}
	rpcClient, cfg := newContextBlockingBeaconRPC(ctx, sentinel)
	b := NewBackwardBeaconDownloader(ctx, rpcClient, nil, nil, nil, cfg)

	var requestSent atomic.Bool
	requestSent.Store(true)
	received := make(chan []*cltypes.SignedBeaconBlock, 1)
	b.sendBlockRequest(ctx, 100, 10, common.Hash{}, received, received, &requestSent)

	select {
	case pid := <-sentinel.banned:
		t.Fatalf("peer %s banned for a protocol-legal empty response", pid)
	default:
	}
	assert.False(t, requestSent.Load(), "an empty response must release the in-flight flag so the request retries")
	assert.Empty(t, received, "an empty response must not be delivered as a batch")
}

// An empty BeaconBlocksByRange response is legal, so it is not a ban, but it is
// also not a reason to keep asking at the stage throttle forever.
func TestBackwardBeaconDownloaderEmptyResponseBackoff(t *testing.T) {
	const base = 600 * time.Millisecond
	for i, want := range []time.Duration{
		1200 * time.Millisecond,
		2400 * time.Millisecond,
		4800 * time.Millisecond,
		5 * time.Second,
		5 * time.Second,
	} {
		require.Equal(t, want, emptyResponseBackoff(base, uint32(i+1)))
	}
}

func TestBackwardBeaconDownloaderBackoffResetsOnBlocks(t *testing.T) {
	b := NewBackwardBeaconDownloader(t.Context(), nil, nil, nil, nil, &clparams.MainnetBeaconConfig)
	b.SetThrottle(600 * time.Millisecond)

	b.backOffEmpty()
	b.backOffEmpty()
	require.Equal(t, uint32(2), b.emptyResponses.Load())

	b.resetEmptyBackoff()
	require.Zero(t, b.emptyResponses.Load())
	require.Equal(t, 600*time.Millisecond, b.baseInterval)
}

func TestFetchBlockFromBeaconAPIByRootRejectsDifferentBlock(t *testing.T) {
	block := makeGloasBlock(10, hash(0xaa), common.Hash{})
	encoded, err := block.EncodeSSZ(nil)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Eth-Consensus-Version", "gloas")
		_, _ = w.Write(encoded)
	}))
	defer server.Close()

	otherRoot := common.HexToHash("0xdead")
	fetched, err := fetchBlockFromBeaconAPIByRoot(t.Context(), server.URL, otherRoot, gloasFromGenesisConfig())
	require.ErrorContains(t, err, "root")
	require.Nil(t, fetched)
}

func TestBackwardBeaconDownloaderFetchEnvelopeUsesRootAndRejectsIdentityMismatch(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	block := makeGloasBlock(10, hash(0xaa), common.Hash{})
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	envelope.Message.BeaconBlockRoot = common.HexToHash("0xdead")
	encoded, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)
	requestedPath := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedPath <- r.URL.Path
		w.Header().Set("Eth-Consensus-Version", "gloas")
		_, _ = w.Write(encoded)
	}))
	defer server.Close()
	downloader := &BackwardBeaconDownloader{httpFallbackURL: server.URL, beaconCfg: cfg}

	fetched, err := downloader.fetchSingleEnvelope(t.Context(), block)
	require.ErrorContains(t, err, "does not match requested block root")
	require.Nil(t, fetched)
	require.Equal(t, "/eth/v1/beacon/execution_payload_envelopes/"+common.Hash(blockRoot).Hex(), <-requestedPath)
}

func TestBackwardBeaconDownloaderFetchSingleEnvelopeFallsBackToP2P(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	block := makeGloasBlock(10, hash(0xaa), common.Hash{})
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	envelope.Message.BeaconBlockRoot = blockRoot
	server := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(server.Close)

	var requestedRoots [][32]byte
	downloader := &BackwardBeaconDownloader{
		httpFallbackURL: server.URL,
		beaconCfg:       cfg,
		requestEnvelopes: func(_ context.Context, roots [][32]byte, _ ...*cltypes.SignedBeaconBlock) (map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope, error) {
			requestedRoots = append(requestedRoots, roots...)
			return map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{blockRoot: envelope}, nil
		},
	}
	downloader.httpPreferred.Store(true)

	fetched, err := downloader.fetchSingleEnvelope(t.Context(), block)
	require.NoError(t, err)
	require.Same(t, envelope, fetched)
	require.Equal(t, [][32]byte{blockRoot}, requestedRoots)
}

func TestBackwardBeaconDownloaderFetchSingleEnvelopePrefersP2PAfterHTTPDemotion(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	block := makeGloasBlock(10, hash(0xaa), common.Hash{})
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	httpEnvelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	httpEnvelope.Message.BeaconBlockRoot = blockRoot
	httpEncoded, err := httpEnvelope.EncodeSSZ(nil)
	require.NoError(t, err)
	p2pEnvelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	p2pEnvelope.Message.BeaconBlockRoot = blockRoot

	var httpRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		httpRequests.Add(1)
		w.Header().Set("Eth-Consensus-Version", "gloas")
		_, _ = w.Write(httpEncoded)
	}))
	t.Cleanup(server.Close)

	downloader := &BackwardBeaconDownloader{
		httpFallbackURL: server.URL,
		beaconCfg:       cfg,
		requestEnvelopes: func(_ context.Context, _ [][32]byte, _ ...*cltypes.SignedBeaconBlock) (map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope, error) {
			return map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{blockRoot: p2pEnvelope}, nil
		},
	}
	downloader.httpPreferred.Store(false)

	fetched, err := downloader.fetchSingleEnvelope(t.Context(), block)
	require.NoError(t, err)
	require.Same(t, p2pEnvelope, fetched)
	require.Zero(t, httpRequests.Load())
}

func TestBackwardBeaconDownloaderFetchSingleEnvelopeRejectsInvalidP2PBeforeHTTP(t *testing.T) {
	testBackwardEnvelopeHTTPFallback(t, true)
}

func TestBackwardBeaconDownloaderFetchGloasEnvelopesRejectsInvalidP2PBeforeHTTP(t *testing.T) {
	testBackwardEnvelopeHTTPFallback(t, false)
}

func testBackwardEnvelopeHTTPFallback(t *testing.T, single bool) {
	t.Helper()
	previousExpiration := requestEnvelopeBatchExpiration
	requestEnvelopeBatchExpiration = 200 * time.Millisecond
	t.Cleanup(func() { requestEnvelopeBatchExpiration = previousExpiration })

	for _, invalidHTTP := range []bool{false, true} {
		t.Run(fmt.Sprintf("invalid_HTTP_signature=%t", invalidHTTP), func(t *testing.T) {
			cfg := gloasFromGenesisConfig()
			cfg.InitializeForkSchedule()
			anchor, block, envelope := backfilledGloasEnvelopeFixture(t, cfg, 10, clparams.BuilderIndexSelfBuild, 0, backfillProposerKey)
			blockRoot := envelope.Message.BeaconBlockRoot
			if invalidHTTP {
				signBackfilledGloasEnvelope(t, cfg, anchor, envelope, backfillOtherKey)
			}
			httpEncoded, err := envelope.EncodeSSZ(nil)
			require.NoError(t, err)
			envelope.Message.BuilderIndex++
			clock := eth_clock.NewEthereumClock(0, common.Hash{}, cfg)
			digest, err := clock.ComputeForkDigest(0)
			require.NoError(t, err)
			var wire bytes.Buffer
			require.NoError(t, ssz_snappy.EncodeAndWrite(&wire, envelope, digest[:]...))
			sentinel := &backfillEnvelopeSentinel{
				invalidResponse: wire.Bytes(),
				invalidProtocol: communication.ExecutionPayloadEnvelopesByRootProtocolV1,
			}
			client := rpc.NewBeaconRpcP2P(t.Context(), sentinel, cfg, clock)

			var httpRequests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				httpRequests.Add(1)
				assert.Equal(t, "/eth/v1/beacon/execution_payload_envelopes/"+blockRoot.Hex(), r.URL.Path)
				w.Header().Set("Eth-Consensus-Version", "gloas")
				_, _ = w.Write(httpEncoded)
			}))
			t.Cleanup(server.Close)

			downloader := NewBackwardBeaconDownloader(t.Context(), client, nil, nil, nil, cfg)
			t.Cleanup(downloader.reqInterval.Stop)
			downloader.SetGloasEnvelopeSignatureValidator(anchor)
			downloader.SetHTTPFallbackURL(server.URL)
			child := makeGloasBlock(11, hash(0xbb), envelope.Message.Payload.BlockHash)
			linkBeaconBlocks(t, block, child)
			downloader.prevBatchTopBlock = child

			var fetched *cltypes.SignedExecutionPayloadEnvelope
			if single {
				fetched, err = downloader.fetchSingleEnvelope(t.Context(), block)
				if invalidHTTP {
					require.ErrorContains(t, err, "invalid signature")
				} else {
					require.NoError(t, err)
				}
			} else {
				envelopes, fullRoots := downloader.fetchGloasEnvelopes(t.Context(), []*cltypes.SignedBeaconBlock{block})
				require.Contains(t, fullRoots, blockRoot)
				fetched = envelopes[blockRoot]
			}
			if invalidHTTP {
				require.Nil(t, fetched)
			} else {
				require.NotNil(t, fetched)
				require.NoError(t, downloader.validateGloasEnvelope(block, fetched))
			}
			require.Equal(t, []string{"invalid-envelope-peer"}, sentinel.banned)
			require.Equal(t, int32(1), httpRequests.Load())
		})
	}
}

func TestBackwardBeaconDownloaderHTTPFallbackValidatesOnlyMissingEnvelopes(t *testing.T) {
	for _, invalidHTTP := range []bool{false, true} {
		t.Run(fmt.Sprintf("invalid_HTTP=%t", invalidHTTP), func(t *testing.T) {
			cfg := gloasFromGenesisConfig()
			p2pBlock := makeGloasBlock(10, hash(0xaa), common.Hash{})
			httpBlock := makeGloasBlock(11, hash(0xbb), hash(0xaa))
			child := makeGloasBlock(12, hash(0xcc), hash(0xbb))
			linkBeaconBlocks(t, p2pBlock, httpBlock, child)
			p2pRoot, err := p2pBlock.Block.HashSSZ()
			require.NoError(t, err)
			httpRoot, err := httpBlock.Block.HashSSZ()
			require.NoError(t, err)
			p2pEnvelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
			p2pEnvelope.Message.BeaconBlockRoot = p2pRoot
			httpEnvelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
			httpEnvelope.Message.BeaconBlockRoot = httpRoot
			encoded, err := httpEnvelope.EncodeSSZ(nil)
			require.NoError(t, err)
			var httpRequests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				httpRequests.Add(1)
				assert.Equal(t, "/eth/v1/beacon/execution_payload_envelopes/"+common.Hash(httpRoot).Hex(), r.URL.Path)
				w.Header().Set("Eth-Consensus-Version", "gloas")
				_, _ = w.Write(encoded)
			}))
			t.Cleanup(server.Close)
			validations := make(map[common.Hash]int)
			downloader := &BackwardBeaconDownloader{
				beaconCfg:         cfg,
				httpFallbackURL:   server.URL,
				prevBatchTopBlock: child,
				requestEnvelopes: func(context.Context, [][32]byte, ...*cltypes.SignedBeaconBlock) (map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope, error) {
					return map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{p2pRoot: p2pEnvelope}, nil
				},
				validateGloasEnvelope: func(_ *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) error {
					validations[envelope.Message.BeaconBlockRoot]++
					if invalidHTTP && envelope.Message.BeaconBlockRoot == httpRoot {
						return errors.New("invalid HTTP envelope")
					}
					return nil
				},
			}

			envelopes, _ := downloader.fetchGloasEnvelopes(t.Context(), []*cltypes.SignedBeaconBlock{p2pBlock, httpBlock})
			require.Same(t, p2pEnvelope, envelopes[p2pRoot])
			if invalidHTTP {
				require.NotContains(t, envelopes, common.Hash(httpRoot))
			} else {
				require.Contains(t, envelopes, common.Hash(httpRoot))
			}
			require.Equal(t, map[common.Hash]int{httpRoot: 1}, validations)
			require.Equal(t, int32(1), httpRequests.Load())
		})
	}
}

func TestBackwardBeaconDownloaderFetchSingleEnvelopeDoesNotRetryP2PTwice(t *testing.T) {
	block := makeGloasBlock(10, hash(0xaa), common.Hash{})
	server := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(server.Close)

	var p2pRequests atomic.Int32
	downloader := &BackwardBeaconDownloader{
		httpFallbackURL: server.URL,
		beaconCfg:       gloasFromGenesisConfig(),
		requestEnvelopes: func(_ context.Context, _ [][32]byte, _ ...*cltypes.SignedBeaconBlock) (map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope, error) {
			p2pRequests.Add(1)
			return nil, nil
		},
	}
	downloader.httpPreferred.Store(false)

	_, err := downloader.fetchSingleEnvelope(t.Context(), block)
	require.Error(t, err)
	require.Equal(t, int32(1), p2pRequests.Load())
}

func TestBackwardBeaconDownloaderRequestMoreDoesNotFinishWhenMatchedCallbackFails(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	child := makeGloasBlock(11, hash(0xbb), hash(0xaa))
	linkBeaconBlocks(t, target, child)
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)
	encodedBlock, err := target.EncodeSSZ(nil)
	require.NoError(t, err)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	envelope.Message.BeaconBlockRoot = targetRoot
	encodedEnvelope, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)
	var httpRequests atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpRequests.Add(1)
		switch {
		case r.URL.Path == "/eth/v2/beacon/blocks/10":
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(encodedBlock)
		case strings.HasPrefix(r.URL.Path, "/eth/v1/beacon/execution_payload_envelopes/"):
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(encodedEnvelope)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	downloader := &BackwardBeaconDownloader{
		expectedRoot:      targetRoot,
		httpFallbackURL:   server.URL,
		beaconCfg:         cfg,
		neverSkip:         false,
		reqInterval:       time.NewTicker(time.Hour),
		prevBatchTopBlock: child,
	}
	defer downloader.reqInterval.Stop()
	downloader.httpPreferred.Store(true)
	downloader.slotToDownload.Store(target.Block.Slot)
	sentinelErr := errors.New("callback failed after deciding to finish")
	failCallback := true
	callbackAttempts := 0
	downloader.SetOnNewBlock(func(_ *cltypes.SignedBeaconBlock, _ *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		callbackAttempts++
		if failCallback {
			return true, sentinelErr
		}
		return true, nil
	})

	require.NoError(t, downloader.RequestMore(t.Context()))
	require.False(t, downloader.Finished())
	require.Equal(t, common.Hash(targetRoot), downloader.expectedRoot)
	require.Equal(t, target.Block.Slot, downloader.Progress())
	require.Equal(t, 1, callbackAttempts)
	require.False(t, downloader.httpPreferred.Load())
	requestsBeforeRetry := httpRequests.Load()
	retryCtx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, downloader.RequestMore(retryCtx), context.DeadlineExceeded)
	require.Equal(t, requestsBeforeRetry, httpRequests.Load())
	require.Equal(t, 1, callbackAttempts)

	failCallback = false
	downloader.httpPreferred.Store(true)
	require.NoError(t, downloader.RequestMore(t.Context()))
	require.Equal(t, 2, callbackAttempts)
	require.True(t, downloader.Finished())
	require.Equal(t, target.Block.ParentRoot, downloader.expectedRoot)
	require.Equal(t, target.Block.Slot-1, downloader.Progress())
}

func TestBackwardBeaconDownloaderRequestMoreDoesNotFinishWhenRootFallbackCallbackFails(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	child := makeGloasBlock(11, hash(0xbb), hash(0xaa))
	linkBeaconBlocks(t, target, child)
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)
	wrong := makeGloasBlock(10, hash(0xbb), common.Hash{0x99})
	wrongRoot, err := wrong.Block.HashSSZ()
	require.NoError(t, err)
	encodedTarget, err := target.EncodeSSZ(nil)
	require.NoError(t, err)
	encodedWrong, err := wrong.EncodeSSZ(nil)
	require.NoError(t, err)
	encodedEnvelopes := make(map[string][]byte)
	for _, root := range []common.Hash{targetRoot, wrongRoot} {
		envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
		envelope.Message.BeaconBlockRoot = root
		encoded, encodeErr := envelope.EncodeSSZ(nil)
		require.NoError(t, encodeErr)
		encodedEnvelopes[root.Hex()] = encoded
	}
	var httpRequests atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpRequests.Add(1)
		switch {
		case r.URL.Path == "/eth/v2/beacon/blocks/10":
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(encodedWrong)
		case r.URL.Path == "/eth/v2/beacon/blocks/"+common.Hash(targetRoot).Hex():
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(encodedTarget)
		case strings.HasPrefix(r.URL.Path, "/eth/v1/beacon/execution_payload_envelopes/"):
			root := strings.TrimPrefix(r.URL.Path, "/eth/v1/beacon/execution_payload_envelopes/")
			encoded, ok := encodedEnvelopes[root]
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(encoded)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	downloader := &BackwardBeaconDownloader{
		expectedRoot:      targetRoot,
		httpFallbackURL:   server.URL,
		beaconCfg:         cfg,
		neverSkip:         false,
		reqInterval:       time.NewTicker(time.Hour),
		prevBatchTopBlock: child,
	}
	defer downloader.reqInterval.Stop()
	downloader.httpPreferred.Store(true)
	downloader.slotToDownload.Store(target.Block.Slot)
	sentinelErr := errors.New("root callback failed after deciding to finish")
	failCallback := true
	callbackAttempts := 0
	downloader.SetOnNewBlock(func(_ *cltypes.SignedBeaconBlock, _ *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		callbackAttempts++
		if failCallback {
			return true, sentinelErr
		}
		return true, nil
	})

	require.NoError(t, downloader.RequestMore(t.Context()))
	require.False(t, downloader.Finished())
	require.Equal(t, common.Hash(targetRoot), downloader.expectedRoot)
	require.Equal(t, target.Block.Slot, downloader.Progress())
	require.Equal(t, 1, callbackAttempts)
	require.Zero(t, downloader.Processed())
	require.False(t, downloader.httpPreferred.Load())
	requestsBeforeRetry := httpRequests.Load()
	retryCtx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, downloader.RequestMore(retryCtx), context.DeadlineExceeded)
	require.Equal(t, requestsBeforeRetry, httpRequests.Load())
	require.Equal(t, 1, callbackAttempts)

	failCallback = false
	downloader.httpPreferred.Store(true)
	require.NoError(t, downloader.RequestMore(t.Context()))
	require.Equal(t, 2, callbackAttempts)
	require.True(t, downloader.Finished())
	require.Equal(t, target.Block.ParentRoot, downloader.expectedRoot)
	require.Equal(t, target.Block.Slot-1, downloader.Progress())
	require.Equal(t, uint64(1), downloader.Processed())
}

func TestBackwardBeaconDownloaderRequestMoreRetainsLookaheadAfterPartialCallbackFailure(t *testing.T) {
	for _, full := range []bool{false, true} {
		name := "empty"
		if full {
			name = "full"
		}
		t.Run(name, func(t *testing.T) {
			cfg := gloasFromGenesisConfig()
			lower := makeGloasBlock(100, hash(0xaa), common.Hash{0x42})
			parentBlockHash := hash(0xcc)
			if full {
				parentBlockHash = hash(0xaa)
			}
			higher := makeGloasBlock(101, hash(0xbb), parentBlockHash)
			linkBeaconBlocks(t, lower, higher)
			batchSuccessor := makeGloasBlock(102, hash(0xdd), hash(0xbb))
			linkBeaconBlocks(t, higher, batchSuccessor)
			lowerRoot, err := lower.Block.HashSSZ()
			require.NoError(t, err)
			higherRoot, err := higher.Block.HashSSZ()
			require.NoError(t, err)
			encodedLower, err := lower.EncodeSSZ(nil)
			require.NoError(t, err)
			encodedHigher, err := higher.EncodeSSZ(nil)
			require.NoError(t, err)
			encodedEnvelopes := make(map[string][]byte)
			for _, root := range []common.Hash{higherRoot, lowerRoot} {
				envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
				envelope.Message.BeaconBlockRoot = root
				encoded, encodeErr := envelope.EncodeSSZ(nil)
				require.NoError(t, encodeErr)
				encodedEnvelopes[root.Hex()] = encoded
			}

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/eth/v2/beacon/blocks/100":
					w.Header().Set("Eth-Consensus-Version", "gloas")
					_, _ = w.Write(encodedLower)
				case "/eth/v2/beacon/blocks/101":
					w.Header().Set("Eth-Consensus-Version", "gloas")
					_, _ = w.Write(encodedHigher)
				default:
					prefix := "/eth/v1/beacon/execution_payload_envelopes/"
					if !strings.HasPrefix(r.URL.Path, prefix) {
						http.NotFound(w, r)
						return
					}
					root := strings.TrimPrefix(r.URL.Path, prefix)
					if !full && root == common.Hash(lowerRoot).Hex() {
						http.NotFound(w, r)
						return
					}
					encoded, ok := encodedEnvelopes[root]
					if !ok {
						http.NotFound(w, r)
						return
					}
					w.Header().Set("Eth-Consensus-Version", "gloas")
					_, _ = w.Write(encoded)
				}
			}))
			defer server.Close()

			downloader := &BackwardBeaconDownloader{
				expectedRoot:      higherRoot,
				httpFallbackURL:   server.URL,
				beaconCfg:         cfg,
				neverSkip:         false,
				reqInterval:       time.NewTicker(time.Hour),
				prevBatchTopBlock: batchSuccessor,
			}
			defer downloader.reqInterval.Stop()
			downloader.httpPreferred.Store(true)
			downloader.slotToDownload.Store(higher.Block.Slot)
			lowerAttempts := 0
			downloader.SetOnNewBlock(func(block *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
				switch block.Block.Slot {
				case higher.Block.Slot:
					require.NotNil(t, envelope)
					return false, nil
				case lower.Block.Slot:
					lowerAttempts++
					if full {
						require.NotNil(t, envelope)
					} else {
						require.Nil(t, envelope)
					}
					if lowerAttempts == 1 {
						return true, errors.New("lower callback failed")
					}
					return true, nil
				default:
					t.Fatalf("unexpected block slot %d", block.Block.Slot)
					return false, nil
				}
			})

			require.NoError(t, downloader.RequestMore(t.Context()))
			require.False(t, downloader.Finished())
			require.Equal(t, common.Hash(lowerRoot), downloader.expectedRoot)
			require.Equal(t, lower.Block.Slot, downloader.Progress())
			require.NotNil(t, downloader.prevBatchTopBlock)
			retainedRoot, err := downloader.prevBatchTopBlock.Block.HashSSZ()
			require.NoError(t, err)
			require.Equal(t, higherRoot, retainedRoot)

			downloader.httpPreferred.Store(true)
			require.NoError(t, downloader.RequestMore(t.Context()))
			require.Equal(t, 2, lowerAttempts)
			require.True(t, downloader.Finished())
			require.Equal(t, lower.Block.ParentRoot, downloader.expectedRoot)
			require.Equal(t, lower.Block.Slot-1, downloader.Progress())
		})
	}
}

func TestBackwardBeaconDownloaderRootFallbackAdvancesOnlyForEnvelope(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	child := makeGloasBlock(11, hash(0xbb), hash(0xaa))
	linkBeaconBlocks(t, target, child)
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)
	encodedBlock, err := target.EncodeSSZ(nil)
	require.NoError(t, err)
	wrongEnvelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	wrongEnvelope.Message.BeaconBlockRoot = common.HexToHash("0xdead")
	encodedWrongEnvelope, err := wrongEnvelope.EncodeSSZ(nil)
	require.NoError(t, err)

	tests := []struct {
		name             string
		writeEnvelope    func(http.ResponseWriter)
		retryWithoutHTTP bool
	}{
		{
			name:             "not found is not an empty proof",
			retryWithoutHTTP: true,
			writeEnvelope: func(w http.ResponseWriter) {
				http.NotFound(w, nil)
			},
		},
		{
			name: "server error",
			writeEnvelope: func(w http.ResponseWriter) {
				w.WriteHeader(http.StatusInternalServerError)
			},
		},
		{
			name: "malformed body",
			writeEnvelope: func(w http.ResponseWriter) {
				w.Header().Set("Eth-Consensus-Version", "gloas")
				_, _ = w.Write([]byte{1, 2, 3})
			},
		},
		{
			name: "wrong version",
			writeEnvelope: func(w http.ResponseWriter) {
				w.Header().Set("Eth-Consensus-Version", "fulu")
				_, _ = w.Write(encodedWrongEnvelope)
			},
		},
		{
			name: "wrong root",
			writeEnvelope: func(w http.ResponseWriter) {
				w.Header().Set("Eth-Consensus-Version", "gloas")
				_, _ = w.Write(encodedWrongEnvelope)
			},
		},
		{
			name: "oversized body",
			writeEnvelope: func(w http.ResponseWriter) {
				w.Header().Set("Eth-Consensus-Version", "gloas")
				_, _ = w.Write(bytes.Repeat([]byte{'x'}, int(clparams.MaxChunkSize)+1))
			},
		},
		{
			name: "transport failure",
			writeEnvelope: func(http.ResponseWriter) {
				panic(http.ErrAbortHandler)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var httpRequests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				httpRequests.Add(1)
				switch {
				case strings.HasPrefix(r.URL.Path, "/eth/v2/beacon/blocks/"):
					w.Header().Set("Eth-Consensus-Version", "gloas")
					_, _ = w.Write(encodedBlock)
				case strings.HasPrefix(r.URL.Path, "/eth/v1/beacon/execution_payload_envelopes/"):
					tt.writeEnvelope(w)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()

			downloader := &BackwardBeaconDownloader{
				expectedRoot:      targetRoot,
				httpFallbackURL:   server.URL,
				beaconCfg:         cfg,
				reqInterval:       time.NewTicker(time.Hour),
				prevBatchTopBlock: child,
			}
			defer downloader.reqInterval.Stop()
			downloader.httpPreferred.Store(true)
			downloader.slotToDownload.Store(target.Block.Slot)
			processed := 0
			downloader.SetOnNewBlock(func(_ *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
				processed++
				require.Nil(t, envelope)
				return false, nil
			})

			require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{makeDenebBlock(11)}))
			require.Zero(t, processed)
			require.Equal(t, common.Hash(targetRoot), downloader.expectedRoot)
			require.Equal(t, target.Block.Slot, downloader.Progress())
			require.False(t, downloader.httpPreferred.Load())
			if tt.retryWithoutHTTP {
				requestsBeforeRetry := httpRequests.Load()
				retryCtx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
				defer cancel()
				require.ErrorIs(t, downloader.RequestMore(retryCtx), context.DeadlineExceeded)
				require.Equal(t, requestsBeforeRetry, httpRequests.Load())
			}
		})
	}
}

func TestBackwardBeaconDownloaderRootFallbackFailsAfterEnvelopeRetryLimit(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	child := makeGloasBlock(11, hash(0xbb), hash(0xaa))
	linkBeaconBlocks(t, target, child)
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)
	encodedBlock, err := target.EncodeSSZ(nil)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/eth/v2/beacon/blocks/") {
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(encodedBlock)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(server.Close)

	downloader := &BackwardBeaconDownloader{
		expectedRoot:      targetRoot,
		httpFallbackURL:   server.URL,
		beaconCfg:         cfg,
		prevBatchTopBlock: child,
	}
	downloader.slotToDownload.Store(target.Block.Slot)
	downloader.SetOnNewBlock(func(*cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		t.Fatal("block must not advance without its required envelope")
		return false, nil
	})

	for range 2 {
		require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{makeDenebBlock(11)}))
	}
	require.ErrorContains(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{makeDenebBlock(11)}), "required GLOAS envelope unavailable")
	require.Equal(t, maxConsecutiveEnvelopeFailures, downloader.consecutiveEnvelopeFailures)
	require.Equal(t, common.Hash(targetRoot), downloader.expectedRoot)
	require.Equal(t, target.Block.Slot, downloader.Progress())
}

func TestBackwardBeaconDownloaderSetExpectedRootResetsEnvelopeFailureRun(t *testing.T) {
	downloader := &BackwardBeaconDownloader{
		expectedRoot:                common.Hash{1},
		consecutiveEnvelopeFailures: maxConsecutiveEnvelopeFailures - 1,
	}

	downloader.SetExpectedRoot(common.Hash{2})

	require.Zero(t, downloader.consecutiveEnvelopeFailures)
}

func TestBackwardBeaconDownloaderMissingRequiredEnvelopeSurvivesRestart(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	lookahead := makeGloasBlock(11, hash(0xbb), hash(0xaa))
	linkBeaconBlocks(t, target, lookahead)
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)
	lookaheadRoot, err := lookahead.Block.HashSSZ()
	require.NoError(t, err)
	targetEnvelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	targetEnvelope.Message.BeaconBlockRoot = targetRoot
	encodedTargetEnvelope, err := targetEnvelope.EncodeSSZ(nil)
	require.NoError(t, err)
	lookaheadEnvelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	lookaheadEnvelope.Message.BeaconBlockRoot = lookaheadRoot
	encodedLookaheadEnvelope, err := lookaheadEnvelope.EncodeSSZ(nil)
	require.NoError(t, err)

	recovered := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Eth-Consensus-Version", "gloas")
		switch {
		case strings.HasSuffix(r.URL.Path, common.Hash(lookaheadRoot).Hex()):
			_, _ = w.Write(encodedLookaheadEnvelope)
		case strings.HasSuffix(r.URL.Path, common.Hash(targetRoot).Hex()) && recovered:
			_, _ = w.Write(encodedTargetEnvelope)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	downloader := &BackwardBeaconDownloader{
		expectedRoot:      targetRoot,
		prevBatchTopBlock: lookahead,
		httpFallbackURL:   server.URL,
		beaconCfg:         cfg,
	}
	downloader.httpPreferred.Store(true)
	downloader.slotToDownload.Store(target.Block.Slot)
	processed := 0
	downloader.SetOnNewBlock(func(_ *cltypes.SignedBeaconBlock, _ *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		return false, nil
	})

	for range 2 {
		downloader.httpPreferred.Store(true)
		require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{target, lookahead}))
	}
	downloader.httpPreferred.Store(true)
	require.ErrorContains(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{target, lookahead}), "required GLOAS envelope unavailable")
	require.Equal(t, 3, downloader.consecutiveEnvelopeFailures)
	require.Zero(t, processed)
	require.False(t, downloader.Finished())
	require.Equal(t, common.Hash(targetRoot), downloader.expectedRoot)
	require.Equal(t, target.Block.Slot, downloader.Progress())
	recovered = true
	restarted := &BackwardBeaconDownloader{
		expectedRoot:                targetRoot,
		prevBatchTopBlock:           lookahead,
		httpFallbackURL:             server.URL,
		beaconCfg:                   cfg,
		consecutiveEnvelopeFailures: 2,
	}
	restarted.httpPreferred.Store(true)
	restarted.slotToDownload.Store(target.Block.Slot)
	restartedProcessed := 0
	restarted.SetOnNewBlock(func(_ *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		restartedProcessed++
		require.Equal(t, targetEnvelope.Message.BeaconBlockRoot, envelope.Message.BeaconBlockRoot)
		return true, nil
	})

	require.NoError(t, restarted.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{target, lookahead}))
	require.Zero(t, restarted.consecutiveEnvelopeFailures)
	require.Equal(t, 1, restartedProcessed)
	require.True(t, restarted.Finished())
	require.Equal(t, target.Block.ParentRoot, restarted.expectedRoot)
}

func TestBackwardBeaconDownloaderInitial404RetainsUnresolvedBlock(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()
	downloader := &BackwardBeaconDownloader{
		expectedRoot:    targetRoot,
		httpFallbackURL: server.URL,
		beaconCfg:       cfg,
	}
	downloader.httpPreferred.Store(true)
	downloader.slotToDownload.Store(target.Block.Slot)
	processed := 0
	downloader.SetOnNewBlock(func(_ *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		require.Nil(t, envelope)
		return false, nil
	})

	require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{target}))
	require.Zero(t, processed)
	require.Equal(t, common.Hash(targetRoot), downloader.expectedRoot)
	require.Equal(t, target.Block.Slot, downloader.Progress())
}

func TestBackwardBeaconDownloaderInitialCheckpointUsesFetchedSuccessor(t *testing.T) {
	for _, full := range []bool{false, true} {
		name := "empty"
		if full {
			name = "full"
		}
		t.Run(name, func(t *testing.T) {
			cfg := gloasFromGenesisConfig()
			target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
			parentBlockHash := hash(0xcc)
			if full {
				parentBlockHash = hash(0xaa)
			}
			successor := makeGloasBlock(12, hash(0xbb), parentBlockHash)
			linkBeaconBlocks(t, target, successor)
			targetRoot, err := target.Block.HashSSZ()
			require.NoError(t, err)
			successorEncoded, err := successor.EncodeSSZ(nil)
			require.NoError(t, err)
			envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
			envelope.Message.BeaconBlockRoot = targetRoot
			envelopeEncoded, err := envelope.EncodeSSZ(nil)
			require.NoError(t, err)

			var successorRequests atomic.Int32
			var envelopeRequests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/eth/v2/beacon/blocks/11":
					successorRequests.Add(1)
					http.NotFound(w, r)
				case r.URL.Path == "/eth/v2/beacon/blocks/12":
					successorRequests.Add(1)
					w.Header().Set("Eth-Consensus-Version", "gloas")
					_, _ = w.Write(successorEncoded)
				case strings.HasSuffix(r.URL.Path, common.Hash(targetRoot).Hex()):
					envelopeRequests.Add(1)
					if !full {
						http.NotFound(w, r)
						return
					}
					w.Header().Set("Eth-Consensus-Version", "gloas")
					_, _ = w.Write(envelopeEncoded)
				default:
					http.NotFound(w, r)
				}
			}))
			t.Cleanup(server.Close)

			downloader := &BackwardBeaconDownloader{
				expectedRoot:           targetRoot,
				httpFallbackURL:        server.URL,
				beaconCfg:              cfg,
				validateGloasSuccessor: acceptGloasSuccessor,
			}
			downloader.SetCurrentSlotSampler(func() uint64 { return 13 })
			downloader.httpPreferred.Store(true)
			downloader.slotToDownload.Store(target.Block.Slot)
			processed := 0
			downloader.SetOnNewBlock(func(_ *cltypes.SignedBeaconBlock, got *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
				processed++
				if full {
					require.NotNil(t, got)
				} else {
					require.Nil(t, got)
				}
				return true, nil
			})

			require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{target}))
			require.Equal(t, int32(2), successorRequests.Load())
			expectedEnvelopeRequests := int32(0)
			if full {
				expectedEnvelopeRequests = 1
			}
			require.Equal(t, expectedEnvelopeRequests, envelopeRequests.Load())
			require.Equal(t, 1, processed)
			require.True(t, downloader.Finished())
			require.Equal(t, target.Block.ParentRoot, downloader.expectedRoot)
		})
	}
}

func TestBackwardBeaconDownloaderRejectsLostCanonicalSuccessor(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	target := makeGloasBlock(10, hash(0xaa), hash(0x42))
	successor := makeGloasBlock(11, hash(0xbb), hash(0xcc))
	linkBeaconBlocks(t, target, successor)
	staleSuccessor := makeGloasBlock(12, hash(0xdd), hash(0xee))
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)
	successorEncoded, err := successor.EncodeSSZ(nil)
	require.NoError(t, err)
	var successorRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		successorRequests.Add(1)
		w.Header().Set("Eth-Consensus-Version", "gloas")
		_, _ = w.Write(successorEncoded)
	}))
	t.Cleanup(server.Close)

	downloader := &BackwardBeaconDownloader{
		expectedRoot:           targetRoot,
		prevBatchTopBlock:      staleSuccessor,
		httpFallbackURL:        server.URL,
		beaconCfg:              cfg,
		validateGloasSuccessor: acceptGloasSuccessor,
	}
	downloader.SetCurrentSlotSampler(func() uint64 { return 12 })
	downloader.SetOnNewBlock(func(*cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		return true, nil
	})

	err = downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{target})

	require.ErrorIs(t, err, errInvalidCanonicalGloasSuccessor)
	require.Zero(t, successorRequests.Load())
}

func TestBackwardBeaconDownloaderRejectsUnvalidatedInitialSuccessor(t *testing.T) {
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	successor := makeGloasBlock(12, hash(0xbb), hash(0xcc))
	linkBeaconBlocks(t, target, successor)
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)

	downloader := &BackwardBeaconDownloader{
		expectedRoot: targetRoot,
		beaconCfg:    gloasFromGenesisConfig(),
		currentSlot:  func() uint64 { return 13 },
		requestBlocksByRange: func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			return []*cltypes.SignedBeaconBlock{successor}, "untrusted-peer", nil
		},
	}
	downloader.slotToDownload.Store(target.Block.Slot)
	processed := 0
	downloader.SetOnNewBlock(func(_ *cltypes.SignedBeaconBlock, _ *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		return true, nil
	})

	require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{target}))
	require.Zero(t, processed)
	require.Equal(t, common.Hash(targetRoot), downloader.expectedRoot)
}

func TestBackwardBeaconDownloaderDoesNotUseP2PInitialSuccessorWithoutCanonicalSource(t *testing.T) {
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	successor := makeGloasBlock(12, hash(0xbb), hash(0xcc))
	linkBeaconBlocks(t, target, successor)
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)

	var requests atomic.Int32
	var validations atomic.Int32
	downloader := &BackwardBeaconDownloader{
		expectedRoot: targetRoot,
		beaconCfg:    gloasFromGenesisConfig(),
		currentSlot:  func() uint64 { return 13 },
		requestBlocksByRange: func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			requests.Add(1)
			return []*cltypes.SignedBeaconBlock{successor}, "peer", nil
		},
		validateGloasSuccessor: func(block *cltypes.SignedBeaconBlock) error {
			validations.Add(1)
			require.Same(t, successor, block)
			return nil
		},
	}
	downloader.slotToDownload.Store(target.Block.Slot)
	processed := 0
	downloader.SetOnNewBlock(func(_ *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		require.Nil(t, envelope)
		return true, nil
	})

	require.ErrorContains(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{target}), "canonical GLOAS successor source")
	require.Zero(t, requests.Load())
	require.Zero(t, validations.Load())
	require.Zero(t, processed)
	require.Equal(t, common.Hash(targetRoot), downloader.expectedRoot)
}

func TestBackwardBeaconDownloaderRequestMoreFailsWithoutCanonicalSuccessorSource(t *testing.T) {
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)

	downloader := &BackwardBeaconDownloader{
		expectedRoot: targetRoot,
		beaconCfg:    gloasFromGenesisConfig(),
		currentSlot:  func() uint64 { return 13 },
		neverSkip:    false,
		reqInterval:  time.NewTicker(time.Nanosecond),
		requestBlocksByRange: func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			return []*cltypes.SignedBeaconBlock{target}, "peer", nil
		},
		validateGloasSuccessor: acceptGloasSuccessor,
	}
	t.Cleanup(downloader.reqInterval.Stop)
	downloader.slotToDownload.Store(target.Block.Slot)

	err = downloader.RequestMore(t.Context())
	require.ErrorContains(t, err, "canonical GLOAS successor source")
	require.Equal(t, common.Hash(targetRoot), downloader.expectedRoot)
	require.Equal(t, target.Block.Slot, downloader.Progress())
}

func TestBackwardBeaconDownloaderRequestMoreUsesStoredGloasBlockWithoutCanonicalSuccessorSource(t *testing.T) {
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	t.Cleanup(db.Close)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		if err := beacon_indicies.WriteBeaconBlockAndIndicies(tx, target, false); err != nil {
			return err
		}
		return beacon_indicies.WriteExecutionBlockHash(tx, targetRoot, hash(0xaa))
	}))

	downloader := &BackwardBeaconDownloader{
		ctx:          t.Context(),
		expectedRoot: targetRoot,
		beaconCfg:    gloasFromGenesisConfig(),
		currentSlot:  func() uint64 { return 13 },
		db:           db,
		neverSkip:    true,
		reqInterval:  time.NewTicker(time.Nanosecond),
		requestBlocksByRange: func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			return []*cltypes.SignedBeaconBlock{target}, "peer", nil
		},
		validateGloasSuccessor: acceptGloasSuccessor,
		blockReader: beaconBlockBodyReaderFunc(func(_ context.Context, _ kv.Tx, root common.Hash) (*cltypes.SignedBeaconBlock, error) {
			if root == targetRoot {
				return target, nil
			}
			return nil, nil
		}),
	}
	t.Cleanup(downloader.reqInterval.Stop)
	downloader.slotToDownload.Store(target.Block.Slot)

	require.NoError(t, downloader.RequestMore(t.Context()))
	require.Equal(t, target.Block.ParentRoot, downloader.expectedRoot)
	require.Equal(t, target.Block.Slot-1, downloader.Progress())
}

func TestBackwardBeaconDownloaderTrustedHTTPSuccessorWinsOverValidatedP2P(t *testing.T) {
	for _, httpFull := range []bool{false, true} {
		name := "http_empty_p2p_full"
		if httpFull {
			name = "http_full_p2p_empty"
		}
		t.Run(name, func(t *testing.T) {
			cfg := gloasFromGenesisConfig()
			target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
			httpParentBlockHash := hash(0xcc)
			p2pParentBlockHash := hash(0xaa)
			if httpFull {
				httpParentBlockHash, p2pParentBlockHash = p2pParentBlockHash, httpParentBlockHash
			}
			httpSuccessor := makeGloasBlock(12, hash(0xbb), httpParentBlockHash)
			p2pSuccessor := makeGloasBlock(12, hash(0xdd), p2pParentBlockHash)
			linkBeaconBlocks(t, target, httpSuccessor)
			linkBeaconBlocks(t, target, p2pSuccessor)
			targetRoot, err := target.Block.HashSSZ()
			require.NoError(t, err)
			httpSuccessorEncoded, err := httpSuccessor.EncodeSSZ(nil)
			require.NoError(t, err)
			envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
			envelope.Message.BeaconBlockRoot = targetRoot
			envelopeEncoded, err := envelope.EncodeSSZ(nil)
			require.NoError(t, err)

			var p2pRequests atomic.Int32
			var envelopeRequests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == "/eth/v2/beacon/blocks/12":
					w.Header().Set("Eth-Consensus-Version", "gloas")
					_, _ = w.Write(httpSuccessorEncoded)
				case strings.HasSuffix(r.URL.Path, common.Hash(targetRoot).Hex()):
					envelopeRequests.Add(1)
					w.Header().Set("Eth-Consensus-Version", "gloas")
					_, _ = w.Write(envelopeEncoded)
				default:
					http.NotFound(w, r)
				}
			}))
			t.Cleanup(server.Close)

			downloader := &BackwardBeaconDownloader{
				expectedRoot:    targetRoot,
				httpFallbackURL: server.URL,
				beaconCfg:       cfg,
				currentSlot:     func() uint64 { return 13 },
				requestBlocksByRange: func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
					p2pRequests.Add(1)
					return []*cltypes.SignedBeaconBlock{p2pSuccessor}, "peer", nil
				},
				validateGloasSuccessor: acceptGloasSuccessor,
			}
			downloader.slotToDownload.Store(target.Block.Slot)
			processed := 0
			downloader.SetOnNewBlock(func(_ *cltypes.SignedBeaconBlock, got *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
				processed++
				if httpFull {
					require.NotNil(t, got)
				} else {
					require.Nil(t, got)
				}
				return true, nil
			})

			require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{target}))
			require.Zero(t, p2pRequests.Load())
			require.Equal(t, 1, processed)
			expectedEnvelopeRequests := int32(0)
			if httpFull {
				expectedEnvelopeRequests = 1
			}
			require.Equal(t, expectedEnvelopeRequests, envelopeRequests.Load())
		})
	}
}

func TestBackwardBeaconDownloaderRequiresValidationForTrustedInitialSuccessor(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	successor := makeGloasBlock(12, hash(0xbb), hash(0xcc))
	linkBeaconBlocks(t, target, successor)
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)
	successorEncoded, err := successor.EncodeSSZ(nil)
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/eth/v2/beacon/blocks/12" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Eth-Consensus-Version", "gloas")
		_, _ = w.Write(successorEncoded)
	}))
	t.Cleanup(server.Close)

	downloader := &BackwardBeaconDownloader{
		expectedRoot:    targetRoot,
		httpFallbackURL: server.URL,
		beaconCfg:       cfg,
		currentSlot:     func() uint64 { return 13 },
	}
	downloader.slotToDownload.Store(target.Block.Slot)
	processed := 0
	downloader.SetOnNewBlock(func(_ *cltypes.SignedBeaconBlock, _ *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		return true, nil
	})

	require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{target}))
	require.Zero(t, processed)
	require.Equal(t, common.Hash(targetRoot), downloader.expectedRoot)
}

func TestBackwardBeaconDownloaderRejectsInvalidTrustedInitialSuccessor(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	successor := makeGloasBlock(12, hash(0xbb), hash(0xcc))
	linkBeaconBlocks(t, target, successor)
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)
	successorEncoded, err := successor.EncodeSSZ(nil)
	require.NoError(t, err)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/eth/v2/beacon/blocks/12" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Eth-Consensus-Version", "gloas")
		_, _ = w.Write(successorEncoded)
	}))
	t.Cleanup(server.Close)

	var validationCalls atomic.Int32
	var p2pRequests atomic.Int32
	downloader := &BackwardBeaconDownloader{
		expectedRoot:    targetRoot,
		httpFallbackURL: server.URL,
		beaconCfg:       cfg,
		currentSlot:     func() uint64 { return 13 },
		requestBlocksByRange: func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			p2pRequests.Add(1)
			return []*cltypes.SignedBeaconBlock{successor}, "peer", nil
		},
		validateGloasSuccessor: func(block *cltypes.SignedBeaconBlock) error {
			validationCalls.Add(1)
			require.Equal(t, common.Bytes96{}, block.Signature)
			return errors.New("invalid proposer signature")
		},
	}
	downloader.slotToDownload.Store(target.Block.Slot)
	processed := 0
	downloader.SetOnNewBlock(func(_ *cltypes.SignedBeaconBlock, _ *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		return true, nil
	})

	require.ErrorIs(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{target}), errInvalidCanonicalGloasSuccessor)
	require.Equal(t, int32(1), validationCalls.Load())
	require.Zero(t, p2pRequests.Load())
	require.Zero(t, processed)
	require.Equal(t, common.Hash(targetRoot), downloader.expectedRoot)
}

func TestBackwardBeaconDownloaderRetriesOmittedSuccessorRange(t *testing.T) {
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	successor := makeGloasBlock(12, hash(0xbb), hash(0xcc))
	linkBeaconBlocks(t, target, successor)
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)
	successorEncoded, err := successor.EncodeSSZ(nil)
	require.NoError(t, err)

	var serveSuccessor atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/eth/v2/beacon/blocks/12" || !serveSuccessor.Load() {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Eth-Consensus-Version", "gloas")
		_, _ = w.Write(successorEncoded)
	}))
	t.Cleanup(server.Close)
	downloader := &BackwardBeaconDownloader{
		expectedRoot:           targetRoot,
		httpFallbackURL:        server.URL,
		beaconCfg:              gloasFromGenesisConfig(),
		currentSlot:            func() uint64 { return 13 },
		validateGloasSuccessor: acceptGloasSuccessor,
	}
	downloader.slotToDownload.Store(target.Block.Slot)
	processed := 0
	downloader.SetOnNewBlock(func(_ *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		require.Nil(t, envelope)
		return true, nil
	})

	require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{target}))
	require.Zero(t, processed)

	serveSuccessor.Store(true)
	require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{target}))
	require.Equal(t, 1, processed)
	require.True(t, downloader.Finished())
}

func TestBackwardBeaconDownloaderAdvancesPastSuccessfulEmptyFallback(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	successor := makeGloasBlock(75, hash(0xbb), hash(0xcc))
	linkBeaconBlocks(t, target, successor)
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)
	successorEncoded, err := successor.EncodeSSZ(nil)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/eth/v2/beacon/blocks/75" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Eth-Consensus-Version", "gloas")
		_, _ = w.Write(successorEncoded)
	}))
	t.Cleanup(server.Close)

	downloader := &BackwardBeaconDownloader{
		expectedRoot:           targetRoot,
		httpFallbackURL:        server.URL,
		beaconCfg:              cfg,
		currentSlot:            func() uint64 { return 76 },
		validateGloasSuccessor: acceptGloasSuccessor,
		requestBlocksByRange: func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
			return nil, "", peers.ErrNoPeers
		},
	}
	downloader.slotToDownload.Store(target.Block.Slot)
	processed := 0
	downloader.SetOnNewBlock(func(_ *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		require.Nil(t, envelope)
		return true, nil
	})

	require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{target}))
	require.Zero(t, processed)
	require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{target}))
	require.Equal(t, 1, processed)
	require.True(t, downloader.Finished())
}

func TestBackwardBeaconDownloaderRejectsDisconnectedSuccessorBatch(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	staleChild := makeGloasBlock(11, hash(0xbb), hash(0xcc))
	canonicalChild := makeGloasBlock(12, hash(0xdd), hash(0xaa))
	linkBeaconBlocks(t, target, staleChild)
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)
	canonicalChild.Block.ParentRoot = common.Hash{0xee}
	staleEncoded, err := staleChild.EncodeSSZ(nil)
	require.NoError(t, err)
	disconnectedEncoded, err := canonicalChild.EncodeSSZ(nil)
	require.NoError(t, err)
	linkBeaconBlocks(t, staleChild, canonicalChild)
	connectedEncoded, err := canonicalChild.EncodeSSZ(nil)
	require.NoError(t, err)
	var serveConnected atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/eth/v2/beacon/blocks/11":
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(staleEncoded)
		case "/eth/v2/beacon/blocks/12":
			w.Header().Set("Eth-Consensus-Version", "gloas")
			if serveConnected.Load() {
				_, _ = w.Write(connectedEncoded)
			} else {
				_, _ = w.Write(disconnectedEncoded)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	downloader := &BackwardBeaconDownloader{
		expectedRoot:           targetRoot,
		httpFallbackURL:        server.URL,
		beaconCfg:              cfg,
		currentSlot:            func() uint64 { return 13 },
		validateGloasSuccessor: acceptGloasSuccessor,
	}
	downloader.httpPreferred.Store(true)
	downloader.slotToDownload.Store(target.Block.Slot)
	processed := 0
	downloader.SetOnNewBlock(func(_ *cltypes.SignedBeaconBlock, _ *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		return true, nil
	})

	require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{target}))
	require.Equal(t, uint64(11), downloader.gloasSuccessorNext)
	require.Zero(t, processed)
	require.Equal(t, common.Hash(targetRoot), downloader.expectedRoot)
	serveConnected.Store(true)
	require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{target}))
	require.Equal(t, 1, processed)
}

func TestBackwardBeaconDownloaderRootFallbackRejectsDisconnectedSuccessorBatch(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	wrong := makeGloasBlock(10, hash(0x99), common.Hash{0x24})
	staleChild := makeGloasBlock(11, hash(0xbb), hash(0xcc))
	disconnected := makeGloasBlock(12, hash(0xdd), hash(0xaa))
	linkBeaconBlocks(t, target, staleChild)
	disconnected.Block.ParentRoot = common.Hash{0xee}
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)
	encoded := make(map[string][]byte)
	for path, block := range map[string]*cltypes.SignedBeaconBlock{
		"/eth/v2/beacon/blocks/10":                               wrong,
		"/eth/v2/beacon/blocks/11":                               staleChild,
		"/eth/v2/beacon/blocks/12":                               disconnected,
		"/eth/v2/beacon/blocks/" + common.Hash(targetRoot).Hex(): target,
	} {
		encoded[path], err = block.EncodeSSZ(nil)
		require.NoError(t, err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, ok := encoded[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Eth-Consensus-Version", "gloas")
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)

	downloader := &BackwardBeaconDownloader{
		expectedRoot:           targetRoot,
		httpFallbackURL:        server.URL,
		beaconCfg:              cfg,
		currentSlot:            func() uint64 { return 13 },
		validateGloasSuccessor: acceptGloasSuccessor,
	}
	downloader.httpPreferred.Store(true)
	downloader.slotToDownload.Store(target.Block.Slot)
	processed := 0
	downloader.SetOnNewBlock(func(_ *cltypes.SignedBeaconBlock, _ *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		return true, nil
	})

	require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{wrong, staleChild, disconnected}))
	require.Equal(t, uint64(11), downloader.gloasSuccessorNext)
	require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{wrong, staleChild, disconnected}))
	require.ErrorIs(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{wrong, staleChild, disconnected}), errInvalidCanonicalGloasSuccessor)
	require.Zero(t, processed)
	require.Equal(t, common.Hash(targetRoot), downloader.expectedRoot)
}

func TestBackwardBeaconDownloaderRootFallbackUsesRetainedChildToClassifyEmpty(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	child := makeGloasBlock(12, hash(0xbb), hash(0xcc))
	linkBeaconBlocks(t, target, child)
	wrong := makeGloasBlock(10, hash(0xdd), common.Hash{0x99})
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)
	targetEncoded, err := target.EncodeSSZ(nil)
	require.NoError(t, err)

	var envelopeRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/eth/v2/beacon/blocks/"+common.Hash(targetRoot).Hex():
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(targetEncoded)
		case strings.HasPrefix(r.URL.Path, "/eth/v1/beacon/execution_payload_envelopes/"):
			envelopeRequests.Add(1)
			http.NotFound(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	downloader := &BackwardBeaconDownloader{
		expectedRoot:      targetRoot,
		httpFallbackURL:   server.URL,
		beaconCfg:         cfg,
		prevBatchTopBlock: child,
	}
	downloader.slotToDownload.Store(target.Block.Slot)
	processed := 0
	downloader.SetOnNewBlock(func(got *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		gotRoot, hashErr := got.Block.HashSSZ()
		require.NoError(t, hashErr)
		require.Equal(t, targetRoot, gotRoot)
		require.Nil(t, envelope)
		return true, nil
	})

	require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{wrong}))
	require.Zero(t, envelopeRequests.Load())
	require.Equal(t, 1, processed)
	require.True(t, downloader.Finished())
	require.Equal(t, target.Block.ParentRoot, downloader.expectedRoot)
}

func TestBackwardBeaconDownloaderIgnoresDisconnectedHTTPLookahead(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	server := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(server.Close)
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	canonicalChild := makeGloasBlock(12, hash(0xbb), hash(0xaa))
	linkBeaconBlocks(t, target, canonicalChild)
	disconnected := makeGloasBlock(11, hash(0xcc), hash(0xdd))
	disconnected.Block.ParentRoot = common.Hash{0xff}
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)

	downloader := &BackwardBeaconDownloader{
		expectedRoot:      targetRoot,
		beaconCfg:         cfg,
		httpFallbackURL:   server.URL,
		prevBatchTopBlock: canonicalChild,
	}
	downloader.httpPreferred.Store(true)
	downloader.slotToDownload.Store(target.Block.Slot)
	processed := 0
	downloader.SetOnNewBlock(func(_ *cltypes.SignedBeaconBlock, _ *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		return true, nil
	})

	require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{target, disconnected}))
	require.Zero(t, processed)
	require.Equal(t, common.Hash(targetRoot), downloader.expectedRoot)
}

func TestBackwardBeaconDownloaderPacesRepeatedUnresolvedEnvelopeRetry(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	child := makeGloasBlock(11, hash(0xbb), hash(0xaa))
	linkBeaconBlocks(t, target, child)
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)
	encodedTarget, err := target.EncodeSSZ(nil)
	require.NoError(t, err)
	var envelopeRequests atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/eth/v2/beacon/blocks/10":
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(encodedTarget)
		case strings.HasPrefix(r.URL.Path, "/eth/v1/beacon/execution_payload_envelopes/"):
			envelopeRequests.Add(1)
			http.NotFound(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	downloader := &BackwardBeaconDownloader{
		expectedRoot:      targetRoot,
		httpFallbackURL:   server.URL,
		beaconCfg:         cfg,
		neverSkip:         false,
		reqInterval:       time.NewTicker(time.Hour),
		prevBatchTopBlock: child,
	}
	defer downloader.reqInterval.Stop()
	downloader.httpPreferred.Store(true)
	downloader.slotToDownload.Store(target.Block.Slot)
	downloader.SetOnNewBlock(func(_ *cltypes.SignedBeaconBlock, _ *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		t.Fatal("unresolved envelope must not reach callback")
		return false, nil
	})

	require.NoError(t, downloader.RequestMore(t.Context()))
	require.Equal(t, int32(1), envelopeRequests.Load())
	require.False(t, downloader.httpPreferred.Load())

	retryCtx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	require.ErrorIs(t, downloader.RequestMore(retryCtx), context.DeadlineExceeded)
	require.GreaterOrEqual(t, time.Since(started), 25*time.Millisecond)
	require.Equal(t, int32(1), envelopeRequests.Load())
}

func TestBackwardBeaconDownloaderProvenFullDoesNotAdvanceOnHTTP404(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	lookahead := makeGloasBlock(11, hash(0xbb), hash(0xaa))
	linkBeaconBlocks(t, target, lookahead)
	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()
	downloader := &BackwardBeaconDownloader{
		expectedRoot:    targetRoot,
		httpFallbackURL: server.URL,
		beaconCfg:       cfg,
	}
	downloader.httpPreferred.Store(true)
	downloader.slotToDownload.Store(target.Block.Slot)
	processed := 0
	downloader.SetOnNewBlock(func(_ *cltypes.SignedBeaconBlock, _ *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		return false, nil
	})

	require.NoError(t, downloader.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{target, lookahead}))
	require.Zero(t, processed)
	require.Equal(t, common.Hash(targetRoot), downloader.expectedRoot)
	require.Equal(t, target.Block.Slot, downloader.Progress())
}

func TestBackwardBeaconDownloaderDoesNotSkipUnknownGloasPayload(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	defer db.Close()

	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		withoutEngine := &BackwardBeaconDownloader{
			beaconCfg:    gloasFromGenesisConfig(),
			expectedRoot: common.Hash{0xaa},
		}
		require.False(t, withoutEngine.canSkipRoot(t.Context(), tx, 0, 0, 10, withoutEngine.expectedRoot))

		engine := execution_client.NewMockExecutionEngine(gomock.NewController(t))
		withEngine := &BackwardBeaconDownloader{
			engine:       engine,
			beaconCfg:    gloasFromGenesisConfig(),
			expectedRoot: common.Hash{0xaa},
		}
		require.False(t, withEngine.canSkipRoot(t.Context(), tx, 0, 0, 10, withEngine.expectedRoot))
		return nil
	}))
}

func TestBackwardBeaconDownloaderSkipsGloasPayloadProvenEmptyBySuccessor(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	stored := makeGloasBlock(11, hash(0xbb), hash(0xaa))
	successor := makeGloasBlock(12, hash(0xcc), hash(0xaa))
	linkBeaconBlocks(t, stored, successor)
	storedRoot, err := stored.Block.HashSSZ()
	require.NoError(t, err)

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	t.Cleanup(db.Close)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.WriteBeaconBlockAndIndicies(tx, stored, false)
	}))
	downloader := &BackwardBeaconDownloader{
		ctx:               t.Context(),
		expectedRoot:      storedRoot,
		prevBatchTopBlock: successor,
		beaconCfg:         cfg,
		db:                db,
		blockReader: beaconBlockBodyReaderFunc(func(context.Context, kv.Tx, common.Hash) (*cltypes.SignedBeaconBlock, error) {
			return stored, nil
		}),
	}
	downloader.slotToDownload.Store(stored.Block.Slot)

	require.NoError(t, downloader.trySkipToExistingBlock(t.Context()))
	require.Equal(t, stored.Block.ParentRoot, downloader.expectedRoot)
	require.Equal(t, stored.Block.Slot-1, downloader.Progress())
	require.Zero(t, downloader.Processed())
}

func TestBackwardBeaconDownloaderSkipRetainsDirectGloasSuccessor(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	target := makeGloasBlock(10, hash(0xaa), common.Hash{0x42})
	storedChild := makeGloasBlock(11, hash(0xbb), hash(0xaa))
	processedChild := makeGloasBlock(12, hash(0xcc), hash(0xbb))
	lookahead := makeGloasBlock(13, hash(0xdd), hash(0xee))
	linkBeaconBlocks(t, target, storedChild, processedChild, lookahead)

	targetRoot, err := target.Block.HashSSZ()
	require.NoError(t, err)
	storedChildRoot, err := storedChild.Block.HashSSZ()
	require.NoError(t, err)
	processedChildRoot, err := processedChild.Block.HashSSZ()
	require.NoError(t, err)
	targetEncoded, err := target.EncodeSSZ(nil)
	require.NoError(t, err)
	processedChildEncoded, err := processedChild.EncodeSSZ(nil)
	require.NoError(t, err)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	envelope.Message.BeaconBlockRoot = targetRoot
	envelopeEncoded, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	t.Cleanup(db.Close)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		if err := beacon_indicies.WriteBeaconBlockAndIndicies(tx, storedChild, false); err != nil {
			return err
		}
		if err := beacon_indicies.WriteExecutionBlockNumber(tx, storedChildRoot, 11); err != nil {
			return err
		}
		return beacon_indicies.WriteExecutionBlockHash(tx, storedChildRoot, hash(0xbb))
	}))

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Eth-Consensus-Version", "gloas")
		switch r.URL.Path {
		case "/eth/v2/beacon/blocks/10":
			_, _ = w.Write(targetEncoded)
		case "/eth/v2/beacon/blocks/12":
			_, _ = w.Write(processedChildEncoded)
		case "/eth/v1/beacon/execution_payload_envelopes/" + common.Hash(targetRoot).Hex():
			_, _ = w.Write(envelopeEncoded)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	downloader := &BackwardBeaconDownloader{
		ctx:               t.Context(),
		expectedRoot:      processedChildRoot,
		prevBatchTopBlock: lookahead,
		httpFallbackURL:   server.URL,
		beaconCfg:         cfg,
		db:                db,
		neverSkip:         true,
		reqInterval:       time.NewTicker(time.Hour),
		blockReader: beaconBlockBodyReaderFunc(func(_ context.Context, _ kv.Tx, root common.Hash) (*cltypes.SignedBeaconBlock, error) {
			if root == storedChildRoot {
				return storedChild, nil
			}
			return nil, nil
		}),
	}
	t.Cleanup(downloader.reqInterval.Stop)
	downloader.httpPreferred.Store(true)
	downloader.slotToDownload.Store(processedChild.Block.Slot)
	processed := 0
	downloader.SetOnNewBlock(func(block *cltypes.SignedBeaconBlock, got *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		if block.Block.Slot == target.Block.Slot {
			require.NotNil(t, got)
			return true, nil
		}
		require.Nil(t, got)
		return false, nil
	})

	require.NoError(t, downloader.RequestMore(t.Context()))
	require.Equal(t, common.Hash(targetRoot), downloader.expectedRoot)
	require.Equal(t, storedChild, downloader.prevBatchTopBlock)
	require.NoError(t, downloader.RequestMore(t.Context()))
	require.Equal(t, 2, processed)
}

func TestBackwardBeaconDownloaderDoesNotSkipGloasWithoutReadableBlock(t *testing.T) {
	stored := makeGloasBlock(11, hash(0xbb), hash(0xaa))
	child := makeGloasBlock(12, hash(0xcc), hash(0xbb))
	linkBeaconBlocks(t, stored, child)
	storedRoot, err := stored.Block.HashSSZ()
	require.NoError(t, err)

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	t.Cleanup(db.Close)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		if err := beacon_indicies.WriteBeaconBlockAndIndicies(tx, stored, false); err != nil {
			return err
		}
		if err := beacon_indicies.WriteExecutionBlockNumber(tx, storedRoot, 11); err != nil {
			return err
		}
		return beacon_indicies.WriteExecutionBlockHash(tx, storedRoot, hash(0xbb))
	}))

	for _, tt := range []struct {
		name        string
		blockReader BeaconBlockBodyReader
		wantErr     error
	}{
		{name: "missing reader"},
		{
			name: "reader error",
			blockReader: beaconBlockBodyReaderFunc(func(context.Context, kv.Tx, common.Hash) (*cltypes.SignedBeaconBlock, error) {
				return nil, errors.New("read failed")
			}),
			wantErr: errors.New("read failed"),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			downloader := &BackwardBeaconDownloader{
				ctx:               t.Context(),
				expectedRoot:      storedRoot,
				prevBatchTopBlock: child,
				beaconCfg:         gloasFromGenesisConfig(),
				db:                db,
				blockReader:       tt.blockReader,
			}
			downloader.slotToDownload.Store(stored.Block.Slot)

			err := downloader.trySkipToExistingBlock(t.Context())
			if tt.wantErr != nil {
				require.EqualError(t, err, tt.wantErr.Error())
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, common.Hash(storedRoot), downloader.expectedRoot)
			require.Equal(t, stored.Block.Slot, downloader.Progress())
			require.Equal(t, child, downloader.prevBatchTopBlock)
		})
	}
}

func TestBackwardGloasAnchorWithoutHTTPAuthenticatesAncestors(t *testing.T) {
	for _, full := range []bool{false, true} {
		t.Run(fmt.Sprint(full), func(t *testing.T) {
			parent := makeGloasBlock(9, hash(0xaa), hash(0x42))
			anchorParentHash := hash(0xcc)
			if full {
				anchorParentHash = hash(0xaa)
			}
			anchor := makeGloasBlock(10, hash(0xbb), anchorParentHash)
			linkBeaconBlocks(t, parent, anchor)
			parentRoot, err := parent.Block.HashSSZ()
			require.NoError(t, err)
			anchorRoot, err := anchor.Block.HashSSZ()
			require.NoError(t, err)
			env := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(gloasFromGenesisConfig())}
			env.Message.BeaconBlockRoot = parentRoot
			d := &BackwardBeaconDownloader{beaconCfg: gloasFromGenesisConfig(), expectedRoot: anchorRoot, currentSlot: func() uint64 { return 13 }, validateGloasSuccessor: acceptGloasSuccessor}
			d.slotToDownload.Store(10)
			anchors, ancestors, requests := 0, 0, 0
			d.SetOnInitialGloasBlock(anchorRoot, func(block *cltypes.SignedBeaconBlock) error { anchors++; require.Same(t, anchor, block); return nil })
			d.requestEnvelopes = func(_ context.Context, roots [][32]byte, _ ...*cltypes.SignedBeaconBlock) (map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope, error) {
				requests++
				require.Equal(t, [][32]byte{parentRoot}, roots)
				return map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{parentRoot: env}, nil
			}
			d.SetOnNewBlock(func(block *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
				ancestors++
				require.Same(t, parent, block)
				if full {
					require.Same(t, env, envelope)
				} else {
					require.Nil(t, envelope)
				}
				return true, nil
			})
			require.NoError(t, d.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{parent, anchor}))
			require.Equal(t, 1, anchors)
			require.Equal(t, 1, ancestors)
			require.Equal(t, full, requests == 1)
			require.True(t, d.Finished())
		})
	}
}

func TestBackwardGloasInitialAnchorPersistenceRetries(t *testing.T) {
	parent := makeGloasBlock(9, hash(0xaa), hash(0x42))
	anchor := makeGloasBlock(10, hash(0xbb), hash(0xcc))
	linkBeaconBlocks(t, parent, anchor)
	anchorRoot, err := anchor.Block.HashSSZ()
	require.NoError(t, err)
	d := &BackwardBeaconDownloader{beaconCfg: gloasFromGenesisConfig(), expectedRoot: anchorRoot}
	d.slotToDownload.Store(10)
	attempts, processed := 0, 0
	d.SetOnInitialGloasBlock(anchorRoot, func(*cltypes.SignedBeaconBlock) error {
		attempts++
		if attempts <= 2 {
			return errors.New("anchor commit failed")
		}
		return nil
	})
	d.SetOnNewBlock(func(block *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		require.Same(t, parent, block)
		require.Nil(t, envelope)
		return true, nil
	})
	for range 2 {
		require.NoError(t, d.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{parent, anchor}))
		require.Equal(t, common.Hash(anchorRoot), d.expectedRoot)
		require.Equal(t, uint64(10), d.Progress())
		require.Nil(t, d.prevBatchTopBlock)
		require.Zero(t, processed)
	}
	require.NoError(t, d.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{parent, anchor}))
	require.Equal(t, 3, attempts)
	require.Equal(t, 1, processed)
	require.True(t, d.Finished())
}

func TestBackwardGloasInitialAnchorRestartUsesPersistedBlock(t *testing.T) {
	parent := makeGloasBlock(9, hash(0xaa), hash(0x42))
	anchor := makeGloasBlock(10, hash(0xbb), hash(0xcc))
	linkBeaconBlocks(t, parent, anchor)
	anchorRoot, err := anchor.Block.HashSSZ()
	require.NoError(t, err)
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	persist := func(block *cltypes.SignedBeaconBlock) error {
		return db.Update(t.Context(), func(tx kv.RwTx) error {
			return beacon_indicies.WriteBeaconBlockAndIndicies(tx, block, true)
		})
	}
	first := &BackwardBeaconDownloader{beaconCfg: gloasFromGenesisConfig(), expectedRoot: anchorRoot}
	first.SetOnInitialGloasBlock(anchorRoot, persist)
	first.slotToDownload.Store(10)
	require.NoError(t, first.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{anchor}))
	reads, processed := 0, 0
	restarted := &BackwardBeaconDownloader{beaconCfg: gloasFromGenesisConfig(), expectedRoot: anchorRoot, db: db}
	restarted.slotToDownload.Store(10)
	restarted.SetOnInitialGloasBlock(anchorRoot, persist)
	restarted.blockReader = beaconBlockBodyReaderFunc(func(_ context.Context, tx kv.Tx, root common.Hash) (*cltypes.SignedBeaconBlock, error) {
		reads++
		require.Equal(t, common.Hash(anchorRoot), root)
		slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, root)
		require.NoError(t, err)
		require.NotNil(t, slot)
		return anchor, nil
	})
	restarted.SetOnNewBlock(func(block *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		require.Same(t, parent, block)
		require.Nil(t, envelope)
		return true, nil
	})
	require.NoError(t, restarted.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{parent}))
	require.Equal(t, 1, reads)
	require.Equal(t, 1, processed)
	require.True(t, restarted.Finished())
}

func TestBackwardGloasInitialAnchorGenesis(t *testing.T) {
	anchor := makeGloasBlock(0, hash(0xaa), hash(0xbb))
	root, err := anchor.Block.HashSSZ()
	require.NoError(t, err)
	d := &BackwardBeaconDownloader{beaconCfg: gloasFromGenesisConfig(), expectedRoot: root}
	persisted := 0
	d.SetOnInitialGloasBlock(root, func(*cltypes.SignedBeaconBlock) error { persisted++; return nil })
	d.SetOnNewBlock(func(*cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		t.Fatal("genesis anchor payload must remain unclassified")
		return false, nil
	})
	require.NoError(t, d.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{anchor}))
	require.Equal(t, 1, persisted)
	require.True(t, d.Finished())
	require.Zero(t, d.Progress())
}

func TestBackwardGloasInitialAnchorRejectsWrongPersistedRoot(t *testing.T) {
	anchor := makeGloasBlock(10, hash(0xaa), hash(0xbb))
	wrong := makeGloasBlock(10, hash(0xcc), hash(0xbb))
	root, err := anchor.Block.HashSSZ()
	require.NoError(t, err)
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	d := &BackwardBeaconDownloader{beaconCfg: gloasFromGenesisConfig(), expectedRoot: root, db: db}
	d.slotToDownload.Store(10)
	d.SetOnInitialGloasBlock(root, func(*cltypes.SignedBeaconBlock) error { t.Fatal("persisted root was not authenticated"); return nil })
	d.blockReader = beaconBlockBodyReaderFunc(func(context.Context, kv.Tx, common.Hash) (*cltypes.SignedBeaconBlock, error) { return wrong, nil })
	require.NoError(t, d.processResponses(t.Context(), nil))
	require.Equal(t, common.Hash(root), d.expectedRoot)
	require.Equal(t, uint64(10), d.Progress())
	require.Nil(t, d.prevBatchTopBlock)
	require.False(t, d.Finished())
}

func TestBackwardGloasInitialAnchorRetainedWhileFullParentEnvelopeMissing(t *testing.T) {
	parent := makeGloasBlock(9, hash(0xaa), hash(0x42))
	anchor := makeGloasBlock(10, hash(0xbb), hash(0xaa))
	linkBeaconBlocks(t, parent, anchor)
	root, err := anchor.Block.HashSSZ()
	require.NoError(t, err)
	parentRoot, err := parent.Block.HashSSZ()
	require.NoError(t, err)
	d := &BackwardBeaconDownloader{beaconCfg: gloasFromGenesisConfig(), expectedRoot: root}
	d.slotToDownload.Store(10)
	persisted, requests, processed := 0, 0, 0
	d.SetOnInitialGloasBlock(root, func(*cltypes.SignedBeaconBlock) error { persisted++; return nil })
	env := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(gloasFromGenesisConfig())}
	env.Message.BeaconBlockRoot = parentRoot
	d.requestEnvelopes = func(_ context.Context, roots [][32]byte, _ ...*cltypes.SignedBeaconBlock) (map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope, error) {
		requests++
		require.Equal(t, [][32]byte{parentRoot}, roots)
		if requests <= 2 {
			return nil, nil
		}
		return map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{parentRoot: env}, nil
	}
	d.SetOnNewBlock(func(block *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		require.Same(t, parent, block)
		require.Same(t, env, envelope)
		return true, nil
	})
	for range 2 {
		require.NoError(t, d.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{parent, anchor}))
		require.Equal(t, common.Hash(parentRoot), d.expectedRoot)
		require.Equal(t, uint64(9), d.Progress())
		require.Same(t, anchor, d.prevBatchTopBlock)
		require.Equal(t, 1, persisted)
		require.Zero(t, processed)
	}
	require.NoError(t, d.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{parent}))
	require.Equal(t, 1, persisted)
	require.Equal(t, 1, processed)
	require.True(t, d.Finished())
}

func writeCheckpointzBlockNotFound(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusInternalServerError)
	_, _ = w.Write([]byte(`{"code":500,"message":"block not found"}`))
}

func newInitialGloasAnchorDownloader(t *testing.T, anchor *cltypes.SignedBeaconBlock, url string, currentSlot uint64, persist func(*cltypes.SignedBeaconBlock) error) *BackwardBeaconDownloader {
	t.Helper()
	root, err := anchor.Block.HashSSZ()
	require.NoError(t, err)
	d := &BackwardBeaconDownloader{
		beaconCfg:              gloasFromGenesisConfig(),
		expectedRoot:           root,
		httpFallbackURL:        url,
		currentSlot:            func() uint64 { return currentSlot },
		validateGloasSuccessor: acceptGloasSuccessor,
	}
	d.slotToDownload.Store(anchor.Block.Slot)
	d.SetOnInitialGloasBlock(root, persist)
	return d
}

func TestBackwardPreGloasPersistedAnchorNotProcessedFromDB(t *testing.T) {
	anchor := makeDenebBlock(10)
	anchor.Block.ParentRoot = hash(0x99)
	anchorRoot, err := anchor.Block.HashSSZ()
	require.NoError(t, err)

	persisted, processed := 0, 0
	d := newInitialGloasAnchorDownloader(t, anchor, "", 13, func(*cltypes.SignedBeaconBlock) error {
		persisted++
		return nil
	})
	d.db = mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	d.blockReader = beaconBlockBodyReaderFunc(func(_ context.Context, _ kv.Tx, root common.Hash) (*cltypes.SignedBeaconBlock, error) {
		require.Equal(t, common.Hash(anchorRoot), root)
		return anchor, nil
	})
	d.SetOnNewBlock(func(*cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		return false, nil
	})

	require.NoError(t, d.processResponses(t.Context(), nil))
	require.Zero(t, processed)
	require.Zero(t, persisted)
	require.Equal(t, common.Hash(anchorRoot), d.expectedRoot)
	require.Equal(t, uint64(10), d.Progress())
}

func TestBackwardGloasAnchorDefersWhenSuccessorSourceFails(t *testing.T) {
	parent := makeGloasBlock(9, hash(0xaa), hash(0x42))
	anchor := makeGloasBlock(10, hash(0xbb), hash(0xcc))
	linkBeaconBlocks(t, parent, anchor)
	anchorRoot, err := anchor.Block.HashSSZ()
	require.NoError(t, err)
	anchorEncoded, err := anchor.EncodeSSZ(nil)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/eth/v2/beacon/blocks/10" {
			writeCheckpointzBlockNotFound(w)
			return
		}
		w.Header().Set("Eth-Consensus-Version", "gloas")
		_, _ = w.Write(anchorEncoded)
	}))
	t.Cleanup(server.Close)

	anchors, processed := 0, 0
	d := newInitialGloasAnchorDownloader(t, anchor, server.URL, 13, func(block *cltypes.SignedBeaconBlock) error {
		anchors++
		require.Same(t, anchor, block)
		return nil
	})
	d.SetOnNewBlock(func(block *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		require.Same(t, parent, block)
		require.Nil(t, envelope)
		return true, nil
	})

	for range maxGloasSuccessorFailures - 1 {
		require.NoError(t, d.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{parent, anchor}))
		require.Zero(t, anchors)
		require.Equal(t, common.Hash(anchorRoot), d.expectedRoot)
	}
	require.NoError(t, d.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{parent, anchor}))
	require.Equal(t, 1, anchors)
	require.Equal(t, 1, processed)
	require.True(t, d.Finished())
}

func TestBackwardGloasAnchorDefersWhenSuccessorSourceHasNoChild(t *testing.T) {
	anchor := makeGloasBlock(10, hash(0xbb), hash(0xcc))
	anchor.Block.ParentRoot = hash(0x99)
	server := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(server.Close)

	anchors := 0
	d := newInitialGloasAnchorDownloader(t, anchor, server.URL, 100, func(*cltypes.SignedBeaconBlock) error {
		anchors++
		return nil
	})

	// Each sweep from the anchor to the current slot takes two ranges.
	for range 2*maxGloasSuccessorFailures - 1 {
		require.NoError(t, d.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{anchor}))
		require.Zero(t, anchors)
	}
	require.NoError(t, d.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{anchor}))
	require.Equal(t, 1, anchors)
	require.Equal(t, hash(0x99), d.expectedRoot)
	require.Equal(t, uint64(9), d.Progress())
}

func TestBackwardGloasAnchorDefersWhenDirectChildIsNotServed(t *testing.T) {
	anchor := makeGloasBlock(10, hash(0xaa), hash(0x42))
	child := makeGloasBlock(11, hash(0xbb), hash(0xcc))
	descendant := makeGloasBlock(12, hash(0xdd), hash(0xee))
	linkBeaconBlocks(t, anchor, child, descendant)
	descendantEncoded, err := descendant.EncodeSSZ(nil)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/eth/v2/beacon/blocks/12" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Eth-Consensus-Version", "gloas")
		_, _ = w.Write(descendantEncoded)
	}))
	t.Cleanup(server.Close)

	anchors := 0
	d := newInitialGloasAnchorDownloader(t, anchor, server.URL, 13, func(*cltypes.SignedBeaconBlock) error {
		anchors++
		return nil
	})

	for range maxGloasSuccessorFailures - 1 {
		require.NoError(t, d.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{anchor}))
		require.Zero(t, anchors)
	}
	require.NoError(t, d.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{anchor}))
	require.Equal(t, 1, anchors)
	require.Equal(t, anchor.Block.ParentRoot, d.expectedRoot)
}

func TestBackwardGloasRootFallbackAnchorDefersWhenSuccessorSourceFails(t *testing.T) {
	anchor := makeGloasBlock(10, hash(0xbb), hash(0xcc))
	anchor.Block.ParentRoot = hash(0x99)
	anchorRoot, err := anchor.Block.HashSSZ()
	require.NoError(t, err)
	anchorEncoded, err := anchor.EncodeSSZ(nil)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/eth/v2/beacon/blocks/"+common.Hash(anchorRoot).Hex() {
			writeCheckpointzBlockNotFound(w)
			return
		}
		w.Header().Set("Eth-Consensus-Version", "gloas")
		_, _ = w.Write(anchorEncoded)
	}))
	t.Cleanup(server.Close)

	anchors := 0
	d := newInitialGloasAnchorDownloader(t, anchor, server.URL, 13, func(block *cltypes.SignedBeaconBlock) error {
		anchors++
		require.Equal(t, anchor.Block.Slot, block.Block.Slot)
		return nil
	})
	d.SetOnNewBlock(func(*cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		t.Fatal("deferred anchor payload must remain unclassified")
		return false, nil
	})

	for range maxGloasSuccessorFailures - 1 {
		require.NoError(t, d.processResponses(t.Context(), nil))
		require.Zero(t, anchors)
		require.Equal(t, common.Hash(anchorRoot), d.expectedRoot)
	}
	require.NoError(t, d.processResponses(t.Context(), nil))
	require.Equal(t, 1, anchors)
	require.Equal(t, hash(0x99), d.expectedRoot)
	require.Equal(t, uint64(9), d.Progress())
}

func TestBackwardGloasPersistedAnchorDefersWhenSourcesFail(t *testing.T) {
	anchor := makeGloasBlock(10, hash(0xbb), hash(0xcc))
	anchor.Block.ParentRoot = hash(0x99)
	anchorRoot, err := anchor.Block.HashSSZ()
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeCheckpointzBlockNotFound(w)
	}))
	t.Cleanup(server.Close)

	anchors := 0
	d := newInitialGloasAnchorDownloader(t, anchor, server.URL, 13, func(*cltypes.SignedBeaconBlock) error {
		anchors++
		return nil
	})
	d.db = mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	d.blockReader = beaconBlockBodyReaderFunc(func(_ context.Context, _ kv.Tx, root common.Hash) (*cltypes.SignedBeaconBlock, error) {
		if root == anchorRoot {
			return anchor, nil
		}
		return nil, nil
	})

	for range maxGloasSuccessorFailures - 1 {
		require.NoError(t, d.processResponses(t.Context(), nil))
		require.Zero(t, anchors)
	}
	require.NoError(t, d.processResponses(t.Context(), nil))
	require.Equal(t, 1, anchors)
	require.Equal(t, hash(0x99), d.expectedRoot)
}

func TestBackwardGloasPersistedAnchorClassifiedWhenSuccessorSourceServesChild(t *testing.T) {
	anchor := makeGloasBlock(10, hash(0xaa), hash(0x42))
	child := makeGloasBlock(11, hash(0xbb), hash(0xaa))
	linkBeaconBlocks(t, anchor, child)
	anchorRoot, err := anchor.Block.HashSSZ()
	require.NoError(t, err)
	childEncoded, err := child.EncodeSSZ(nil)
	require.NoError(t, err)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(gloasFromGenesisConfig())}
	envelope.Message.BeaconBlockRoot = anchorRoot
	envelopeEncoded, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/eth/v2/beacon/blocks/11":
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(childEncoded)
		case "/eth/v1/beacon/execution_payload_envelopes/" + common.Hash(anchorRoot).Hex():
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(envelopeEncoded)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	persisted, processed := 0, 0
	d := newInitialGloasAnchorDownloader(t, anchor, server.URL, 12, func(*cltypes.SignedBeaconBlock) error {
		persisted++
		return nil
	})
	d.db = mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	d.blockReader = beaconBlockBodyReaderFunc(func(_ context.Context, _ kv.Tx, root common.Hash) (*cltypes.SignedBeaconBlock, error) {
		if root == anchorRoot {
			return anchor, nil
		}
		return nil, nil
	})
	d.SetOnNewBlock(func(block *cltypes.SignedBeaconBlock, got *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		require.Same(t, anchor, block)
		require.NotNil(t, got)
		require.Equal(t, common.Hash(anchorRoot), got.Message.BeaconBlockRoot)
		return false, nil
	})

	require.NoError(t, d.processResponses(t.Context(), nil))
	require.Equal(t, 1, processed)
	require.Zero(t, persisted)
	require.Equal(t, anchor.Block.ParentRoot, d.expectedRoot)
	require.Equal(t, anchor.Block.Slot-1, d.Progress())
}

func TestBackwardGloasPersistedAnchorRetriedWithRetainedChild(t *testing.T) {
	anchor := makeGloasBlock(10, hash(0xaa), hash(0x42))
	child := makeGloasBlock(11, hash(0xbb), hash(0xaa))
	linkBeaconBlocks(t, anchor, child)
	anchorRoot, err := anchor.Block.HashSSZ()
	require.NoError(t, err)
	childEncoded, err := child.EncodeSSZ(nil)
	require.NoError(t, err)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(gloasFromGenesisConfig())}
	envelope.Message.BeaconBlockRoot = anchorRoot
	envelopeEncoded, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)
	var envelopeRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/eth/v2/beacon/blocks/11":
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(childEncoded)
		case "/eth/v1/beacon/execution_payload_envelopes/" + common.Hash(anchorRoot).Hex():
			if envelopeRequests.Add(1) == 1 {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(envelopeEncoded)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	persisted, processed := 0, 0
	d := newInitialGloasAnchorDownloader(t, anchor, server.URL, 12, func(*cltypes.SignedBeaconBlock) error {
		persisted++
		return nil
	})
	d.db = mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	d.blockReader = beaconBlockBodyReaderFunc(func(_ context.Context, _ kv.Tx, root common.Hash) (*cltypes.SignedBeaconBlock, error) {
		if root == anchorRoot {
			return anchor, nil
		}
		return nil, nil
	})
	d.SetOnNewBlock(func(block *cltypes.SignedBeaconBlock, got *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		require.Same(t, anchor, block)
		require.NotNil(t, got)
		require.Equal(t, common.Hash(anchorRoot), got.Message.BeaconBlockRoot)
		return false, nil
	})

	require.NoError(t, d.processResponses(t.Context(), nil))
	require.Zero(t, processed)
	require.Zero(t, persisted)
	require.Equal(t, common.Hash(anchorRoot), d.expectedRoot)
	require.Equal(t, anchor.Block.Slot, d.Progress())

	require.NoError(t, d.processResponses(t.Context(), nil))
	require.Equal(t, 1, processed)
	require.Zero(t, persisted)
	require.Equal(t, anchor.Block.ParentRoot, d.expectedRoot)
	require.Equal(t, anchor.Block.Slot-1, d.Progress())
	require.Equal(t, int32(2), envelopeRequests.Load())
}

func TestBackwardGloasAnchorRootFallbackAfterDBReadError(t *testing.T) {
	anchor := makeGloasBlock(10, hash(0xaa), hash(0x42))
	child := makeGloasBlock(11, hash(0xbb), hash(0xcc))
	linkBeaconBlocks(t, anchor, child)
	anchorRoot, err := anchor.Block.HashSSZ()
	require.NoError(t, err)
	anchorEncoded, err := anchor.EncodeSSZ(nil)
	require.NoError(t, err)
	childEncoded, err := child.EncodeSSZ(nil)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Eth-Consensus-Version", "gloas")
		switch r.URL.Path {
		case "/eth/v2/beacon/blocks/" + common.Hash(anchorRoot).Hex():
			_, _ = w.Write(anchorEncoded)
		case "/eth/v2/beacon/blocks/11":
			_, _ = w.Write(childEncoded)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	persisted, processed := 0, 0
	d := newInitialGloasAnchorDownloader(t, anchor, server.URL, 12, func(*cltypes.SignedBeaconBlock) error {
		persisted++
		return nil
	})
	d.db = mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	d.blockReader = beaconBlockBodyReaderFunc(func(context.Context, kv.Tx, common.Hash) (*cltypes.SignedBeaconBlock, error) {
		return nil, errors.New("read failed")
	})
	d.SetOnNewBlock(func(block *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		root, err := block.Block.HashSSZ()
		require.NoError(t, err)
		require.Equal(t, anchorRoot, root)
		require.Nil(t, envelope)
		return false, nil
	})

	require.NoError(t, d.processResponses(t.Context(), nil))
	require.Equal(t, 1, processed)
	require.Zero(t, persisted)
	require.Equal(t, anchor.Block.ParentRoot, d.expectedRoot)
	require.Equal(t, anchor.Block.Slot-1, d.Progress())
}

func TestBackwardGloasAnchorClassifiedWhenSuccessorSourceRecovers(t *testing.T) {
	anchor := makeGloasBlock(10, hash(0xaa), hash(0x42))
	successor := makeGloasBlock(12, hash(0xbb), hash(0xcc))
	linkBeaconBlocks(t, anchor, successor)
	successorEncoded, err := successor.EncodeSSZ(nil)
	require.NoError(t, err)
	var failing atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case failing.Load():
			writeCheckpointzBlockNotFound(w)
		case r.URL.Path == "/eth/v2/beacon/blocks/12":
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(successorEncoded)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	d := newInitialGloasAnchorDownloader(t, anchor, server.URL, 13, func(*cltypes.SignedBeaconBlock) error {
		t.Fatal("anchor must be classified by the source that serves its child")
		return nil
	})
	processed := 0
	d.SetOnNewBlock(func(block *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		require.Same(t, anchor, block)
		require.Nil(t, envelope)
		return true, nil
	})

	failing.Store(true)
	for range maxGloasSuccessorFailures - 1 {
		require.NoError(t, d.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{anchor}))
		require.Zero(t, processed)
	}
	failing.Store(false)
	require.NoError(t, d.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{anchor}))
	require.Equal(t, 1, processed)
	require.True(t, d.Finished())
	require.Equal(t, anchor.Block.ParentRoot, d.expectedRoot)
}

func TestBackwardGloasSuccessorFromPartialHTTPRange(t *testing.T) {
	anchor := makeGloasBlock(10, hash(0xaa), hash(0x42))
	successor := makeGloasBlock(11, hash(0xbb), hash(0xcc))
	linkBeaconBlocks(t, anchor, successor)
	successorEncoded, err := successor.EncodeSSZ(nil)
	require.NoError(t, err)
	successorServed := make(chan struct{})
	closeSuccessorServed := sync.OnceFunc(func() { close(successorServed) })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/eth/v2/beacon/blocks/11":
			defer closeSuccessorServed()
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(successorEncoded)
		default:
			// Delay failures until after the child response has been served.
			<-successorServed
			time.Sleep(100 * time.Millisecond)
			writeCheckpointzBlockNotFound(w)
		}
	}))
	t.Cleanup(server.Close)

	d := newInitialGloasAnchorDownloader(t, anchor, server.URL, 13, func(*cltypes.SignedBeaconBlock) error {
		t.Fatal("anchor must be classified from the served child")
		return nil
	})
	processed := 0
	d.SetOnNewBlock(func(block *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		require.Same(t, anchor, block)
		require.Nil(t, envelope)
		return true, nil
	})

	require.NoError(t, d.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{anchor}))
	require.Equal(t, 1, processed)
}

func TestBackwardGloasAnchorClassifiedWhenLaterSlotFailureCancelsChild(t *testing.T) {
	anchor := makeGloasBlock(10, hash(0xaa), hash(0x42))
	child := makeGloasBlock(12, hash(0xbb), hash(0xcc))
	linkBeaconBlocks(t, anchor, child)
	childEncoded, err := child.EncodeSSZ(nil)
	require.NoError(t, err)
	childStarted := make(chan struct{})
	var childRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/eth/v2/beacon/blocks/12":
			if childRequests.Add(1) == 1 {
				close(childStarted)
				<-r.Context().Done()
				return
			}
			w.Header().Set("Eth-Consensus-Version", "gloas")
			_, _ = w.Write(childEncoded)
		case "/eth/v2/beacon/blocks/13":
			select {
			case <-childStarted:
				writeCheckpointzBlockNotFound(w)
			case <-r.Context().Done():
			}
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	d := newInitialGloasAnchorDownloader(t, anchor, server.URL, 14, func(*cltypes.SignedBeaconBlock) error {
		t.Fatal("anchor must be classified by the source that serves its child")
		return nil
	})
	processed := 0
	d.SetOnNewBlock(func(block *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed++
		require.Same(t, anchor, block)
		require.Nil(t, envelope)
		return true, nil
	})

	require.NoError(t, d.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{anchor}))
	require.Equal(t, 1, processed)
	require.True(t, d.Finished())
	require.Equal(t, int32(2), childRequests.Load())
}

func TestBackwardGloasSuccessorRangeRetryOfOnly404sIsEmpty(t *testing.T) {
	anchor := makeGloasBlock(10, hash(0xaa), hash(0x42))
	anchorRoot, err := anchor.Block.HashSSZ()
	require.NoError(t, err)
	var slot12Requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/eth/v2/beacon/blocks/12" && slot12Requests.Add(1) == 1 {
			writeCheckpointzBlockNotFound(w)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(server.Close)

	d := newInitialGloasAnchorDownloader(t, anchor, server.URL, 100, func(*cltypes.SignedBeaconBlock) error {
		t.Fatal("anchor must not be persisted")
		return nil
	})
	require.NoError(t, d.processResponses(t.Context(), []*cltypes.SignedBeaconBlock{anchor}))
	require.Equal(t, uint64(75), d.gloasSuccessorNext)
	require.Equal(t, common.Hash(anchorRoot), d.expectedRoot)
	require.Equal(t, int32(2), slot12Requests.Load())
}

func TestBackwardRootFallbackDoesNotAdvanceWhenEnvelopeFetchFails(t *testing.T) {
	block := makeGloasBlock(7, hash(1), hash(2))
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	encoded, err := block.EncodeSSZ(nil)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/eth/v2/beacon/blocks/0x"+common.Bytes2Hex(root[:]) {
			w.Header().Set("Eth-Consensus-Version", clparams.GloasVersion.String())
			_, _ = w.Write(encoded)
			return
		}
		http.Error(w, "temporary failure", http.StatusInternalServerError)
	}))
	defer server.Close()

	processed := 0
	downloader := &BackwardBeaconDownloader{
		httpFallbackURL: server.URL,
		beaconCfg:       &clparams.MainnetBeaconConfig,
		expectedRoot:    root,
		onNewBlock: func(*cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
			processed++
			return false, nil
		},
	}
	downloader.slotToDownload.Store(block.Block.Slot)

	require.NoError(t, downloader.processResponses(context.Background(), nil))
	require.Zero(t, processed)
	require.Equal(t, common.Hash(root), downloader.expectedRoot)
	require.Equal(t, block.Block.Slot, downloader.slotToDownload.Load())
}

func TestBackwardBeaconDownloaderRejectsOversizedEnvelopeResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(make([]byte, clparams.MaxChunkSize+1))
	}))
	defer server.Close()
	downloader := &BackwardBeaconDownloader{
		httpFallbackURL: server.URL,
		beaconCfg:       &clparams.MainnetBeaconConfig,
	}

	_, err := downloader.fetchSingleEnvelope(context.Background(), makeGloasBlock(1, hash(1), hash(2)))
	require.ErrorContains(t, err, "response body exceeds")
}

func TestForwardBeaconDownloaderRejectsOversizedEnvelopeResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(make([]byte, clparams.MaxChunkSize+1))
	}))
	defer server.Close()
	block := makeGloasBlock(1, hash(1), hash(2))
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	received := map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{}

	fetched := fetchEnvelopesFromBeaconAPI(
		context.Background(), server.URL, []*cltypes.SignedBeaconBlock{block}, [][32]byte{root}, received, &clparams.MainnetBeaconConfig,
	)
	require.Zero(t, fetched)
	require.Empty(t, received)
}

func TestEnvelopeHTTPFallbackRejectsMismatchedBlockRoot(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: cltypes.NewExecutionPayloadEnvelope(cfg),
	}
	envelope.Message.BeaconBlockRoot = hash(0xff)
	encoded, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Eth-Consensus-Version", clparams.GloasVersion.String())
		_, _ = w.Write(encoded)
	}))
	defer server.Close()

	block := makeGloasBlock(1, hash(1), hash(2))
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	downloader := &BackwardBeaconDownloader{
		httpFallbackURL: server.URL,
		beaconCfg:       cfg,
	}
	_, err = downloader.fetchSingleEnvelope(context.Background(), block)
	require.ErrorContains(t, err, "block root")

	received := map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{}
	fetched := fetchEnvelopesFromBeaconAPI(
		context.Background(), server.URL, []*cltypes.SignedBeaconBlock{block}, [][32]byte{root}, received, cfg,
	)
	require.Zero(t, fetched)
	require.Empty(t, received)
}

func TestEnvelopeHTTPFallbackRejectsConfiguredRequestLimit(t *testing.T) {
	cfg := *gloasFromGenesisConfig()
	cfg.MaxBuilderDepositRequestsPerPayload = 1
	block := makeGloasBlock(1, hash(1), hash(2))
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(&cfg)}
	envelope.Message.BeaconBlockRoot = root
	envelope.Message.ExecutionRequests.BuilderDeposits.Append(&solid.BuilderDepositRequest{})
	envelope.Message.ExecutionRequests.BuilderDeposits.Append(&solid.BuilderDepositRequest{})
	encoded, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Eth-Consensus-Version", clparams.GloasVersion.String())
		_, _ = w.Write(encoded)
	}))
	defer server.Close()

	downloader := &BackwardBeaconDownloader{httpFallbackURL: server.URL, beaconCfg: &cfg}
	_, err = downloader.fetchSingleEnvelope(context.Background(), block)
	require.ErrorContains(t, err, "builder deposits")

	received := map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{}
	require.Zero(t, fetchEnvelopesFromBeaconAPI(context.Background(), server.URL, []*cltypes.SignedBeaconBlock{block}, [][32]byte{root}, received, &cfg))
	require.Empty(t, received)
}

func TestEnvelopeHTTPFallbackFetchesByBlockRoot(t *testing.T) {
	cfg := gloasFromGenesisConfig()
	block := makeGloasBlock(9, hash(1), hash(2))
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	envelope.Message.BeaconBlockRoot = root
	encoded, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)
	wantPath := "/eth/v1/beacon/execution_payload_envelopes/0x" + common.Bytes2Hex(root[:])
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != wantPath {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Eth-Consensus-Version", clparams.GloasVersion.String())
		_, _ = w.Write(encoded)
	}))
	defer server.Close()

	downloader := &BackwardBeaconDownloader{httpFallbackURL: server.URL, beaconCfg: cfg}
	fetchedEnvelope, err := downloader.fetchSingleEnvelope(context.Background(), block)
	require.NoError(t, err)
	require.NotNil(t, fetchedEnvelope)

	received := map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{}
	require.Equal(t, 1, fetchEnvelopesFromBeaconAPI(context.Background(), server.URL, []*cltypes.SignedBeaconBlock{block}, [][32]byte{root}, received, cfg).fetched)
	require.Contains(t, received, common.Hash(root))
}

func TestEnvelopeHTTPFallbackRejectsPreGloasVersion(t *testing.T) {
	block := makeGloasBlock(1, hash(1), hash(2))
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig),
	}
	envelope.Message.BeaconBlockRoot = root
	encoded, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Eth-Consensus-Version", clparams.FuluVersion.String())
		_, _ = w.Write(encoded)
	}))
	defer server.Close()

	downloader := &BackwardBeaconDownloader{
		httpFallbackURL: server.URL,
		beaconCfg:       &clparams.MainnetBeaconConfig,
	}
	_, err = downloader.fetchSingleEnvelope(context.Background(), block)
	require.ErrorContains(t, err, "consensus version")

	received := map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{}
	fetched := fetchEnvelopesFromBeaconAPI(
		context.Background(), server.URL, []*cltypes.SignedBeaconBlock{block}, [][32]byte{root}, received, &clparams.MainnetBeaconConfig,
	)
	require.Zero(t, fetched)
	require.Empty(t, received)
}

func TestBackwardBeaconDownloaderCountsOnlyProcessedBlocks(t *testing.T) {
	parent := makeDenebBlock(10)
	parentRoot, err := parent.Block.HashSSZ()
	require.NoError(t, err)
	child := makeDenebBlock(11)
	child.Block.ParentRoot = parentRoot
	childRoot, err := child.Block.HashSSZ()
	require.NoError(t, err)

	b := NewBackwardBeaconDownloader(t.Context(), nil, nil, nil, nil, &clparams.MainnetBeaconConfig)
	b.SetSlotToDownload(11)
	b.SetExpectedRoot(childRoot)
	failNext := true
	b.SetOnNewBlock(func(*cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		if failNext {
			failNext = false
			return false, errors.New("collector failed")
		}
		return false, nil
	})

	responses := []*cltypes.SignedBeaconBlock{parent, child}
	require.NoError(t, b.processResponses(t.Context(), responses))
	require.Zero(t, b.Processed(), "a block whose onNewBlock failed is retried, not processed")
	require.Equal(t, uint64(11), b.Progress())

	require.NoError(t, b.processResponses(t.Context(), responses))
	require.Equal(t, uint64(2), b.Processed())
	require.Equal(t, uint64(9), b.Progress())
}
