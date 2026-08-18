package stages

import (
	"context"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	state2 "github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

type selectedHeadEnvelopeTestStore struct {
	mu        sync.Mutex
	envelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope
}

func (s *selectedHeadEnvelopeTestStore) HasEnvelope(root common.Hash) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.envelopes[root] != nil
}

func (s *selectedHeadEnvelopeTestStore) OnExecutionPayload(_ context.Context, envelope *cltypes.SignedExecutionPayloadEnvelope, _, _ bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.envelopes[envelope.Message.BeaconBlockRoot] = envelope
	return nil
}

func TestWaitForSelectedHeadEnvelopeRequestsAndAppliesPeerEnvelope(t *testing.T) {
	headRoot := common.HexToHash("0x1234")
	store := &selectedHeadEnvelopeTestStore{envelopes: make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope)}
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: headRoot}}
	requests := make(chan [][32]byte, 1)

	waitForSelectedHeadEnvelope(context.Background(), store, func(_ context.Context, roots [][32]byte) (map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope, error) {
		requests <- roots
		return map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{headRoot: envelope}, nil
	}, headRoot, time.Second, true, false)

	require.Equal(t, [][32]byte{headRoot}, <-requests)
	require.True(t, store.HasEnvelope(headRoot))
}

func TestSelectedHeadEnvelopeRequestAttemptsOncePerHead(t *testing.T) {
	cfg := &Cfg{}
	firstHead := common.HexToHash("0x1234")
	secondHead := common.HexToHash("0x5678")

	firstAttempt, ok, wait := claimSelectedHeadEnvelopeRequest(cfg, firstHead)
	require.True(t, ok)
	require.True(t, wait)
	_, ok, wait = claimSelectedHeadEnvelopeRequest(cfg, firstHead)
	require.False(t, ok)
	require.True(t, wait)
	releaseSelectedHeadEnvelopeRequest(cfg, firstAttempt)
	_, ok, wait = claimSelectedHeadEnvelopeRequest(cfg, firstHead)
	require.False(t, ok)
	require.False(t, wait)
	observeSelectedHeadEnvelopeRequest(cfg, secondHead)
	observeSelectedHeadEnvelopeRequest(cfg, firstHead)
	thirdAttempt, ok, wait := claimSelectedHeadEnvelopeRequest(cfg, firstHead)
	require.True(t, ok)
	require.True(t, wait)
	releaseSelectedHeadEnvelopeRequest(cfg, thirdAttempt)

	observeSelectedHeadEnvelopeRequest(cfg, secondHead)
	secondHeadAttempt, ok, wait := claimSelectedHeadEnvelopeRequest(cfg, secondHead)
	require.True(t, ok)
	require.True(t, wait)
	releaseSelectedHeadEnvelopeRequest(cfg, secondHeadAttempt)
	secondAttempt, ok, wait := claimSelectedHeadEnvelopeRequest(cfg, firstHead)
	require.True(t, ok)
	require.True(t, wait)
	releaseSelectedHeadEnvelopeRequest(cfg, firstAttempt)
	_, ok, wait = claimSelectedHeadEnvelopeRequest(cfg, firstHead)
	require.False(t, ok)
	require.True(t, wait)
	releaseSelectedHeadEnvelopeRequest(cfg, secondAttempt)
}

func TestGloasRecoveryCursorAdvancesAfterIncompleteFetch(t *testing.T) {
	cfg := &Cfg{}
	scanRoot := common.HexToHash("0x1234")

	advanceGloasEnvelopeRecoveryCursor(cfg, scanRoot, false)
	require.Equal(t, scanRoot, cfg.gloasEnvelopeRecoveryCursor)

	advanceGloasEnvelopeRecoveryCursor(cfg, scanRoot, true)
	require.Equal(t, common.Hash{}, cfg.gloasEnvelopeRecoveryCursor)
}

func TestGloasVerificationItemFailureOnlyStopsOnCancellation(t *testing.T) {
	completeBatch := true
	require.True(t, continueGloasVerificationAfterItemFailure(context.Background(), &completeBatch))
	require.True(t, completeBatch)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	completeBatch = true
	require.False(t, continueGloasVerificationAfterItemFailure(canceled, &completeBatch))
	require.False(t, completeBatch)
}

func TestGloasVerificationImmediateFailureDoesNotFreezeBatch(t *testing.T) {
	completeBatch := true
	processed := 0
	items := []*gloasVerificationItem{{}, {}}
	process := func(gloasVerificationItem) bool {
		processed++
		if processed == 1 {
			return continueGloasVerificationAfterItemFailure(context.Background(), &completeBatch)
		}
		return true
	}

	processImmediateGloasVerificationItems(items[0], items[1], process, &completeBatch)

	require.Equal(t, 2, processed)
	require.True(t, completeBatch)
}

func TestBlockSupportsExecutionPayloadEnvelopeUsesBlockVersion(t *testing.T) {
	require.False(t, blockSupportsExecutionPayloadEnvelope(nil))
	require.False(t, blockSupportsExecutionPayloadEnvelope(&cltypes.SignedBeaconBlock{}))
	require.False(t, blockSupportsExecutionPayloadEnvelope(&cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{Body: &cltypes.BeaconBody{Version: clparams.FuluVersion}},
	}))
	require.True(t, blockSupportsExecutionPayloadEnvelope(&cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{Body: &cltypes.BeaconBody{Version: clparams.GloasVersion}},
	}))
}

func TestWaitForSelectedHeadEnvelopeDoesNotRequestUnclaimedHead(t *testing.T) {
	store := &selectedHeadEnvelopeTestStore{envelopes: make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope)}
	requests := make(chan struct{}, 1)
	waitForSelectedHeadEnvelope(context.Background(), store, func(context.Context, [][32]byte) (map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope, error) {
		requests <- struct{}{}
		return nil, nil
	}, common.HexToHash("0x1234"), time.Millisecond, false, false)

	select {
	case <-requests:
		t.Fatal("unclaimed head triggered a peer request")
	default:
	}
}

func TestWaitForSelectedHeadEnvelopeBoundsEmptyPeerResponse(t *testing.T) {
	store := &selectedHeadEnvelopeTestStore{envelopes: make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope)}
	requests := make(chan struct{}, 1)
	waitForSelectedHeadEnvelope(context.Background(), store, func(requestCtx context.Context, _ [][32]byte) (map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope, error) {
		requests <- struct{}{}
		<-requestCtx.Done()
		return nil, requestCtx.Err()
	}, common.HexToHash("0x1234"), 10*time.Millisecond, true, false)

	require.Len(t, requests, 1)
}

func TestWaitForSelectedHeadEnvelopeDoesNotJoinLateRequester(t *testing.T) {
	headRoot := common.HexToHash("0x1234")
	store := &selectedHeadEnvelopeTestStore{envelopes: make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope)}
	release := make(chan struct{})
	requestDone := make(chan struct{})
	returned := make(chan struct{})
	go func() {
		waitForSelectedHeadEnvelope(context.Background(), store, func(context.Context, [][32]byte) (map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope, error) {
			defer close(requestDone)
			<-release
			return map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{
				headRoot: {Message: &cltypes.ExecutionPayloadEnvelope{BeaconBlockRoot: headRoot}},
			}, nil
		}, headRoot, 10*time.Millisecond, true, false)
		close(returned)
	}()

	select {
	case <-returned:
	case <-time.After(100 * time.Millisecond):
		close(release)
		<-returned
		t.Fatal("selected-head wait joined a late requester")
	}
	close(release)
	<-requestDone
	require.False(t, store.HasEnvelope(headRoot))
}

func TestValidateAnchorEnvelope(t *testing.T) {
	cfg, st, bid, env, anchorRoot := validAnchorEnvelopeFixture(t, 1)

	require.NoError(t, validateAnchorEnvelope(cfg, st, anchorRoot, bid, env))

	tests := []struct {
		name    string
		mutate  func(*cltypes.ExecutionPayloadBid, *cltypes.SignedExecutionPayloadEnvelope)
		wantErr string
	}{
		{
			name: "beacon root mismatch",
			mutate: func(_ *cltypes.ExecutionPayloadBid, env *cltypes.SignedExecutionPayloadEnvelope) {
				env.Message.BeaconBlockRoot = common.HexToHash("0x99")
			},
			wantErr: "beacon block root mismatch",
		},
		{
			name: "parent root mismatch",
			mutate: func(_ *cltypes.ExecutionPayloadBid, env *cltypes.SignedExecutionPayloadEnvelope) {
				env.Message.ParentBeaconBlockRoot = common.HexToHash("0x98")
			},
			wantErr: "parent beacon block root mismatch",
		},
		{
			name: "builder index mismatch",
			mutate: func(_ *cltypes.ExecutionPayloadBid, env *cltypes.SignedExecutionPayloadEnvelope) {
				env.Message.BuilderIndex++
			},
			wantErr: "builder index mismatch",
		},
		{
			name: "block hash mismatch",
			mutate: func(bid *cltypes.ExecutionPayloadBid, _ *cltypes.SignedExecutionPayloadEnvelope) {
				bid.BlockHash = common.HexToHash("0x97")
			},
			wantErr: "block hash mismatch",
		},
		{
			name: "parent block hash mismatch",
			mutate: func(_ *cltypes.ExecutionPayloadBid, env *cltypes.SignedExecutionPayloadEnvelope) {
				env.Message.Payload.ParentHash = common.HexToHash("0x96")
			},
			wantErr: "parent block hash mismatch",
		},
		{
			name: "prev randao mismatch",
			mutate: func(_ *cltypes.ExecutionPayloadBid, env *cltypes.SignedExecutionPayloadEnvelope) {
				env.Message.Payload.PrevRandao = common.HexToHash("0x95")
			},
			wantErr: "prev randao mismatch",
		},
		{
			name: "fee recipient mismatch",
			mutate: func(_ *cltypes.ExecutionPayloadBid, env *cltypes.SignedExecutionPayloadEnvelope) {
				env.Message.Payload.FeeRecipient = common.HexToAddress("0x0000000000000000000000000000000000000094")
			},
			wantErr: "fee recipient mismatch",
		},
		{
			name: "gas limit mismatch",
			mutate: func(_ *cltypes.ExecutionPayloadBid, env *cltypes.SignedExecutionPayloadEnvelope) {
				env.Message.Payload.GasLimit++
			},
			wantErr: "gas limit mismatch",
		},
		{
			name: "slot mismatch",
			mutate: func(_ *cltypes.ExecutionPayloadBid, env *cltypes.SignedExecutionPayloadEnvelope) {
				env.Message.Payload.SlotNumber++
			},
			wantErr: "slot mismatch",
		},
		{
			name: "nil execution requests",
			mutate: func(_ *cltypes.ExecutionPayloadBid, env *cltypes.SignedExecutionPayloadEnvelope) {
				env.Message.ExecutionRequests = nil
			},
			wantErr: "nil execution requests",
		},
		{
			name: "execution requests root mismatch",
			mutate: func(bid *cltypes.ExecutionPayloadBid, _ *cltypes.SignedExecutionPayloadEnvelope) {
				bid.ExecutionRequestsRoot = common.HexToHash("0x93")
			},
			wantErr: "execution requests root mismatch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, st, bid, env, anchorRoot := validAnchorEnvelopeFixture(t, 1)
			tt.mutate(bid, env)
			require.ErrorContains(t, validateAnchorEnvelope(cfg, st, anchorRoot, bid, env), tt.wantErr)
		})
	}
}

func TestAnchorEnvelopeMatches(t *testing.T) {
	_, _, _, env, anchorRoot := validAnchorEnvelopeFixture(t, 1)

	require.True(t, anchorEnvelopeMatches(env, anchorRoot))

	// Server finalized ahead of the local anchor: the HTTP endpoint returns a valid
	// envelope for a newer block, which must be rejected so we fall back to a
	// root-specific P2P request instead of failing anchor validation.
	require.False(t, anchorEnvelopeMatches(env, common.HexToHash("0x99")))
	require.False(t, anchorEnvelopeMatches(nil, anchorRoot))
	require.False(t, anchorEnvelopeMatches(&cltypes.SignedExecutionPayloadEnvelope{}, anchorRoot))
}

func TestVerifyAnchorEnvelopeSignature(t *testing.T) {
	_, st, bid, env, _ := validAnchorEnvelopeFixture(t, 2)
	require.NoError(t, verifyAnchorEnvelopeSignature(st.BeaconConfig(), st, env, bid.Slot))

	cfg, st, bid, env, _ := validAnchorEnvelopeFixture(t, clparams.BuilderIndexSelfBuild)
	require.NoError(t, verifyAnchorEnvelopeSignature(cfg, st, env, bid.Slot))

	t.Run("invalid signature", func(t *testing.T) {
		cfg, st, bid, env, _ := validAnchorEnvelopeFixture(t, 1)
		env.Signature[0] ^= 0x01
		require.Error(t, verifyAnchorEnvelopeSignature(cfg, st, env, bid.Slot))
	})

	t.Run("builder index out of range", func(t *testing.T) {
		cfg, st, bid, env, _ := validAnchorEnvelopeFixture(t, 1)
		env.Message.BuilderIndex = 63
		require.ErrorContains(t, verifyAnchorEnvelopeSignature(cfg, st, env, bid.Slot), "builder index 63 out of range")
	})

	t.Run("nil builders", func(t *testing.T) {
		cfg, st, bid, env, _ := validAnchorEnvelopeFixture(t, 1)
		st.SetBuilders(nil)
		require.ErrorContains(t, verifyAnchorEnvelopeSignature(cfg, st, env, bid.Slot), "builders not found")
	})
}

func TestGloasPayloadHelpers(t *testing.T) {
	require.False(t, validPendingGloasPayload(forkchoice.PendingELPayload{}))
	require.False(t, validPendingGloasPayload(forkchoice.PendingELPayload{Block: &cltypes.SignedBeaconBlock{}}))

	hash, ok := gloasEnvelopePayloadHash(&cltypes.SignedExecutionPayloadEnvelope{})
	require.False(t, ok)
	require.Equal(t, common.Hash{}, hash)

	want := common.HexToHash("0x1234")
	hash, ok = gloasEnvelopePayloadHash(&cltypes.SignedExecutionPayloadEnvelope{
		Message: &cltypes.ExecutionPayloadEnvelope{
			Payload: &cltypes.Eth1Block{BlockHash: want},
		},
	})
	require.True(t, ok)
	require.Equal(t, want, hash)
}

func TestGloasVerificationHeadUsesForkChoiceHead(t *testing.T) {
	want := common.HexToHash("0x22")
	fc := &mock_services.ForkChoiceStorageMock{HeadVal: want}

	got, err := gloasVerificationHeadRoot(fc)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestGloasPayloadValidationRequiresExecutionClient(t *testing.T) {
	require.False(t, canValidateGloasPayloads(&Cfg{}))
	require.True(t, canValidateGloasPayloads(&Cfg{executionClient: &testExecutionEngine{supportInsertion: false}}))
	require.True(t, canValidateGloasPayloads(&Cfg{executionClient: &testExecutionEngine{supportInsertion: true}}))
}

func TestForwardSyncValidatesOnlyWhenInsertionIsUnavailable(t *testing.T) {
	localEL := &Cfg{executionClient: &testExecutionEngine{supportInsertion: true}}
	remoteEL := &Cfg{executionClient: &testExecutionEngine{supportInsertion: false}}

	require.False(t, shouldValidateForwardSyncPayload(localEL, true))
	require.True(t, shouldValidateForwardSyncPayload(remoteEL, false))
	require.False(t, shouldValidateForwardSyncPayload(&Cfg{}, false))
}

func TestValidateAnchorPayloadWithAnyExecutionClient(t *testing.T) {
	cfg, _, bid, env, anchorRoot := validAnchorEnvelopeFixture(t, 1)
	remoteEL := &testExecutionEngine{
		supportInsertion: false,
		payloadStatus:    execution_client.PayloadStatusValidated,
	}

	require.NoError(t, validateAnchorPayloadWithExecutionClient(context.Background(), &Cfg{
		beaconCfg:             cfg,
		executionClient:       remoteEL,
		gloasPayloadValidator: remoteEL,
		forkChoice:            &forkchoice.ForkChoiceStore{},
	}, anchorRoot, bid, env))
	require.Equal(t, 1, remoteEL.newPayloadCalls)

	localEL := &testExecutionEngine{
		supportInsertion: true,
		payloadStatus:    execution_client.PayloadStatusValidated,
	}
	require.NoError(t, validateAnchorPayloadWithExecutionClient(context.Background(), &Cfg{
		beaconCfg:             cfg,
		executionClient:       localEL,
		gloasPayloadValidator: localEL,
		forkChoice:            &forkchoice.ForkChoiceStore{},
	}, anchorRoot, bid, env))
	require.Equal(t, 1, localEL.newPayloadCalls)
}

func TestDrainPendingGloasPayloadsRequeuesNotValidatedPayload(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	blockRoot := common.HexToHash("0x1234")
	parentRoot := common.HexToHash("0x5678")
	execHash := common.HexToHash("0x9abc")
	fc := &forkchoice.ForkChoiceStore{}
	engine := &testExecutionEngine{
		supportInsertion: true,
		payloadStatus:    execution_client.PayloadStatusNotValidated,
	}
	payload := cltypes.NewEth1Block(clparams.GloasVersion, &cfg)
	payload.BlockHash = execHash
	pending := forkchoice.PendingELPayload{
		Block: &cltypes.SignedBeaconBlock{
			Block: &cltypes.BeaconBlock{
				Slot:       1,
				ParentRoot: parentRoot,
				Body: &cltypes.BeaconBody{
					Version: clparams.GloasVersion,
					SignedExecutionPayloadBid: &cltypes.SignedExecutionPayloadBid{
						Message: &cltypes.ExecutionPayloadBid{
							BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48),
						},
					},
				},
			},
		},
		Envelope: &cltypes.SignedExecutionPayloadEnvelope{
			Message: &cltypes.ExecutionPayloadEnvelope{
				BeaconBlockRoot: blockRoot,
				Payload:         payload,
			},
		},
	}
	fc.RequeuePendingELPayload(pending)

	drainPendingGloasPayloads(context.Background(), &Cfg{
		beaconCfg:             &cfg,
		executionClient:       engine,
		gloasPayloadValidator: engine,
		forkChoice:            fc,
	})

	require.Equal(t, 1, engine.newPayloadCalls)
	queued := fc.DrainPendingELPayloads()
	require.Len(t, queued, 1)
	require.Equal(t, blockRoot, queued[0].Envelope.Message.BeaconBlockRoot)
}

func TestDrainPendingGloasPayloadsStopsAfterCancellation(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	engine := &testExecutionEngine{supportInsertion: false}
	engine.newPayloadFn = func(ctx context.Context) (execution_client.PayloadStatus, error) {
		<-ctx.Done()
		return execution_client.PayloadStatusNone, ctx.Err()
	}
	fc := &forkchoice.ForkChoiceStore{}
	for i := byte(1); i <= 3; i++ {
		root := common.Hash{i}
		payload := cltypes.NewEth1Block(clparams.GloasVersion, cfg)
		payload.BlockHash = common.Hash{i + 10}
		envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
		envelope.Message.BeaconBlockRoot = root
		envelope.Message.Payload = payload
		body := cltypes.NewBeaconBody(cfg, clparams.GloasVersion)
		body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{
			BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](0, 48),
		}}
		fc.RequeuePendingELPayload(forkchoice.PendingELPayload{
			Block:    &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Body: body}},
			Envelope: envelope,
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	drainPendingGloasPayloads(ctx, &Cfg{beaconCfg: cfg, executionClient: engine, gloasPayloadValidator: engine, forkChoice: fc})

	require.Equal(t, 1, engine.newPayloadCalls)
	require.Len(t, fc.DrainPendingELPayloads(), 3)
}

func TestGloasPayloadRetryPhasesRotateFirstClass(t *testing.T) {
	calls := [3]int{}
	for offset := range uint32(3) {
		phases := make([]func(context.Context), 3)
		for i := range phases {
			index := i
			phases[i] = func(ctx context.Context) {
				calls[index]++
				<-ctx.Done()
			}
		}
		runGloasPayloadRetryPhases(context.Background(), 20*time.Millisecond, offset, phases...)
	}
	require.Equal(t, [3]int{1, 1, 1}, calls)
}

func validAnchorEnvelopeFixture(t *testing.T, builderIndex uint64) (*clparams.BeaconChainConfig, *state2.CachingBeaconState, *cltypes.ExecutionPayloadBid, *cltypes.SignedExecutionPayloadEnvelope, common.Hash) {
	t.Helper()

	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	cfg.GloasForkEpoch = 0
	cfg.GloasForkVersion = 0x80000038
	cfg.InitializeForkSchedule()

	st := state2.New(&cfg)
	st.SetVersion(clparams.GloasVersion)
	st.SetSlot(64)
	st.SetGenesisValidatorsRoot(common.HexToHash("0x01"))
	st.SetFork(&cltypes.Fork{
		PreviousVersion: utils.Uint32ToBytes4(uint32(cfg.GloasForkVersion)),
		CurrentVersion:  utils.Uint32ToBytes4(uint32(cfg.GloasForkVersion)),
		Epoch:           0,
	})
	st.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{ProposerIndex: 0})

	privKey, err := bls.NewPrivateKeyFromIKM([]byte("01234567890123456789012345678901"))
	require.NoError(t, err)
	pubkey := common.Bytes48(bls.CompressPublicKey(privKey.PublicKey()))

	st.AddValidator(solid.NewValidatorFromParameters(pubkey, common.Hash{}, cfg.MaxEffectiveBalance, false, 0, 0, cfg.FarFutureEpoch, cfg.FarFutureEpoch), cfg.MaxEffectiveBalance)
	builders := solid.NewStaticListSSZ[*cltypes.Builder](64, 73)
	if builderIndex != clparams.BuilderIndexSelfBuild {
		for i := uint64(0); i <= builderIndex; i++ {
			builders.Append(&cltypes.Builder{Pubkey: pubkey})
		}
	}
	st.SetBuilders(builders)

	anchorRoot := common.HexToHash("0x12")
	parentRoot := common.HexToHash("0x11")
	parentHash := common.HexToHash("0x10")
	prevRandao := common.HexToHash("0x13")
	feeRecipient := common.HexToAddress("0x0000000000000000000000000000000000000014")
	requests := cltypes.NewExecutionRequestsWithVersion(&cfg, clparams.GloasVersion)
	requestsRoot, err := requests.HashSSZ()
	require.NoError(t, err)
	requestsHash := cltypes.ComputeExecutionRequestHash(cltypes.GetExecutionRequestsList(&cfg, requests))

	payload := cltypes.NewEth1Block(clparams.GloasVersion, &cfg)
	payload.ParentHash = parentHash
	payload.FeeRecipient = feeRecipient
	payload.PrevRandao = prevRandao
	payload.GasLimit = 30_000_000
	payload.SlotNumber = 64
	payload.Extra = solid.NewExtraData()
	payload.Transactions = &solid.TransactionsSSZ{}
	payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44)
	payload.BlockAccessList = solid.NewByteListSSZ(cfg.MaxBytesPerTransaction)
	payload.BlockHash = anchorPayloadHeaderHash(t, payload, parentRoot, requestsHash)

	bid := &cltypes.ExecutionPayloadBid{
		ParentBlockHash:       parentHash,
		ParentBlockRoot:       parentRoot,
		BlockHash:             payload.BlockHash,
		PrevRandao:            prevRandao,
		FeeRecipient:          feeRecipient,
		GasLimit:              payload.GasLimit,
		BuilderIndex:          builderIndex,
		Slot:                  payload.SlotNumber,
		BlobKzgCommitments:    *solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48),
		ExecutionRequestsRoot: requestsRoot,
	}
	env := &cltypes.SignedExecutionPayloadEnvelope{
		Message: &cltypes.ExecutionPayloadEnvelope{
			Payload:               payload,
			ExecutionRequests:     requests,
			BuilderIndex:          builderIndex,
			BeaconBlockRoot:       anchorRoot,
			ParentBeaconBlockRoot: parentRoot,
		},
	}
	signAnchorEnvelope(t, st, privKey, env, bid.Slot)
	return &cfg, st, bid, env, anchorRoot
}

func signAnchorEnvelope(t *testing.T, st *state2.CachingBeaconState, privKey *bls.PrivateKey, env *cltypes.SignedExecutionPayloadEnvelope, slot uint64) {
	t.Helper()

	domain, err := st.GetDomain(st.BeaconConfig().DomainBeaconBuilder, state2.GetEpochAtSlot(st.BeaconConfig(), slot))
	require.NoError(t, err)
	signingRoot, err := fork.ComputeSigningRoot(env.Message, domain)
	require.NoError(t, err)
	copy(env.Signature[:], privKey.Sign(signingRoot[:]).Bytes())
}

func anchorPayloadHeaderHash(t *testing.T, payload *cltypes.Eth1Block, parentRoot common.Hash, requestsHash common.Hash) common.Hash {
	t.Helper()

	withdrawalsHash := types.DeriveSha(types.Withdrawals(nil))
	blobGasUsed := payload.BlobGasUsed
	excessBlobGas := payload.ExcessBlobGas
	blockAccessListHash := empty.BlockAccessListHash
	slotNumber := payload.SlotNumber
	header := &types.Header{
		ParentHash:            payload.ParentHash,
		UncleHash:             empty.UncleHash,
		Coinbase:              payload.FeeRecipient,
		Root:                  payload.StateRoot,
		TxHash:                types.DeriveSha(types.BinaryTransactions(nil)),
		ReceiptHash:           payload.ReceiptsRoot,
		Bloom:                 payload.LogsBloom,
		Difficulty:            *merge.ProofOfStakeDifficulty,
		GasLimit:              payload.GasLimit,
		GasUsed:               payload.GasUsed,
		Time:                  payload.Time,
		Extra:                 nil,
		MixDigest:             payload.PrevRandao,
		Nonce:                 merge.ProofOfStakeNonce,
		BaseFee:               new(uint256.Int),
		WithdrawalsHash:       &withdrawalsHash,
		ParentBeaconBlockRoot: &parentRoot,
		BlobGasUsed:           &blobGasUsed,
		ExcessBlobGas:         &excessBlobGas,
		RequestsHash:          &requestsHash,
		BlockAccessListHash:   &blockAccessListHash,
		SlotNumber:            &slotNumber,
	}
	header.Number.SetUint64(payload.BlockNumber)
	return header.Hash()
}

type testExecutionEngine struct {
	supportInsertion bool
	payloadStatus    execution_client.PayloadStatus
	newPayloadCalls  int
	newPayloadFn     func(context.Context) (execution_client.PayloadStatus, error)
}

func (t *testExecutionEngine) NewPayload(ctx context.Context, _ *cltypes.Eth1Block, _ *common.Hash, _ []common.Hash, _ []hexutil.Bytes) (execution_client.PayloadStatus, error) {
	t.newPayloadCalls++
	if t.newPayloadFn != nil {
		return t.newPayloadFn(ctx)
	}
	return t.payloadStatus, nil
}

func (t *testExecutionEngine) NewPayloadWithAdmission(ctx context.Context, payload *cltypes.Eth1Block, parentRoot *common.Hash, hashes []common.Hash, requests []hexutil.Bytes) (execution_client.PayloadStatus, error) {
	return t.NewPayload(ctx, payload, parentRoot, hashes, requests)
}

func (t *testExecutionEngine) ForkChoiceUpdate(context.Context, common.Hash, common.Hash, common.Hash, *engine_types.PayloadAttributes, clparams.StateVersion) ([]byte, error) {
	return nil, nil
}

func (t *testExecutionEngine) SupportInsertion() bool { return t.supportInsertion }

func (t *testExecutionEngine) InsertBlocks(context.Context, []*types.Block) error {
	return nil
}

func (t *testExecutionEngine) InsertBlock(context.Context, *types.Block) error { return nil }

func (t *testExecutionEngine) CurrentHeader(context.Context) (*types.Header, error) { return nil, nil }

func (t *testExecutionEngine) IsCanonicalHash(context.Context, common.Hash) (bool, error) {
	return false, nil
}

func (t *testExecutionEngine) Ready(context.Context) (bool, error) { return true, nil }

func (t *testExecutionEngine) GetBodiesByRange(context.Context, uint64, uint64) ([]*types.RawBody, error) {
	return nil, nil
}

func (t *testExecutionEngine) GetBodiesByHashes(context.Context, []common.Hash) ([]*types.RawBody, error) {
	return nil, nil
}

func (t *testExecutionEngine) HasBlock(context.Context, common.Hash) (bool, error) { return false, nil }

func (t *testExecutionEngine) FrozenBlocks(context.Context) uint64 { return 0 }

func (t *testExecutionEngine) HasGapInSnapshots(context.Context) bool { return false }

func (t *testExecutionEngine) GetAssembledBlock(context.Context, []byte, clparams.StateVersion) (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
	return nil, nil, nil, nil, nil
}

func (t *testExecutionEngine) GetBlobs(context.Context, []common.Hash, clparams.StateVersion) ([][]byte, [][][]byte, error) {
	return nil, nil, nil
}

func (t *testExecutionEngine) GetClientVersionV1(context.Context, *engine_types.ClientVersionV1) ([]engine_types.ClientVersionV1, error) {
	return nil, nil
}

var _ execution_client.ExecutionEngine = (*testExecutionEngine)(nil)
