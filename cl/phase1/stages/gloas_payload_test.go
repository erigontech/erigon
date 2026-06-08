package stages

import (
	"context"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	state2 "github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
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

func TestStandaloneExecutionClientDoesNotRunLocalGloasRetry(t *testing.T) {
	require.False(t, canRetryGloasPayloads(&Cfg{}))
	require.False(t, canRetryGloasPayloads(&Cfg{executionClient: &testExecutionEngine{supportInsertion: false}}))
	require.True(t, canRetryGloasPayloads(&Cfg{executionClient: &testExecutionEngine{supportInsertion: true}}))
}

func TestValidateAnchorPayloadIfLocalELFollowsSupportInsertion(t *testing.T) {
	cfg, _, bid, env, anchorRoot := validAnchorEnvelopeFixture(t, 1)
	remoteEL := &testExecutionEngine{
		supportInsertion: false,
		payloadStatus:    execution_client.PayloadStatusInvalidated,
	}

	require.NoError(t, validateAnchorPayloadIfLocalEL(context.Background(), &Cfg{
		beaconCfg:       cfg,
		executionClient: remoteEL,
		forkChoice:      &forkchoice.ForkChoiceStore{},
	}, anchorRoot, bid, env))
	require.Equal(t, 0, remoteEL.newPayloadCalls)

	localEL := &testExecutionEngine{
		supportInsertion: true,
		payloadStatus:    execution_client.PayloadStatusValidated,
	}
	require.NoError(t, validateAnchorPayloadIfLocalEL(context.Background(), &Cfg{
		beaconCfg:       cfg,
		executionClient: localEL,
		forkChoice:      &forkchoice.ForkChoiceStore{},
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
		beaconCfg:       &cfg,
		executionClient: engine,
		forkChoice:      fc,
	})

	require.Equal(t, 1, engine.newPayloadCalls)
	queued := fc.DrainPendingELPayloads()
	require.Len(t, queued, 1)
	require.Equal(t, blockRoot, queued[0].Envelope.Message.BeaconBlockRoot)
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
	requests := cltypes.NewExecutionRequests(&cfg)
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
}

func (t *testExecutionEngine) NewPayload(context.Context, *cltypes.Eth1Block, *common.Hash, []common.Hash, []hexutil.Bytes) (execution_client.PayloadStatus, error) {
	t.newPayloadCalls++
	return t.payloadStatus, nil
}

func (t *testExecutionEngine) ForkChoiceUpdate(context.Context, common.Hash, common.Hash, common.Hash, *engine_types.PayloadAttributes, clparams.StateVersion) ([]byte, error) {
	return nil, nil
}

func (t *testExecutionEngine) SupportInsertion() bool { return t.supportInsertion }

func (t *testExecutionEngine) InsertBlocks(context.Context, []*types.Block, bool) error { return nil }

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

var _ execution_client.ExecutionEngine = (*testExecutionEngine)(nil)
