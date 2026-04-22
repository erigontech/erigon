package epbs

import (
	"context"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

var testBuilderIndex = uint64(42)

var testGenesisValidatorsRoot = common.HexToHash("0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95")

func newTestManager(t *testing.T) (*BuilderManager, *bls.PrivateKey) {
	t.Helper()

	privKey, err := bls.GenerateKey()
	require.NoError(t, err)

	signer, err := NewLocalSignerFromBytes(privKey.Bytes())
	require.NoError(t, err)

	cfg := clparams.MainnetBeaconConfig
	mgr := NewBuilderManager(signer, &testBuilderIndex, &cfg, testGenesisValidatorsRoot)
	return mgr, privKey
}

// verifySignature is a test helper that manually recomputes the signing root
// and verifies a BLS signature, matching the verification logic in
// cl/transition/impl/eth2/operations.go:verifyExecutionPayloadBidSignature.
func verifySignature(t *testing.T, sig common.Bytes96, pk common.Bytes48, bid *cltypes.ExecutionPayloadBid) {
	t.Helper()

	cfg := clparams.MainnetBeaconConfig
	epoch := bid.Slot / cfg.SlotsPerEpoch
	stateVersion := cfg.GetCurrentStateVersion(epoch)
	forkVersion := utils.Uint32ToBytes4(cfg.GetForkVersionByVersion(stateVersion))
	domain, err := fork.ComputeDomain(cfg.DomainBeaconBuilder[:], forkVersion, testGenesisValidatorsRoot)
	require.NoError(t, err)

	signingRoot, err := fork.ComputeSigningRoot(bid, domain)
	require.NoError(t, err)

	valid, err := bls.Verify(sig[:], signingRoot[:], pk[:])
	require.NoError(t, err)
	require.True(t, valid, "BLS signature must verify")
}

func TestBuilderManager_DomainBeaconBuilder(t *testing.T) {
	// Verify DomainBeaconBuilder is 0x0B000000.
	cfg := clparams.MainnetBeaconConfig
	require.Equal(t, common.Bytes4{0x0B, 0x00, 0x00, 0x00}, cfg.DomainBeaconBuilder)
}

func TestBuilderManager_SignBid(t *testing.T) {
	mgr, _ := newTestManager(t)
	ctx := context.Background()

	bid := &cltypes.ExecutionPayloadBid{
		ParentBlockHash:       common.HexToHash("0xaaaa"),
		ParentBlockRoot:       common.HexToHash("0xbbbb"),
		BlockHash:             common.HexToHash("0xcccc"),
		PrevRandao:            common.HexToHash("0xdddd"),
		FeeRecipient:          common.HexToAddress("0xee"),
		GasLimit:              30000000,
		Slot:                  100,
		Value:                 1000,
		ExecutionPayment:      500,
		BlobKzgCommitments:    *solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48),
		ExecutionRequestsRoot: common.HexToHash("0xff"),
	}

	signed, err := mgr.SignBid(ctx, bid)
	require.NoError(t, err)
	require.NotNil(t, signed)
	require.Equal(t, bid, signed.Message)

	// Verify the signature using independent domain + signing root computation.
	verifySignature(t, signed.Signature, mgr.Pubkey(), bid)
}

func TestBuilderManager_SignEnvelope(t *testing.T) {
	mgr, _ := newTestManager(t)
	ctx := context.Background()
	cfg := clparams.MainnetBeaconConfig

	envelope := cltypes.NewExecutionPayloadEnvelope(&cfg)
	envelope.BeaconBlockRoot = common.HexToHash("0xabcd")
	// Initialize nil inner fields so HashSSZ doesn't panic.
	envelope.Payload.Transactions = &solid.TransactionsSSZ{}
	envelope.Payload.Extra = solid.NewExtraData()
	envelope.Payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44)

	slot := uint64(200)
	signed, err := mgr.SignEnvelope(ctx, envelope, slot)
	require.NoError(t, err)
	require.NotNil(t, signed)
	require.Equal(t, envelope, signed.Message)

	// Verify signature.
	epoch := slot / cfg.SlotsPerEpoch
	stateVersion := cfg.GetCurrentStateVersion(epoch)
	forkVersion := utils.Uint32ToBytes4(cfg.GetForkVersionByVersion(stateVersion))
	domain, err := fork.ComputeDomain(cfg.DomainBeaconBuilder[:], forkVersion, testGenesisValidatorsRoot)
	require.NoError(t, err)

	signingRoot, err := fork.ComputeSigningRoot(envelope, domain)
	require.NoError(t, err)

	pk := mgr.Pubkey()
	valid, err := bls.Verify(signed.Signature[:], signingRoot[:], pk[:])
	require.NoError(t, err)
	require.True(t, valid, "envelope signature must verify")
}

func TestBuilderManager_SignBid_WrongKey_Fails(t *testing.T) {
	mgr, _ := newTestManager(t)
	ctx := context.Background()

	bid := &cltypes.ExecutionPayloadBid{
		Slot:               64,
		BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48),
	}

	signed, err := mgr.SignBid(ctx, bid)
	require.NoError(t, err)

	// Verify with a different key — should fail.
	otherKey, err := bls.GenerateKey()
	require.NoError(t, err)
	otherPK := bls.CompressPublicKey(otherKey.PublicKey())

	cfg := clparams.MainnetBeaconConfig
	epoch := bid.Slot / cfg.SlotsPerEpoch
	stateVersion := cfg.GetCurrentStateVersion(epoch)
	forkVersion := utils.Uint32ToBytes4(cfg.GetForkVersionByVersion(stateVersion))
	domain, err := fork.ComputeDomain(cfg.DomainBeaconBuilder[:], forkVersion, testGenesisValidatorsRoot)
	require.NoError(t, err)

	signingRoot, err := fork.ComputeSigningRoot(bid, domain)
	require.NoError(t, err)

	valid, err := bls.Verify(signed.Signature[:], signingRoot[:], otherPK)
	require.NoError(t, err)
	require.False(t, valid, "signature should not verify against a different key")
}

func TestBuilderManager_DomainUsesCorrectForkVersion(t *testing.T) {
	mgr, _ := newTestManager(t)
	ctx := context.Background()

	bid := &cltypes.ExecutionPayloadBid{
		Slot:               0,
		BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48),
	}

	signed0, err := mgr.SignBid(ctx, bid)
	require.NoError(t, err)

	// Far future slot to get a different fork version.
	bid2 := *bid
	bid2.Slot = 1_000_000
	signed1M, err := mgr.SignBid(ctx, &bid2)
	require.NoError(t, err)

	// If fork versions differ, signatures differ (different domain → different signing root).
	cfg := clparams.MainnetBeaconConfig
	v0 := cfg.GetCurrentStateVersion(0)
	v1 := cfg.GetCurrentStateVersion(1_000_000 / cfg.SlotsPerEpoch)
	if v0 != v1 {
		require.NotEqual(t, signed0.Signature, signed1M.Signature,
			"different fork versions should produce different signatures")
	}

	// Both must verify against their respective signing roots.
	verifySignature(t, signed0.Signature, mgr.Pubkey(), bid)
	verifySignature(t, signed1M.Signature, mgr.Pubkey(), &bid2)
}

func TestBuilderManager_Pubkey(t *testing.T) {
	mgr, privKey := newTestManager(t)
	expected := bls.CompressPublicKey(privKey.PublicKey())
	require.Equal(t, common.Bytes48(expected), mgr.Pubkey())
}

func TestBuilderManager_BuilderIndex(t *testing.T) {
	mgr, _ := newTestManager(t)
	idx, ok := mgr.BuilderIndex()
	require.True(t, ok)
	require.Equal(t, testBuilderIndex, idx)
}

func TestBuilderManager_SignBid_StampsBuilderIndex(t *testing.T) {
	mgr, _ := newTestManager(t)
	ctx := context.Background()

	// Caller passes a bid with a WRONG BuilderIndex (0, not 42).
	bid := &cltypes.ExecutionPayloadBid{
		Slot:               100,
		BuilderIndex:       0, // intentionally wrong
		BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48),
	}

	signed, err := mgr.SignBid(ctx, bid)
	require.NoError(t, err)

	// The manager must have stamped its own index.
	require.Equal(t, testBuilderIndex, signed.Message.BuilderIndex,
		"SignBid must overwrite BuilderIndex to the manager's own index")

	// The signature must verify against the stamped message (index=42).
	verifySignature(t, signed.Signature, mgr.Pubkey(), signed.Message)
}

func TestBuilderManager_SignEnvelope_StampsBuilderIndex(t *testing.T) {
	mgr, _ := newTestManager(t)
	ctx := context.Background()
	cfg := clparams.MainnetBeaconConfig

	envelope := cltypes.NewExecutionPayloadEnvelope(&cfg)
	envelope.BuilderIndex = 999 // intentionally wrong
	envelope.BeaconBlockRoot = common.HexToHash("0xabcd")
	envelope.Payload.Transactions = &solid.TransactionsSSZ{}
	envelope.Payload.Extra = solid.NewExtraData()
	envelope.Payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44)

	slot := uint64(200)
	signed, err := mgr.SignEnvelope(ctx, envelope, slot)
	require.NoError(t, err)

	// The manager must have stamped its own index.
	require.Equal(t, testBuilderIndex, signed.Message.BuilderIndex,
		"SignEnvelope must overwrite BuilderIndex to the manager's own index")

	// Verify the signature against the stamped envelope.
	epoch := slot / cfg.SlotsPerEpoch
	stateVersion := cfg.GetCurrentStateVersion(epoch)
	forkVersion := utils.Uint32ToBytes4(cfg.GetForkVersionByVersion(stateVersion))
	domain, err := fork.ComputeDomain(cfg.DomainBeaconBuilder[:], forkVersion, testGenesisValidatorsRoot)
	require.NoError(t, err)

	signingRoot, err := fork.ComputeSigningRoot(signed.Message, domain)
	require.NoError(t, err)

	pk := mgr.Pubkey()
	valid, err := bls.Verify(signed.Signature[:], signingRoot[:], pk[:])
	require.NoError(t, err)
	require.True(t, valid, "envelope signature must verify after index stamping")
}

// depositStub is a minimal ssz.HashableSSZ for testing SignDeposit.
type depositStub struct{ root common.Hash }

func (d *depositStub) HashSSZ() ([32]byte, error) { return d.root, nil }

func TestBuilderManager_SignDeposit(t *testing.T) {
	mgr, _ := newTestManager(t)
	ctx := context.Background()

	stub := &depositStub{root: common.HexToHash("0xdeposit")}
	sig, err := mgr.SignDeposit(ctx, stub, 100)
	require.NoError(t, err)

	// Manually compute expected signing root.
	cfg := clparams.MainnetBeaconConfig
	epoch := uint64(100) / cfg.SlotsPerEpoch
	stateVersion := cfg.GetCurrentStateVersion(epoch)
	forkVersion := utils.Uint32ToBytes4(cfg.GetForkVersionByVersion(stateVersion))
	domain, err := fork.ComputeDomain(cfg.DomainBeaconBuilder[:], forkVersion, testGenesisValidatorsRoot)
	require.NoError(t, err)

	signingRoot, err := fork.ComputeSigningRoot(stub, domain)
	require.NoError(t, err)

	pk := mgr.Pubkey()
	valid, err := bls.Verify(sig[:], signingRoot[:], pk[:])
	require.NoError(t, err)
	require.True(t, valid, "deposit signature must verify")
}
