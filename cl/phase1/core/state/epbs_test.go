package state_test

import (
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	state2 "github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

// TestIsBuilderWithdrawalCredential_0x03 verifies that withdrawal credentials
// with the 0x03 prefix are recognised as builder credentials.
func TestIsBuilderWithdrawalCredential_0x03(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig

	var creds common.Hash
	creds[0] = 0x03
	addr := common.HexToAddress("0xdeadbeef")
	copy(creds[12:], addr[:])

	require.True(t, state2.IsBuilderWithdrawalCredential(creds, &cfg),
		"0x03 prefix must be recognised as builder withdrawal credential")
}

// TestIsBuilderWithdrawalCredential_NotBuilder tests that non-0x03 prefixes
// are not classified as builder credentials.
func TestIsBuilderWithdrawalCredential_NotBuilder(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig

	for _, prefix := range []byte{0x00, 0x01, 0x02, 0x04, 0xFF} {
		var creds common.Hash
		creds[0] = prefix
		require.False(t, state2.IsBuilderWithdrawalCredential(creds, &cfg),
			"prefix 0x%02x must NOT be classified as builder credential", prefix)
	}
}

func TestGetProposerDependentRoot(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.SlotsPerEpoch = 32
	cfg.SlotsPerHistoricalRoot = 8192
	cfg.MinSeedLookahead = 1
	s := state2.New(&cfg)
	s.SetSlot(100)
	want := common.Hash{0x42}
	s.SetBlockRootAt(63, want)

	got, err := state2.GetProposerDependentRoot(s, 3)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestGetProposerDependentRootRejectsUnderflow(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.SlotsPerEpoch = 32
	cfg.MinSeedLookahead = 1
	s := state2.New(&cfg)
	s.SetSlot(100)

	_, err := state2.GetProposerDependentRoot(s, 0)
	require.Error(t, err)

	_, err = state2.GetProposerDependentRoot(s, 1)
	require.Error(t, err)
}

// TestApplyDepositForBuilder_NewBuilder_WithValidSignature verifies that a
// new builder deposit with 0x03 credentials and a valid signature creates
// a builder entry in the state registry.
//
// This covers the routing path: deposit with 0x03 prefix → ApplyDepositForBuilder
// → IsValidDepositSignature → AddBuilderToRegistry.
func TestApplyDepositForBuilder_NewBuilder_WithValidSignature(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig

	s := state2.New(&cfg)
	builders := solid.NewStaticListSSZ[*cltypes.Builder](64, 73)
	s.SetBuilders(builders)

	pubkey, creds, amount, sig := makeValidBuilderDeposit(t, &cfg)

	// Pre-conditions.
	require.Equal(t, byte(0x03), creds[0])
	require.True(t, state2.IsBuilderWithdrawalCredential(creds, &cfg))

	slot := uint64(100)
	state2.ApplyDepositForBuilder(s, pubkey, creds, amount, sig, slot)

	// Post-condition: builder was added.
	newBuilders := s.GetBuilders()
	require.Equal(t, 1, newBuilders.Len(), "exactly one builder should be registered")

	b := newBuilders.Get(0)
	require.Equal(t, pubkey, b.Pubkey)
	require.Equal(t, byte(0x03), b.Version, "builder Version must match creds[0]")
	require.Equal(t, common.BytesToAddress(creds[12:]), b.ExecutionAddress)
	require.Equal(t, amount, b.Balance)
}

// TestApplyDepositForBuilder_TopUp verifies that depositing to an existing
// builder increases its balance instead of creating a duplicate.
func TestApplyDepositForBuilder_TopUp(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig

	s := state2.New(&cfg)
	builders := solid.NewStaticListSSZ[*cltypes.Builder](64, 73)

	feeRecipient := common.HexToAddress("0xaaaa")

	pubkey, creds, _, _ := makeValidBuilderDeposit(t, &cfg)
	builders.Append(&cltypes.Builder{
		Pubkey:            pubkey,
		Version:           creds[0],
		ExecutionAddress:  feeRecipient,
		Balance:           1e9,
		DepositEpoch:      0,
		WithdrawableEpoch: cfg.FarFutureEpoch,
	})
	s.SetBuilders(builders)

	topUpAmount := uint64(2e9)
	// Signature is not checked for existing builders — zero sig is fine.
	state2.ApplyDepositForBuilder(s, pubkey, creds, topUpAmount, common.Bytes96{}, 200)

	updatedBuilders := s.GetBuilders()
	require.Equal(t, 1, updatedBuilders.Len(), "no new builder should be created on top-up")
	require.Equal(t, uint64(1e9)+topUpAmount, updatedBuilders.Get(0).Balance)
}

// TestApplyDepositForBuilder_InvalidSignature_Ignored verifies that a new
// builder deposit with an invalid signature is silently ignored.
func TestApplyDepositForBuilder_InvalidSignature_Ignored(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig

	s := state2.New(&cfg)
	builders := solid.NewStaticListSSZ[*cltypes.Builder](64, 73)
	s.SetBuilders(builders)

	var (
		pubkey common.Bytes48
		creds  common.Hash
	)
	pubkey[0] = 0xAA
	creds[0] = 0x03

	state2.ApplyDepositForBuilder(s, pubkey, creds, 1e9, common.Bytes96{}, 0)

	require.Equal(t, 0, s.GetBuilders().Len(),
		"invalid signature should prevent builder registration")
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func makeValidBuilderDeposit(t *testing.T, cfg *clparams.BeaconChainConfig) (
	pubkey common.Bytes48,
	withdrawalCredentials common.Hash,
	amount uint64,
	signature common.Bytes96,
) {
	t.Helper()

	privKey, err := bls.GenerateKey()
	require.NoError(t, err)

	compressed := bls.CompressPublicKey(privKey.PublicKey())
	copy(pubkey[:], compressed)

	feeRecipient := common.HexToAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

	// Build withdrawal credentials: 0x03 + 11 zero bytes + 20-byte address.
	withdrawalCredentials[0] = byte(cfg.BuilderWithdrawalPrefix)
	copy(withdrawalCredentials[12:], feeRecipient[:])

	amount = cfg.MinDepositAmount // 1e9 Gwei

	// Compute deposit signing root matching IsValidDepositSignature.
	dd := &cltypes.DepositData{
		PubKey:                pubkey,
		WithdrawalCredentials: withdrawalCredentials,
		Amount:                amount,
	}

	msgHash, err := dd.MessageHash()
	require.NoError(t, err)

	domain, err := fork.ComputeDomain(
		cfg.DomainDeposit[:],
		utils.Uint32ToBytes4(uint32(cfg.GenesisForkVersion)),
		[32]byte{},
	)
	require.NoError(t, err)

	signingRoot := utils.Sha256(msgHash[:], domain)

	sigObj := privKey.Sign(signingRoot[:])
	copy(signature[:], sigObj.Bytes())

	return pubkey, withdrawalCredentials, amount, signature
}
