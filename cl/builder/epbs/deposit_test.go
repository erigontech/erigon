package epbs

import (
	"context"
	"errors"
	"testing"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

var testFeeRecipient = common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678")

// ---------------------------------------------------------------------------
// BuildWithdrawalCredentials
// ---------------------------------------------------------------------------

func TestBuildWithdrawalCredentials_PrefixIs0x03(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	creds := BuildWithdrawalCredentials(testFeeRecipient, &cfg)

	require.Equal(t, byte(cfg.BuilderWithdrawalPrefix), creds[0],
		"first byte must be BuilderWithdrawalPrefix (0x03)")
	require.Equal(t, byte(0x03), creds[0])
}

func TestBuildWithdrawalCredentials_ZeroPadding(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	creds := BuildWithdrawalCredentials(testFeeRecipient, &cfg)

	// Bytes 1..11 must be zero.
	for i := 1; i < 12; i++ {
		require.Equal(t, byte(0), creds[i], "byte %d must be zero", i)
	}
}

func TestBuildWithdrawalCredentials_AddressEmbedded(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	creds := BuildWithdrawalCredentials(testFeeRecipient, &cfg)

	// Bytes 12..31 must equal the fee recipient address.
	var addr common.Address
	copy(addr[:], creds[12:32])
	require.Equal(t, testFeeRecipient, addr)
}

// ---------------------------------------------------------------------------
// BuildDepositData
// ---------------------------------------------------------------------------

func TestBuildDepositData_Fields(t *testing.T) {
	privKey, err := bls.GenerateKey()
	require.NoError(t, err)

	signer, err := NewLocalSignerFromBytes(privKey.Bytes())
	require.NoError(t, err)

	cfg := clparams.MainnetBeaconConfig
	amount := cfg.MinDepositAmount // 1 ETH = 1e9 Gwei

	dd, err := BuildDepositData(context.Background(), signer, testFeeRecipient, amount, &cfg)
	require.NoError(t, err)

	// Pubkey matches signer.
	require.Equal(t, signer.Pubkey(), dd.PubKey)

	// Amount matches.
	require.Equal(t, amount, dd.Amount)

	// Withdrawal credentials have 0x03 prefix.
	require.Equal(t, byte(0x03), dd.WithdrawalCredentials[0],
		"withdrawal_credentials[0] must be 0x03 (BuilderWithdrawalPrefix)")

	// Execution address embedded correctly.
	var addr common.Address
	copy(addr[:], dd.WithdrawalCredentials[12:32])
	require.Equal(t, testFeeRecipient, addr)
}

func TestBuildDepositData_SignatureVerifiesWithDomainDeposit(t *testing.T) {
	privKey, err := bls.GenerateKey()
	require.NoError(t, err)

	signer, err := NewLocalSignerFromBytes(privKey.Bytes())
	require.NoError(t, err)

	cfg := clparams.MainnetBeaconConfig

	dd, err := BuildDepositData(context.Background(), signer, testFeeRecipient, cfg.MinDepositAmount, &cfg)
	require.NoError(t, err)

	// Verify using the same logic as state.IsValidDepositSignature:
	// domain = ComputeDomain(DomainDeposit, genesisForkVersion, [32]byte{})
	domain, err := fork.ComputeDomain(
		cfg.DomainDeposit[:],
		utils.Uint32ToBytes4(uint32(cfg.GenesisForkVersion)),
		[32]byte{},
	)
	require.NoError(t, err)

	messageRoot, err := dd.MessageHash()
	require.NoError(t, err)

	signingRoot := utils.Sha256(messageRoot[:], domain)

	valid, err := bls.Verify(dd.Signature[:], signingRoot[:], dd.PubKey[:])
	require.NoError(t, err)
	require.True(t, valid, "deposit signature must verify with DomainDeposit")
}

func TestBuildDepositData_DoesNotVerifyWithDomainBeaconBuilder(t *testing.T) {
	privKey, err := bls.GenerateKey()
	require.NoError(t, err)

	signer, err := NewLocalSignerFromBytes(privKey.Bytes())
	require.NoError(t, err)

	cfg := clparams.MainnetBeaconConfig

	dd, err := BuildDepositData(context.Background(), signer, testFeeRecipient, cfg.MinDepositAmount, &cfg)
	require.NoError(t, err)

	// If we verify using DomainBeaconBuilder instead, it must fail.
	wrongDomain, err := fork.ComputeDomain(
		cfg.DomainBeaconBuilder[:],
		utils.Uint32ToBytes4(uint32(cfg.GenesisForkVersion)),
		[32]byte{},
	)
	require.NoError(t, err)

	messageRoot, err := dd.MessageHash()
	require.NoError(t, err)

	wrongSigningRoot := utils.Sha256(messageRoot[:], wrongDomain)

	valid, err := bls.Verify(dd.Signature[:], wrongSigningRoot[:], dd.PubKey[:])
	require.NoError(t, err)
	require.False(t, valid,
		"deposit signature must NOT verify with DomainBeaconBuilder — deposits use DomainDeposit")
}

func TestBuildDepositData_MatchesIsValidDepositSignature(t *testing.T) {
	privKey, err := bls.GenerateKey()
	require.NoError(t, err)

	signer, err := NewLocalSignerFromBytes(privKey.Bytes())
	require.NoError(t, err)

	cfg := clparams.MainnetBeaconConfig

	dd, err := BuildDepositData(context.Background(), signer, testFeeRecipient, cfg.MinDepositAmount, &cfg)
	require.NoError(t, err)

	// Use the exact same verifier used on-chain: state.IsValidDepositSignature.
	// This is in cl/phase1/core/state/epbs.go.
	valid, err := state.IsValidDepositSignature(&cfg, dd.PubKey, dd.WithdrawalCredentials, dd.Amount, dd.Signature)
	require.NoError(t, err)
	require.True(t, valid, "deposit must pass IsValidDepositSignature (the on-chain verifier)")
}

// ---------------------------------------------------------------------------
// ResolveIndex
// ---------------------------------------------------------------------------

// mockSyncedData is a minimal mock for synced_data.SyncedData that only
// implements ViewHeadState — the only method ResolveIndex uses.
type mockSyncedData struct {
	state *state.CachingBeaconState
	err   error
}

func (m *mockSyncedData) OnHeadState(_ *state.CachingBeaconState) error { return nil }
func (m *mockSyncedData) UnsetHeadState()                               {}
func (m *mockSyncedData) ViewHeadState(fn synced_data.ViewHeadStateFn) error {
	if m.err != nil {
		return m.err
	}
	if m.state != nil {
		return fn(m.state)
	}
	return nil
}
func (m *mockSyncedData) ViewPreviousHeadState(synced_data.ViewHeadStateFn) error {
	return nil
}
func (m *mockSyncedData) Syncing() bool                { return false }
func (m *mockSyncedData) HeadSlot() uint64             { return 0 }
func (m *mockSyncedData) HeadRoot() common.Hash        { return common.Hash{} }
func (m *mockSyncedData) CommitteeCount(uint64) uint64 { return 0 }
func (m *mockSyncedData) ValidatorPublicKeyByIndex(int) (common.Bytes48, error) {
	return common.Bytes48{}, nil
}
func (m *mockSyncedData) ValidatorIndexByPublicKey(common.Bytes48) (uint64, bool, error) {
	return 0, false, nil
}
func (m *mockSyncedData) HistoricalRootElementAtIndex(int) (common.Hash, error) {
	return common.Hash{}, nil
}
func (m *mockSyncedData) HistoricalSummaryElementAtIndex(int) (*cltypes.HistoricalSummary, error) {
	return nil, nil
}

func TestResolveIndex_Found(t *testing.T) {
	privKey, err := bls.GenerateKey()
	require.NoError(t, err)

	signer, err := NewLocalSignerFromBytes(privKey.Bytes())
	require.NoError(t, err)

	cfg := clparams.MainnetBeaconConfig
	mgr := NewBuilderManager(signer, nil, &cfg, testGenesisValidatorsRoot)

	// Build a state with builders.
	builders := solid.NewStaticListSSZ[*cltypes.Builder](64, 73)
	// Add a dummy builder at index 0.
	builders.Append(&cltypes.Builder{
		Pubkey:  common.Bytes48{0x01},
		Balance: 1e9,
	})
	// Add our builder at index 1.
	builders.Append(&cltypes.Builder{
		Pubkey:  signer.Pubkey(),
		Balance: 2e9,
	})

	s := state.New(&cfg)
	s.SetBuilders(builders)

	sd := &mockSyncedData{state: s}
	idx, found, err := mgr.ResolveIndex(sd)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(1), idx)
}

func TestResolveIndex_NotFound(t *testing.T) {
	privKey, err := bls.GenerateKey()
	require.NoError(t, err)

	signer, err := NewLocalSignerFromBytes(privKey.Bytes())
	require.NoError(t, err)

	cfg := clparams.MainnetBeaconConfig
	mgr := NewBuilderManager(signer, nil, &cfg, testGenesisValidatorsRoot)

	// State with no matching builder.
	builders := solid.NewStaticListSSZ[*cltypes.Builder](64, 73)
	builders.Append(&cltypes.Builder{
		Pubkey:  common.Bytes48{0xFF},
		Balance: 1e9,
	})

	s := state.New(&cfg)
	s.SetBuilders(builders)

	sd := &mockSyncedData{state: s}
	_, found, err := mgr.ResolveIndex(sd)
	require.NoError(t, err)
	require.False(t, found)
}

func TestResolveIndex_NilBuilders(t *testing.T) {
	privKey, err := bls.GenerateKey()
	require.NoError(t, err)

	signer, err := NewLocalSignerFromBytes(privKey.Bytes())
	require.NoError(t, err)

	cfg := clparams.MainnetBeaconConfig
	mgr := NewBuilderManager(signer, nil, &cfg, testGenesisValidatorsRoot)

	// State with nil builders (not set).
	s := state.New(&cfg)

	sd := &mockSyncedData{state: s}
	_, found, err := mgr.ResolveIndex(sd)
	require.NoError(t, err)
	require.False(t, found)
}

func TestResolveIndex_ViewHeadStateError(t *testing.T) {
	privKey, err := bls.GenerateKey()
	require.NoError(t, err)

	signer, err := NewLocalSignerFromBytes(privKey.Bytes())
	require.NoError(t, err)

	cfg := clparams.MainnetBeaconConfig
	mgr := NewBuilderManager(signer, nil, &cfg, testGenesisValidatorsRoot)

	sd := &mockSyncedData{err: errors.New("not synced")}
	_, _, err = mgr.ResolveIndex(sd)
	require.Error(t, err)
	require.Contains(t, err.Error(), "resolve index")
}
