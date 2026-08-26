package epbs

import (
	"context"
	"errors"
	"testing"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

var testFeeRecipient = common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678")

// ---------------------------------------------------------------------------
// BuildWithdrawalCredentials
// ---------------------------------------------------------------------------

func TestBuildWithdrawalCredentials_UsesConfiguredBuilderPrefix(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	creds := BuildWithdrawalCredentials(testFeeRecipient, &cfg)

	require.Equal(t, byte(cfg.BuilderWithdrawalPrefix), creds[0])
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
// BuildBuilderDepositRequest
// ---------------------------------------------------------------------------

func TestBuildBuilderDepositRequest_Fields(t *testing.T) {
	privKey, err := bls.GenerateKey()
	require.NoError(t, err)

	signer, err := NewLocalSignerFromBytes(privKey.Bytes())
	require.NoError(t, err)

	cfg := clparams.MainnetBeaconConfig
	amount := cfg.MinDepositAmount // 1 ETH = 1e9 Gwei

	request, err := BuildBuilderDepositRequest(context.Background(), signer, testFeeRecipient, amount, &cfg)
	require.NoError(t, err)

	require.Equal(t, signer.Pubkey(), request.PubKey)
	require.Equal(t, amount, request.Amount)
	require.Equal(t, byte(cfg.BuilderWithdrawalPrefix), request.WithdrawalCredentials[0])

	var addr common.Address
	copy(addr[:], request.WithdrawalCredentials[12:32])
	require.Equal(t, testFeeRecipient, addr)
}

func TestBuildBuilderDepositRequest_SignatureVerifiesWithBuilderDomain(t *testing.T) {
	privKey, err := bls.GenerateKey()
	require.NoError(t, err)

	signer, err := NewLocalSignerFromBytes(privKey.Bytes())
	require.NoError(t, err)

	cfg := clparams.MainnetBeaconConfig

	request, err := BuildBuilderDepositRequest(context.Background(), signer, testFeeRecipient, cfg.MinDepositAmount, &cfg)
	require.NoError(t, err)

	valid, err := state.IsValidBuilderDepositSignature(&cfg, request)
	require.NoError(t, err)
	require.True(t, valid)
}

func TestBuildBuilderDepositRequest_DoesNotVerifyAsValidatorDeposit(t *testing.T) {
	privKey, err := bls.GenerateKey()
	require.NoError(t, err)

	signer, err := NewLocalSignerFromBytes(privKey.Bytes())
	require.NoError(t, err)

	cfg := clparams.MainnetBeaconConfig

	request, err := BuildBuilderDepositRequest(context.Background(), signer, testFeeRecipient, cfg.MinDepositAmount, &cfg)
	require.NoError(t, err)

	valid, err := state.IsValidDepositSignature(&cfg, request.PubKey, request.WithdrawalCredentials, request.Amount, request.Signature)
	require.NoError(t, err)
	require.False(t, valid)
}

func TestBuildBuilderDepositRequest_RejectsInsufficientAmount(t *testing.T) {
	privKey, err := bls.GenerateKey()
	require.NoError(t, err)
	signer, err := NewLocalSignerFromBytes(privKey.Bytes())
	require.NoError(t, err)
	cfg := clparams.MainnetBeaconConfig

	_, err = BuildBuilderDepositRequest(context.Background(), signer, testFeeRecipient, cfg.MinDepositAmount-1, &cfg)
	require.ErrorContains(t, err, "minimum")
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
func (m *mockSyncedData) OnHeadStateWithBlockRoot(_ *state.CachingBeaconState, _ common.Hash) error {
	return nil
}
func (m *mockSyncedData) UnsetHeadState() {}
func (m *mockSyncedData) SelectedHead() (common.Hash, uint64, bool) {
	return common.Hash{}, 0, false
}
func (m *mockSyncedData) StateHead() (common.Hash, uint64, bool) {
	return common.Hash{}, 0, false
}
func (m *mockSyncedData) ViewHeadState(fn synced_data.ViewHeadStateFn) error {
	if m.err != nil {
		return m.err
	}
	if m.state != nil {
		return fn(m.state)
	}
	return nil
}
func (m *mockSyncedData) ViewHeadStateWithIdentity(fn synced_data.ViewHeadStateWithIdentityFn) error {
	if m.err != nil {
		return m.err
	}
	if m.state != nil {
		return fn(m.state, common.Hash{}, 0)
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

func TestResolveIndexSkipsNilBuilderEntry(t *testing.T) {
	privKey, err := bls.GenerateKey()
	require.NoError(t, err)
	signer, err := NewLocalSignerFromBytes(privKey.Bytes())
	require.NoError(t, err)
	cfg := clparams.MainnetBeaconConfig
	mgr := NewBuilderManager(signer, nil, &cfg, testGenesisValidatorsRoot)
	builders := solid.NewStaticListSSZ[*cltypes.Builder](64, 73)
	builders.Append(nil)
	builders.Append(&cltypes.Builder{Pubkey: signer.Pubkey(), Balance: cfg.MinDepositAmount})
	s := state.New(&cfg)
	s.SetBuilders(builders)

	idx, found, err := mgr.ResolveIndex(&mockSyncedData{state: s})
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
