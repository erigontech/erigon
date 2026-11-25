package statechange

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
)

func signedPendingDeposit(t *testing.T, cfg *clparams.BeaconChainConfig, privateKey *bls.PrivateKey, prefix byte, amount uint64) *solid.PendingDeposit {
	t.Helper()
	if privateKey == nil {
		var err error
		privateKey, err = bls.GenerateKey()
		require.NoError(t, err)
	}
	d := &solid.PendingDeposit{Amount: amount}
	copy(d.PubKey[:], bls.CompressPublicKey(privateKey.PublicKey()))
	d.WithdrawalCredentials[0] = prefix
	message, err := (&cltypes.DepositData{
		PubKey:                d.PubKey,
		WithdrawalCredentials: d.WithdrawalCredentials,
		Amount:                d.Amount,
	}).MessageHash()
	require.NoError(t, err)
	domain, err := fork.ComputeDomain(cfg.DomainDeposit[:], utils.Uint32ToBytes4(uint32(cfg.GenesisForkVersion)), [32]byte{})
	require.NoError(t, err)
	signingRoot := crypto.Sha256(message[:], domain)
	copy(d.Signature[:], privateKey.Sign(signingRoot[:]).Bytes())
	return d
}

func TestProcessPendingDepositsBlsCredentialsForNewValidatorsFromHeze(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	for _, test := range []struct {
		name               string
		version            clparams.StateVersion
		prefix             byte
		wantValidatorAdded bool
	}{
		{"heze 0x00", clparams.HezeVersion, byte(cfg.BLSWithdrawalPrefixByte), false},
		{"heze 0x01", clparams.HezeVersion, byte(cfg.ETH1AddressWithdrawalPrefixByte), true},
		{"heze 0x02", clparams.HezeVersion, byte(cfg.CompoundingWithdrawalPrefix), true},
		{"gloas 0x00", clparams.GloasVersion, byte(cfg.BLSWithdrawalPrefixByte), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := newRewardsTestState(t, 64)
			upgradeTo(t, s, test.version)
			before := s.ValidatorLength()
			deposits := solid.NewPendingDepositList(cfg)
			deposits.Append(signedPendingDeposit(t, cfg, nil, test.prefix, cfg.MinActivationBalance))
			s.SetPendingDeposits(deposits)

			require.NoError(t, ProcessPendingDeposits(s))
			require.Zero(t, s.GetPendingDeposits().Len())
			if test.wantValidatorAdded {
				require.Equal(t, before+1, s.ValidatorLength())
			} else {
				require.Equal(t, before, s.ValidatorLength())
			}
		})
	}
}

func TestProcessPendingDepositsTopsUpExistingValidatorWithBlsCredentialsInHeze(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	s := newRewardsTestState(t, 64)
	upgradeTo(t, s, clparams.HezeVersion)
	validator := s.ValidatorSet().Get(5)
	creds := validator.WithdrawalCredentials()
	creds[0] = byte(cfg.BLSWithdrawalPrefixByte)
	validator.SetWithdrawalCredentials(creds)
	balance, err := s.ValidatorBalance(5)
	require.NoError(t, err)

	deposits := solid.NewPendingDepositList(cfg)
	deposits.Append(&solid.PendingDeposit{
		PubKey:                validator.PublicKey(),
		WithdrawalCredentials: common.Hash(creds),
		Amount:                1_000_000_000,
	})
	s.SetPendingDeposits(deposits)

	require.NoError(t, ProcessPendingDeposits(s))
	after, err := s.ValidatorBalance(5)
	require.NoError(t, err)
	require.Equal(t, balance+1_000_000_000, after)
}

func TestProcessPendingDepositsSkippedBlsDepositsConsumeChurnInHeze(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	s := newRewardsTestState(t, 64)
	upgradeTo(t, s, clparams.GloasVersion)
	churn := state.GetActivationChurnLimit(s)
	amount := churn * 2 / 5
	deposits := solid.NewPendingDepositList(cfg)
	for range 3 {
		deposits.Append(signedPendingDeposit(t, cfg, nil, byte(cfg.BLSWithdrawalPrefixByte), amount))
	}
	s.SetPendingDeposits(deposits)
	s.SetDepositRequestsStartIndex(s.Eth1DepositIndex())
	require.NoError(t, s.UpgradeToHeze())
	before := s.ValidatorLength()

	require.NoError(t, ProcessPendingDeposits(s))
	require.Equal(t, before, s.ValidatorLength())
	require.Equal(t, 1, s.GetPendingDeposits().Len())
	require.Equal(t, churn-2*amount, s.GetDepositBalanceToConsume())
}

func TestProcessPendingDepositsSamePubkeyTwiceInHeze(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	bls0 := byte(cfg.BLSWithdrawalPrefixByte)
	eth1 := byte(cfg.ETH1AddressWithdrawalPrefixByte)
	for _, test := range []struct {
		name          string
		first, second byte
		wantBalance   uint64
	}{
		{"0x00 then 0x01", bls0, eth1, cfg.MinActivationBalance},
		{"0x01 then 0x00", eth1, bls0, 2 * cfg.MinActivationBalance},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := newRewardsTestState(t, 64)
			upgradeTo(t, s, clparams.HezeVersion)
			before := s.ValidatorLength()
			privateKey, err := bls.GenerateKey()
			require.NoError(t, err)
			first := signedPendingDeposit(t, cfg, privateKey, test.first, cfg.MinActivationBalance)
			second := signedPendingDeposit(t, cfg, privateKey, test.second, cfg.MinActivationBalance)
			deposits := solid.NewPendingDepositList(cfg)
			deposits.Append(first)
			deposits.Append(second)
			s.SetPendingDeposits(deposits)

			require.NoError(t, ProcessPendingDeposits(s))
			require.Zero(t, s.GetPendingDeposits().Len())
			require.Equal(t, before+1, s.ValidatorLength())
			index, ok := s.ValidatorIndexByPubkey(first.PubKey)
			require.True(t, ok)
			require.Equal(t, uint64(before), index)
			creds := s.ValidatorSet().Get(int(index)).WithdrawalCredentials()
			require.Equal(t, eth1, creds[0])
			balance, err := s.ValidatorBalance(int(index))
			require.NoError(t, err)
			require.Equal(t, test.wantBalance, balance)
		})
	}
}
