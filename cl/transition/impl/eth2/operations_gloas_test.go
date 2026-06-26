package eth2_test

import (
	"strings"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

func TestProcessExecutionPayloadEnvelopeRejectsNilEnvelope(t *testing.T) {
	s := state.New(&clparams.MainnetBeaconConfig)
	machine := &eth2.Impl{}

	require.Error(t, machine.ProcessExecutionPayloadEnvelope(s, nil))
	require.Error(t, machine.ProcessExecutionPayloadEnvelope(s, &cltypes.SignedExecutionPayloadEnvelope{}))
	require.Error(t, machine.ProcessExecutionPayloadEnvelope(s, &cltypes.SignedExecutionPayloadEnvelope{
		Message: &cltypes.ExecutionPayloadEnvelope{},
	}))
}

func TestProcessDepositRequestRoutesBuilderDomainDeposit(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	s := state.New(&cfg)
	s.SetVersion(clparams.GloasVersion)
	s.SetBuilders(solid.NewStaticListSSZ[*cltypes.Builder](64, new(cltypes.Builder).EncodingSizeSSZ()))
	machine := &eth2.Impl{}

	pubkey, withdrawalCredentials, amount, signature := signedBuilderDeposit(t, &cfg, cfg.DomainBuilderDeposit, clparams.PayloadBuilderVersion)

	err := machine.ProcessDepositRequest(s, &solid.DepositRequest{
		PubKey:                pubkey,
		WithdrawalCredentials: withdrawalCredentials,
		Amount:                amount,
		Signature:             signature,
		Index:                 1,
	})
	require.NoError(t, err)

	require.Equal(t, 1, s.GetBuilders().Len())
	require.Equal(t, 0, s.GetPendingDeposits().Len())
	require.Equal(t, clparams.PayloadBuilderVersion, s.GetBuilders().Get(0).Version)
}

func TestProcessDepositRequestKeepsValidatorDomainDepositPending(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	s := state.New(&cfg)
	s.SetVersion(clparams.GloasVersion)
	s.SetBuilders(solid.NewStaticListSSZ[*cltypes.Builder](64, new(cltypes.Builder).EncodingSizeSSZ()))
	machine := &eth2.Impl{}

	pubkey, withdrawalCredentials, amount, signature := signedBuilderDeposit(t, &cfg, cfg.DomainDeposit, clparams.PayloadBuilderVersion)

	err := machine.ProcessDepositRequest(s, &solid.DepositRequest{
		PubKey:                pubkey,
		WithdrawalCredentials: withdrawalCredentials,
		Amount:                amount,
		Signature:             signature,
		Index:                 1,
	})
	require.NoError(t, err)

	require.Equal(t, 0, s.GetBuilders().Len())
	require.Equal(t, 1, s.GetPendingDeposits().Len())
}

func TestProcessDepositRequestRejectsMalformedBuilderCredentialsPadding(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	s := state.New(&cfg)
	s.SetVersion(clparams.GloasVersion)
	s.SetBuilders(solid.NewStaticListSSZ[*cltypes.Builder](64, new(cltypes.Builder).EncodingSizeSSZ()))
	machine := &eth2.Impl{}

	privKey, err := bls.GenerateKey()
	require.NoError(t, err)

	var pubkey common.Bytes48
	copy(pubkey[:], bls.CompressPublicKey(privKey.PublicKey()))

	// Build credentials with byte 0 == PayloadBuilderVersion but non-zero padding in bytes 1..11
	var withdrawalCredentials common.Hash
	withdrawalCredentials[0] = clparams.PayloadBuilderVersion
	withdrawalCredentials[5] = 0xFF // malformed: non-zero in reserved padding
	feeRecipient := common.HexToAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	copy(withdrawalCredentials[12:], feeRecipient[:])

	amount := cfg.MinDepositAmount
	depositData := &cltypes.DepositData{
		PubKey:                pubkey,
		WithdrawalCredentials: withdrawalCredentials,
		Amount:                amount,
	}
	messageRoot, err := depositData.MessageHash()
	require.NoError(t, err)
	domain, err := fork.ComputeDomain(cfg.DomainBuilderDeposit[:], utils.Uint32ToBytes4(uint32(cfg.GenesisForkVersion)), [32]byte{})
	require.NoError(t, err)
	signingRoot := utils.Sha256(messageRoot[:], domain)

	var signature common.Bytes96
	copy(signature[:], privKey.Sign(signingRoot[:]).Bytes())

	err = machine.ProcessDepositRequest(s, &solid.DepositRequest{
		PubKey:                pubkey,
		WithdrawalCredentials: withdrawalCredentials,
		Amount:                amount,
		Signature:             signature,
		Index:                 1,
	})
	require.NoError(t, err)

	// Malformed credentials must NOT be routed to builder registry
	require.Equal(t, 0, s.GetBuilders().Len())
	require.Equal(t, 1, s.GetPendingDeposits().Len())
}

func TestProcessDepositRequestRejectsMalformed0x03CredentialsPadding(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	s := state.New(&cfg)
	s.SetVersion(clparams.GloasVersion)
	s.SetBuilders(solid.NewStaticListSSZ[*cltypes.Builder](64, new(cltypes.Builder).EncodingSizeSSZ()))
	machine := &eth2.Impl{}

	privKey, err := bls.GenerateKey()
	require.NoError(t, err)

	var pubkey common.Bytes48
	copy(pubkey[:], bls.CompressPublicKey(privKey.PublicKey()))

	// Build credentials with 0x03 prefix but non-zero padding in bytes 1..11
	var withdrawalCredentials common.Hash
	withdrawalCredentials[0] = byte(cfg.BuilderWithdrawalPrefix)
	withdrawalCredentials[3] = 0xAB // malformed: non-zero in reserved padding
	feeRecipient := common.HexToAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	copy(withdrawalCredentials[12:], feeRecipient[:])

	amount := cfg.MinDepositAmount
	depositData := &cltypes.DepositData{
		PubKey:                pubkey,
		WithdrawalCredentials: withdrawalCredentials,
		Amount:                amount,
	}
	messageRoot, err := depositData.MessageHash()
	require.NoError(t, err)
	domain, err := fork.ComputeDomain(cfg.DomainDeposit[:], utils.Uint32ToBytes4(uint32(cfg.GenesisForkVersion)), [32]byte{})
	require.NoError(t, err)
	signingRoot := utils.Sha256(messageRoot[:], domain)

	var signature common.Bytes96
	copy(signature[:], privKey.Sign(signingRoot[:]).Bytes())

	err = machine.ProcessDepositRequest(s, &solid.DepositRequest{
		PubKey:                pubkey,
		WithdrawalCredentials: withdrawalCredentials,
		Amount:                amount,
		Signature:             signature,
		Index:                 1,
	})
	require.NoError(t, err)

	// Malformed 0x03 credentials must NOT be routed to builder registry
	require.Equal(t, 0, s.GetBuilders().Len())
	require.Equal(t, 1, s.GetPendingDeposits().Len())
}

func TestProcessDepositRequestDoesNotRouteFutureVersionBuilderDomainDeposit(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	s := state.New(&cfg)
	s.SetVersion(clparams.GloasVersion)
	s.SetBuilders(solid.NewStaticListSSZ[*cltypes.Builder](64, new(cltypes.Builder).EncodingSizeSSZ()))
	machine := &eth2.Impl{}

	pubkey, withdrawalCredentials, amount, signature := signedBuilderDeposit(t, &cfg, cfg.DomainBuilderDeposit, 0x02)

	err := machine.ProcessDepositRequest(s, &solid.DepositRequest{
		PubKey:                pubkey,
		WithdrawalCredentials: withdrawalCredentials,
		Amount:                amount,
		Signature:             signature,
		Index:                 1,
	})
	require.NoError(t, err)

	require.Equal(t, 0, s.GetBuilders().Len())
	require.Equal(t, 1, s.GetPendingDeposits().Len())
}

func TestProcessDepositRequestPendingValidatorBlocksSamePubkeyBuilderDeposit(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	s := state.New(&cfg)
	s.SetVersion(clparams.GloasVersion)
	s.SetBuilders(solid.NewStaticListSSZ[*cltypes.Builder](64, new(cltypes.Builder).EncodingSizeSSZ()))
	machine := &eth2.Impl{}

	privKey, err := bls.GenerateKey()
	require.NoError(t, err)
	pubkey, withdrawalCredentials, amount, signature := signedDepositWithKey(t, &cfg, cfg.DomainDeposit, clparams.PayloadBuilderVersion, privKey)
	err = machine.ProcessDepositRequest(s, &solid.DepositRequest{
		PubKey:                pubkey,
		WithdrawalCredentials: withdrawalCredentials,
		Amount:                amount,
		Signature:             signature,
		Index:                 1,
	})
	require.NoError(t, err)

	_, _, _, builderSignature := signedDepositWithKey(t, &cfg, cfg.DomainBuilderDeposit, clparams.PayloadBuilderVersion, privKey)
	err = machine.ProcessDepositRequest(s, &solid.DepositRequest{
		PubKey:                pubkey,
		WithdrawalCredentials: withdrawalCredentials,
		Amount:                amount,
		Signature:             builderSignature,
		Index:                 2,
	})
	require.NoError(t, err)

	require.Equal(t, 0, s.GetBuilders().Len())
	require.Equal(t, 2, s.GetPendingDeposits().Len())
}

func TestProcessExecutionPayloadBidRejectsBuilderVersionMismatch(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	s := state.New(&cfg)
	s.SetVersion(clparams.GloasVersion)
	s.SetFinalizedCheckpoint(solid.Checkpoint{Epoch: 1})
	s.SetBuilders(solid.NewStaticListSSZ[*cltypes.Builder](64, new(cltypes.Builder).EncodingSizeSSZ()))
	s.GetBuilders().Append(&cltypes.Builder{
		Pubkey:            common.Bytes48{1},
		Version:           byte(cfg.BuilderWithdrawalPrefix),
		Balance:           cfg.MinDepositAmount + 1,
		DepositEpoch:      0,
		WithdrawableEpoch: cfg.FarFutureEpoch,
	})

	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	block.Block.Body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{
		Message: &cltypes.ExecutionPayloadBid{
			BuilderIndex:       0,
			Value:              1,
			BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48),
		},
	}

	err := (&eth2.Impl{}).ProcessExecutionPayloadBid(s, block.Block)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "builder version mismatch"), err.Error())
}

func signedBuilderDeposit(t *testing.T, cfg *clparams.BeaconChainConfig, domainType common.Bytes4, version byte) (common.Bytes48, common.Hash, uint64, common.Bytes96) {
	t.Helper()

	privKey, err := bls.GenerateKey()
	require.NoError(t, err)
	return signedDepositWithKey(t, cfg, domainType, version, privKey)
}

func signedDepositWithKey(t *testing.T, cfg *clparams.BeaconChainConfig, domainType common.Bytes4, version byte, privKey *bls.PrivateKey) (common.Bytes48, common.Hash, uint64, common.Bytes96) {
	t.Helper()
	var pubkey common.Bytes48
	copy(pubkey[:], bls.CompressPublicKey(privKey.PublicKey()))

	var withdrawalCredentials common.Hash
	withdrawalCredentials[0] = version
	feeRecipient := common.HexToAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	copy(withdrawalCredentials[12:], feeRecipient[:])

	amount := cfg.MinDepositAmount
	depositData := &cltypes.DepositData{
		PubKey:                pubkey,
		WithdrawalCredentials: withdrawalCredentials,
		Amount:                amount,
	}
	messageRoot, err := depositData.MessageHash()
	require.NoError(t, err)
	domain, err := fork.ComputeDomain(domainType[:], utils.Uint32ToBytes4(uint32(cfg.GenesisForkVersion)), [32]byte{})
	require.NoError(t, err)
	signingRoot := utils.Sha256(messageRoot[:], domain)

	var signature common.Bytes96
	copy(signature[:], privKey.Sign(signingRoot[:]).Bytes())
	return pubkey, withdrawalCredentials, amount, signature
}
