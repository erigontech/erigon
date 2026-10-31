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

package machine_test

import (
	"encoding/binary"
	"fmt"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/transition/machine"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
)

type blockSignatureKeys struct {
	validators []*bls.PrivateKey
	builder    *bls.PrivateKey
}

type blockSignatureProcessor struct{}

type validatingBlockSignatureProcessor struct {
	blockSignatureProcessor
}

type payloadAttestationSlotProcessor struct {
	blockSignatureProcessor
}

func (blockSignatureProcessor) ProcessBlockHeader(abstract.BeaconState, uint64, uint64, common.Hash, [32]byte) error {
	return nil
}

func (blockSignatureProcessor) ProcessWithdrawals(abstract.BeaconState, *solid.ListSSZ[*cltypes.Withdrawal]) error {
	return nil
}

func (blockSignatureProcessor) ProcessExecutionPayload(abstract.BeaconState, cltypes.GenericBeaconBody) error {
	return nil
}

func (blockSignatureProcessor) ProcessExecutionPayloadBid(abstract.BeaconState, cltypes.GenericBeaconBlock) error {
	return nil
}

func (blockSignatureProcessor) ProcessParentExecutionPayload(abstract.BeaconState, cltypes.GenericBeaconBlock) error {
	return nil
}

func (blockSignatureProcessor) ApplyParentExecutionPayload(abstract.BeaconState, *cltypes.ExecutionRequests) error {
	return nil
}

func (blockSignatureProcessor) ProcessRandao(abstract.BeaconState, [96]byte, uint64) error {
	return nil
}

func (blockSignatureProcessor) ProcessEth1Data(abstract.BeaconState, *cltypes.Eth1Data) error {
	return nil
}

func (blockSignatureProcessor) ProcessSyncAggregate(abstract.BeaconState, *cltypes.SyncAggregate) error {
	return nil
}

func (blockSignatureProcessor) ProcessExecutionPayloadEnvelope(abstract.BeaconState, *cltypes.SignedExecutionPayloadEnvelope) error {
	return nil
}

func (blockSignatureProcessor) ProcessProposerSlashing(abstract.BeaconState, *cltypes.ProposerSlashing) error {
	return nil
}

func (blockSignatureProcessor) ProcessAttesterSlashing(abstract.BeaconState, *cltypes.AttesterSlashing) error {
	return nil
}

func (blockSignatureProcessor) ProcessAttestations(abstract.BeaconState, *solid.ListSSZ[*solid.Attestation], uint64) error {
	return nil
}

func (blockSignatureProcessor) ProcessDeposit(abstract.BeaconState, *cltypes.Deposit) error {
	return nil
}

func (blockSignatureProcessor) ProcessVoluntaryExit(abstract.BeaconState, *cltypes.SignedVoluntaryExit) error {
	return nil
}

func (blockSignatureProcessor) ProcessBlsToExecutionChange(abstract.BeaconState, *cltypes.SignedBLSToExecutionChange) error {
	return nil
}

func (blockSignatureProcessor) ProcessDepositRequest(abstract.BeaconState, *solid.DepositRequest) error {
	return nil
}

func (blockSignatureProcessor) ProcessWithdrawalRequest(abstract.BeaconState, *solid.WithdrawalRequest) error {
	return nil
}

func (blockSignatureProcessor) ProcessConsolidationRequest(abstract.BeaconState, *solid.ConsolidationRequest) error {
	return nil
}

func (blockSignatureProcessor) ProcessBuilderDepositRequest(abstract.BeaconState, *solid.BuilderDepositRequest) error {
	return nil
}

func (blockSignatureProcessor) ProcessBuilderExitRequest(abstract.BeaconState, *solid.BuilderExitRequest) error {
	return nil
}

func (blockSignatureProcessor) ProcessPayloadAttestation(abstract.BeaconState, *cltypes.PayloadAttestation) error {
	return nil
}

func (blockSignatureProcessor) FullValidate() bool { return false }

func (validatingBlockSignatureProcessor) FullValidate() bool { return true }

func (payloadAttestationSlotProcessor) ProcessPayloadAttestation(s abstract.BeaconState, attestation *cltypes.PayloadAttestation) error {
	if attestation.Data.Slot+1 != s.Slot() {
		return fmt.Errorf("payload attestation slot %d does not precede state slot %d", attestation.Data.Slot, s.Slot())
	}
	return nil
}

func TestProcessBlockBatchRejectsInvalidPayloadAttestationSignature(t *testing.T) {
	s, keys, block := blockSignatureFixture(t, 4)
	require.NoError(t, machine.ProcessBlock(blockSignatureProcessor{}, s, block))

	signPayloadAttestation(t, s, block.Body.PayloadAttestations.Get(0), []*bls.PrivateKey{keys.builder})

	err := machine.ProcessBlock(blockSignatureProcessor{}, s, block)
	require.EqualError(t, err, "block signature validation failed")
	require.NotErrorIs(t, err, machine.ErrInvalidExecutionPayloadBid)
}

func TestProcessBlockBatchRejectsInvalidExecutionPayloadBidSignature(t *testing.T) {
	s, keys, block := blockSignatureFixture(t, 4)
	require.NoError(t, machine.ProcessBlock(blockSignatureProcessor{}, s, block))

	signExecutionPayloadBid(t, s, block.Body.SignedExecutionPayloadBid, keys.validators[0])

	err := machine.ProcessBlock(blockSignatureProcessor{}, s, block)
	require.EqualError(t, err, "processBlock: invalid execution payload bid: invalid bid signature")
	require.ErrorIs(t, err, machine.ErrInvalidExecutionPayloadBid)
}

func TestProcessBlockBatchInvalidBidTakesPrecedenceOverInvalidPayloadAttestation(t *testing.T) {
	s, keys, block := blockSignatureFixture(t, 4)
	require.NoError(t, machine.ProcessBlock(blockSignatureProcessor{}, s, block))

	signExecutionPayloadBid(t, s, block.Body.SignedExecutionPayloadBid, keys.validators[0])
	signPayloadAttestation(t, s, block.Body.PayloadAttestations.Get(0), []*bls.PrivateKey{keys.builder})

	err := machine.ProcessBlock(blockSignatureProcessor{}, s, block)
	require.Error(t, err)
	require.ErrorIs(t, err, machine.ErrInvalidExecutionPayloadBid)
}

func TestProcessBlockBatchSelfBuildDoesNotClassifyInvalidPayloadAttestationAsBid(t *testing.T) {
	s, keys, block := blockSignatureFixture(t, 4)
	setSelfBuildBid(block)
	require.NoError(t, machine.ProcessBlock(blockSignatureProcessor{}, s, block))

	signPayloadAttestation(t, s, block.Body.PayloadAttestations.Get(0), []*bls.PrivateKey{keys.builder})

	err := machine.ProcessBlock(blockSignatureProcessor{}, s, block)
	require.Error(t, err)
	require.NotErrorIs(t, err, machine.ErrInvalidExecutionPayloadBid)
}

func TestProcessBlockEarlierOperationFailureMasksInvalidBidSignature(t *testing.T) {
	s, keys, block := blockSignatureFixture(t, 4)
	processor := payloadAttestationSlotProcessor{}
	require.NoError(t, machine.ProcessBlock(processor, s, block))

	signExecutionPayloadBid(t, s, block.Body.SignedExecutionPayloadBid, keys.validators[0])
	block.Body.PayloadAttestations.Get(0).Data.Slot = s.Slot() - 2

	err := machine.ProcessBlock(processor, s, block)
	require.ErrorContains(t, err, "payload attestation slot")
	require.NotErrorIs(t, err, machine.ErrInvalidExecutionPayloadBid)
}

func TestProcessBlockBatchDuplicatePTCMemberRequiresDuplicateSignature(t *testing.T) {
	s, keys, block := blockSignatureFixture(t, 2)
	setPTC(t, s, 0, 0)
	attestation := block.Body.PayloadAttestations.Get(0)
	signPayloadAttestation(t, s, attestation, []*bls.PrivateKey{keys.validators[0], keys.validators[0]})
	require.NoError(t, machine.ProcessBlock(blockSignatureProcessor{}, s, block))

	signPayloadAttestation(t, s, attestation, []*bls.PrivateKey{keys.validators[0]})

	err := machine.ProcessBlock(blockSignatureProcessor{}, s, block)
	require.Error(t, err)
	require.NotErrorIs(t, err, machine.ErrInvalidExecutionPayloadBid)
}

func TestProcessBlockBatchRejectsInfiniteAggregatePayloadPublicKey(t *testing.T) {
	s, _, block := blockSignatureFixture(t, 2)
	setSelfBuildBid(block)
	require.NoError(t, machine.ProcessBlock(blockSignatureProcessor{}, s, block))

	keys := setComplementaryValidatorKeys(t, s)
	signPayloadAttestation(t, s, block.Body.PayloadAttestations.Get(0), keys)

	err := machine.ProcessBlock(blockSignatureProcessor{}, s, block)
	require.ErrorIs(t, err, bls.ErrInfinitePublicKey)
	require.NotErrorIs(t, err, machine.ErrInvalidExecutionPayloadBid)
}

func TestProcessBlockBatchRejectsUndecodablePayloadAttestationSignature(t *testing.T) {
	s, _, block := blockSignatureFixture(t, 4)
	require.NoError(t, machine.ProcessBlock(blockSignatureProcessor{}, s, block))

	for i := range block.Body.PayloadAttestations.Get(0).Signature {
		block.Body.PayloadAttestations.Get(0).Signature[i] = 0xff
	}

	err := machine.ProcessBlock(blockSignatureProcessor{}, s, block)
	require.Error(t, err)
	require.NotErrorIs(t, err, machine.ErrInvalidExecutionPayloadBid)
}

func TestProcessBlockBatchRejectsInfinitePayloadAttestationSignature(t *testing.T) {
	s, _, block := blockSignatureFixture(t, 4)
	require.NoError(t, machine.ProcessBlock(blockSignatureProcessor{}, s, block))

	block.Body.PayloadAttestations.Get(0).Signature = common.Bytes96(bls.InfiniteSignature)

	err := machine.ProcessBlock(blockSignatureProcessor{}, s, block)
	require.Error(t, err)
	require.NotErrorIs(t, err, machine.ErrInvalidExecutionPayloadBid)
}

func TestProcessBlockBatchAcceptsFourPayloadAttestations(t *testing.T) {
	s, keys, block := blockSignatureFixture(t, 4)
	block.Body.PayloadAttestations = newPayloadAttestationList(s)
	for i := range 4 {
		block.Body.PayloadAttestations.Append(newPayloadAttestation(t, s, keys.validators, i&1 != 0, i&2 != 0))
	}

	require.NoError(t, machine.ProcessBlock(blockSignatureProcessor{}, s, block))
}

func TestProcessBlockBatchAcceptsValidRandaoAndBid(t *testing.T) {
	s, keys, block := blockSignatureFixture(t, 4)
	signRandao(t, s, block, keys.validators[0])

	require.NoError(t, machine.ProcessBlock(validatingBlockSignatureProcessor{}, s, block))
}

func TestProcessBlockBatchRejectsMalformedExecutionPayloadBidSignature(t *testing.T) {
	s, _, block := blockSignatureFixture(t, 4)
	require.NoError(t, machine.ProcessBlock(blockSignatureProcessor{}, s, block))

	for i := range block.Body.SignedExecutionPayloadBid.Signature {
		block.Body.SignedExecutionPayloadBid.Signature[i] = 0xff
	}

	err := machine.ProcessBlock(blockSignatureProcessor{}, s, block)
	require.EqualError(t, err, "processBlock: failed to verify bid signature: bls(signature): could not deserialize")
	require.NotErrorIs(t, err, machine.ErrInvalidExecutionPayloadBid)
	require.ErrorIs(t, err, bls.ErrDeserializeSignature)
}

func TestProcessBlockPreservesPreGloasBatchError(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	s := state.New(&cfg)
	s.SetVersion(clparams.Phase0Version)
	require.NoError(t, s.SetSlot(1))
	s.SetFork(&cltypes.Fork{
		PreviousVersion: utils.Uint32ToBytes4(uint32(cfg.GenesisForkVersion)),
		CurrentVersion:  utils.Uint32ToBytes4(uint32(cfg.GenesisForkVersion)),
	})
	require.NoError(t, s.AddValidator(solid.NewValidator(), cfg.MaxEffectiveBalance))
	block := cltypes.NewBeaconBlock(&cfg, clparams.Phase0Version)
	block.Slot = s.Slot()

	err := machine.ProcessBlock(validatingBlockSignatureProcessor{}, s, block)

	require.ErrorIs(t, err, bls.ErrDeserializePublicKey)
}

func TestProcessBlockPreservesGloasBatchErrorAfterValidBid(t *testing.T) {
	s, _, block := blockSignatureFixture(t, 4)
	block.Body.PayloadAttestations = newPayloadAttestationList(s)
	proposer, err := s.ValidatorForValidatorIndex(0)
	require.NoError(t, err)
	clear(proposer.PublicKeyBytes())

	err = machine.ProcessBlock(validatingBlockSignatureProcessor{}, s, block)
	require.ErrorIs(t, err, bls.ErrDeserializePublicKey)
	require.NotErrorIs(t, err, machine.ErrInvalidExecutionPayloadBid)
}

func blockSignatureFixture(t testing.TB, validatorCount int) (*state.CachingBeaconState, blockSignatureKeys, *cltypes.BeaconBlock) {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	cfg.GloasForkEpoch = 0
	cfg.GloasForkVersion = 0x80000038
	cfg.PtcSize = uint64(validatorCount)
	cfg.InitializeForkSchedule()

	s := state.New(&cfg)
	s.SetVersion(clparams.GloasVersion)
	require.NoError(t, s.SetSlot(cfg.SlotsPerEpoch*2+1))
	s.SetGenesisValidatorsRoot(common.HexToHash("0x01"))
	s.SetFork(&cltypes.Fork{
		PreviousVersion: utils.Uint32ToBytes4(uint32(cfg.GloasForkVersion)),
		CurrentVersion:  utils.Uint32ToBytes4(uint32(cfg.GloasForkVersion)),
	})
	s.SetFinalizedCheckpoint(solid.Checkpoint{Epoch: 1})

	keys := blockSignatureKeys{validators: make([]*bls.PrivateKey, validatorCount)}
	for i := range validatorCount {
		keys.validators[i] = deterministicBLSKey(t, uint64(i+1))
		publicKey := common.Bytes48(bls.CompressPublicKey(keys.validators[i].PublicKey()))
		validator := solid.NewValidatorFromParameters(
			publicKey,
			common.Hash{},
			cfg.MaxEffectiveBalance,
			false,
			0,
			0,
			cfg.FarFutureEpoch,
			cfg.FarFutureEpoch,
		)
		require.NoError(t, s.AddValidator(validator, cfg.MaxEffectiveBalance))
	}

	previousSlot := s.Slot() - 1
	ptcWindow := solid.NewUint64VectorOfVectors(int((2+cfg.MinSeedLookahead)*cfg.SlotsPerEpoch), validatorCount)
	ptc := solid.NewUint64VectorSSZ(validatorCount)
	for i := range validatorCount {
		ptc.Set(i, uint64(i))
	}
	ptcWindow.Set(int(cfg.SlotsPerEpoch+previousSlot%cfg.SlotsPerEpoch), ptc)
	s.SetPtcWindow(ptcWindow)

	parentRoot := common.HexToHash("0x02")
	s.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{Slot: previousSlot, ParentRoot: parentRoot})

	keys.builder = deterministicBLSKey(t, uint64(validatorCount+1))
	builderPublicKey := common.Bytes48(bls.CompressPublicKey(keys.builder.PublicKey()))
	builders := solid.NewStaticListSSZ[*cltypes.Builder](1, new(cltypes.Builder).EncodingSizeSSZ())
	builders.Append(&cltypes.Builder{
		Pubkey:            builderPublicKey,
		Version:           cfg.PayloadBuilderVersion,
		Balance:           cfg.MinDepositAmount,
		WithdrawableEpoch: cfg.FarFutureEpoch,
	})
	s.SetBuilders(builders)

	block := cltypes.NewBeaconBlock(&cfg, clparams.GloasVersion)
	block.Slot = s.Slot()
	block.ProposerIndex = 0
	block.Body.SignedExecutionPayloadBid.Message.BuilderIndex = 0
	block.Body.SignedExecutionPayloadBid.Message.Slot = s.Slot()
	signExecutionPayloadBid(t, s, block.Body.SignedExecutionPayloadBid, keys.builder)

	block.Body.PayloadAttestations.Append(newPayloadAttestation(t, s, keys.validators, true, false))

	return s, keys, block
}

func newPayloadAttestationList(s *state.CachingBeaconState) *solid.ListSSZ[*cltypes.PayloadAttestation] {
	return solid.NewStaticProgressiveListSSZ[*cltypes.PayloadAttestation](
		int(s.BeaconConfig().MaxPayloadAttestations),
		cltypes.PayloadAttestationSSZSizeWithPtcSize(s.BeaconConfig().PtcSize),
	)
}

func newPayloadAttestation(t testing.TB, s *state.CachingBeaconState, keys []*bls.PrivateKey, payloadPresent, blobDataAvailable bool) *cltypes.PayloadAttestation {
	t.Helper()
	attestation := &cltypes.PayloadAttestation{
		AggregationBits: solid.NewBitVector(int(s.BeaconConfig().PtcSize)),
		Data: &cltypes.PayloadAttestationData{
			BeaconBlockRoot:   s.LatestBlockHeader().ParentRoot,
			Slot:              s.Slot() - 1,
			PayloadPresent:    payloadPresent,
			BlobDataAvailable: blobDataAvailable,
		},
	}
	for i := range s.BeaconConfig().PtcSize {
		require.NoError(t, attestation.AggregationBits.SetBitAt(int(i), true))
	}
	signPayloadAttestation(t, s, attestation, keys)
	return attestation
}

func setSelfBuildBid(block *cltypes.BeaconBlock) {
	block.Body.SignedExecutionPayloadBid.Message.BuilderIndex = clparams.BuilderIndexSelfBuild
	block.Body.SignedExecutionPayloadBid.Message.Value = 0
	block.Body.SignedExecutionPayloadBid.Signature = common.Bytes96(bls.InfiniteSignature)
}

func setPTC(t testing.TB, s *state.CachingBeaconState, indices ...uint64) {
	t.Helper()
	ptc := solid.NewUint64VectorSSZ(len(indices))
	for i, index := range indices {
		ptc.Set(i, index)
	}
	windowIndex := int(s.BeaconConfig().SlotsPerEpoch + (s.Slot()-1)%s.BeaconConfig().SlotsPerEpoch)
	window := s.GetPtcWindow()
	window.Set(windowIndex, ptc)
	s.SetPtcWindow(window)
}

func setComplementaryValidatorKeys(t testing.TB, s *state.CachingBeaconState) []*bls.PrivateKey {
	t.Helper()
	first := deterministicBLSKey(t, 100)
	order, ok := new(big.Int).SetString("52435875175126190479447740508185965837690552500527637822603658699938581184513", 10)
	require.True(t, ok)
	secondBytes := make([]byte, 32)
	new(big.Int).Sub(order, new(big.Int).SetBytes(first.Bytes())).FillBytes(secondBytes)
	second, err := bls.NewPrivateKeyFromBytes(secondBytes)
	require.NoError(t, err)
	keys := []*bls.PrivateKey{first, second}
	for i, key := range keys {
		validator, err := s.ValidatorForValidatorIndex(i)
		require.NoError(t, err)
		copy(validator.PublicKeyBytes(), bls.CompressPublicKey(key.PublicKey()))
	}
	return keys
}

func signRandao(t testing.TB, s *state.CachingBeaconState, block *cltypes.BeaconBlock, key *bls.PrivateKey) {
	t.Helper()
	epoch := state.Epoch(s)
	domain, err := s.GetDomain(s.BeaconConfig().DomainRandao, epoch)
	require.NoError(t, err)
	message := make([]byte, 32)
	binary.LittleEndian.PutUint64(message, epoch)
	signingRoot := crypto.Sha256(message, domain)
	copy(block.Body.RandaoReveal[:], key.Sign(signingRoot[:]).Bytes())
}

func deterministicBLSKey(t testing.TB, index uint64) *bls.PrivateKey {
	t.Helper()
	ikm := make([]byte, 32)
	binary.LittleEndian.PutUint64(ikm, index)
	key, err := bls.NewPrivateKeyFromIKM(ikm)
	require.NoError(t, err)
	return key
}

func signPayloadAttestation(t testing.TB, s *state.CachingBeaconState, attestation *cltypes.PayloadAttestation, keys []*bls.PrivateKey) {
	t.Helper()
	domain, err := s.GetDomain(s.BeaconConfig().DomainPtcAttester, state.GetEpochAtSlot(s.BeaconConfig(), attestation.Data.Slot))
	require.NoError(t, err)
	signingRoot, err := fork.ComputeSigningRoot(attestation.Data, domain)
	require.NoError(t, err)
	signatures := make([][]byte, len(keys))
	for i, key := range keys {
		signatures[i] = key.Sign(signingRoot[:]).Bytes()
	}
	aggregate, err := bls.AggregateSignatures(signatures)
	require.NoError(t, err)
	copy(attestation.Signature[:], aggregate)
}

func signExecutionPayloadBid(t testing.TB, s *state.CachingBeaconState, bid *cltypes.SignedExecutionPayloadBid, key *bls.PrivateKey) {
	t.Helper()
	domain, err := s.GetDomain(s.BeaconConfig().DomainBeaconBuilder, state.Epoch(s))
	require.NoError(t, err)
	signingRoot, err := fork.ComputeSigningRoot(bid.Message, domain)
	require.NoError(t, err)
	copy(bid.Signature[:], key.Sign(signingRoot[:]).Bytes())
}
