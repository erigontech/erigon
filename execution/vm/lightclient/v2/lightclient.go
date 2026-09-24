// Package v2 is used for tendermint v0.34.22 and its compatible version.
package v2

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cometbft/cometbft/crypto/ed25519"
	cmtmath "github.com/cometbft/cometbft/libs/math"
	tmproto "github.com/cometbft/cometbft/proto/tendermint/types"
	"github.com/cometbft/cometbft/types"
)

const (
	uint64TypeLength                uint64 = 8
	consensusStateLengthBytesLength uint64 = 32
	validateResultMetaDataLength    uint64 = 32

	chainIDLength              uint64 = 32
	heightLength               uint64 = 8
	validatorSetHashLength     uint64 = 32
	validatorPubkeyLength      uint64 = 32
	validatorVotingPowerLength uint64 = 8
	relayerAddressLength       uint64 = 20
	relayerBlsKeyLength        uint64 = 48
	singleValidatorBytesLength uint64 = validatorPubkeyLength + validatorVotingPowerLength + relayerAddressLength + relayerBlsKeyLength
	maxConsensusStateLength    uint64 = chainIDLength + heightLength + validatorSetHashLength + 99*singleValidatorBytesLength // Maximum validator quantity 99
)

type ConsensusState struct {
	ChainID              string
	Height               uint64
	NextValidatorSetHash []byte
	ValidatorSet         *types.ValidatorSet
}

// output:
// | chainID   | height   | nextValidatorSetHash | [{validator pubkey, voting power, relayer address, relayer bls pubkey}] |
// | 32 bytes  | 8 bytes  | 32 bytes             | [{32 bytes, 8 bytes, 20 bytes, 48 bytes}]                               |
func (cs ConsensusState) EncodeConsensusState() ([]byte, error) {
	validatorSetLength := uint64(len(cs.ValidatorSet.Validators))
	serializeLength := chainIDLength + heightLength + validatorSetHashLength + validatorSetLength*singleValidatorBytesLength
	if serializeLength > maxConsensusStateLength {
		return nil, fmt.Errorf("too many validators %d, consensus state bytes should not exceed %d", len(cs.ValidatorSet.Validators), maxConsensusStateLength)
	}

	encodingBytes := make([]byte, serializeLength)

	pos := uint64(0)
	if uint64(len(cs.ChainID)) > chainIDLength {
		return nil, errors.New("chainID length should be no more than 32")
	}
	copy(encodingBytes[pos:pos+chainIDLength], cs.ChainID)
	pos += chainIDLength

	binary.BigEndian.PutUint64(encodingBytes[pos:pos+heightLength], cs.Height)
	pos += heightLength

	copy(encodingBytes[pos:pos+validatorSetHashLength], cs.NextValidatorSetHash)
	pos += validatorSetHashLength

	for index := uint64(0); index < validatorSetLength; index++ {
		validator := cs.ValidatorSet.Validators[index]
		pubkey, ok := validator.PubKey.(ed25519.PubKey)
		if !ok {
			return nil, errors.New("invalid pubkey type")
		}

		copy(encodingBytes[pos:pos+validatorPubkeyLength], pubkey[:])
		pos += validatorPubkeyLength

		binary.BigEndian.PutUint64(encodingBytes[pos:pos+validatorVotingPowerLength], uint64(validator.VotingPower))
		pos += validatorVotingPowerLength

		copy(encodingBytes[pos:pos+relayerAddressLength], validator.RelayerAddress)
		pos += relayerAddressLength

		copy(encodingBytes[pos:pos+relayerBlsKeyLength], validator.BlsKey)
		pos += relayerBlsKeyLength
	}

	return encodingBytes, nil
}

func (cs *ConsensusState) ApplyLightBlock(block *types.LightBlock, isHertz bool) (bool, error) {
	if uint64(block.Height) <= cs.Height {
		return false, fmt.Errorf("block height <= consensus height (%d < %d)", block.Height, cs.Height)
	}

	if err := block.ValidateBasic(cs.ChainID); err != nil {
		return false, err
	}

	if cs.Height == uint64(block.Height-1) {
		if !bytes.Equal(cs.NextValidatorSetHash, block.ValidatorsHash) {
			return false, fmt.Errorf("validators hash mismatch, expected: %s, real: %s", cs.NextValidatorSetHash, block.ValidatorsHash)
		}
		err := block.ValidatorSet.VerifyCommitLight(cs.ChainID, block.Commit.BlockID, block.Height, block.Commit)
		if err != nil {
			return false, err
		}
	} else {
		// Ensure that +`trustLevel` (default 1/3) or more of last trusted validators signed correctly.
		err := cs.ValidatorSet.VerifyCommitLightTrusting(cs.ChainID, block.Commit, cmtmath.Fraction{Numerator: 1, Denominator: 3})
		if err != nil {
			return false, err
		}

		// Ensure that +2/3 of new validators signed correctly.
		//
		// NOTE: this should always be the last check because untrustedVals can be
		// intentionally made very large to DOS the light client. not the case for
		// VerifyAdjacent, where validator set is known in advance.
		err = block.ValidatorSet.VerifyCommitLight(cs.ChainID, block.Commit.BlockID, block.Height, block.Commit)
		if err != nil {
			return false, err
		}
	}

	valSetChanged := !(bytes.Equal(cs.ValidatorSet.Hash(), block.ValidatorsHash))
	// update consensus state
	cs.Height = uint64(block.Height)
	cs.NextValidatorSetHash = block.NextValidatorsHash
	cs.ValidatorSet = block.ValidatorSet

	if !isHertz {
		// This logic is wrong, fixed in hertz fork.
		return !(bytes.Equal(cs.ValidatorSet.Hash(), block.ValidatorsHash)), nil
	}
	return valSetChanged, nil
}

// input:
// | chainID   | height   | nextValidatorSetHash | [{validator pubkey, voting power, relayer address, relayer bls pubkey}] |
// | 32 bytes  | 8 bytes  | 32 bytes             | [{32 bytes, 8 bytes, 20 bytes, 48 bytes}]                               |
func DecodeConsensusState(input []byte) (ConsensusState, error) {
	minimumLength := chainIDLength + heightLength + validatorSetHashLength
	inputLen := uint64(len(input))
	if inputLen <= minimumLength || (inputLen-minimumLength)%singleValidatorBytesLength != 0 {
		return ConsensusState{}, fmt.Errorf("expected input size %d+%d*N, actual input size: %d", minimumLength, singleValidatorBytesLength, inputLen)
	}

	pos := uint64(0)
	chainID := string(bytes.Trim(input[pos:pos+chainIDLength], "\x00"))
	pos += chainIDLength

	height := binary.BigEndian.Uint64(input[pos : pos+heightLength])
	pos += heightLength

	nextValidatorSetHash := input[pos : pos+validatorSetHashLength]
	pos += validatorSetHashLength

	validatorSetLength := (inputLen - minimumLength) / singleValidatorBytesLength
	validatorSetBytes := input[pos:]
	validatorSet := make([]*types.Validator, 0, validatorSetLength)
	for index := uint64(0); index < validatorSetLength; index++ {
		validatorBytes := validatorSetBytes[singleValidatorBytesLength*index : singleValidatorBytesLength*(index+1)]

		pos = 0
		pubkey := ed25519.PubKey(make([]byte, ed25519.PubKeySize))
		copy(pubkey[:], validatorBytes[:validatorPubkeyLength])
		pos += validatorPubkeyLength

		votingPower := int64(binary.BigEndian.Uint64(validatorBytes[pos : pos+validatorVotingPowerLength]))
		pos += validatorVotingPowerLength

		relayerAddress := make([]byte, relayerAddressLength)
		copy(relayerAddress, validatorBytes[pos:pos+relayerAddressLength])
		pos += relayerAddressLength

		relayerBlsKey := make([]byte, relayerBlsKeyLength)
		copy(relayerBlsKey, validatorBytes[pos:])

		validator := types.NewValidator(pubkey, votingPower)
		validator.SetRelayerAddress(relayerAddress)
		validator.SetBlsKey(relayerBlsKey)
		validatorSet = append(validatorSet, validator)
	}

	consensusState := ConsensusState{
		ChainID:              chainID,
		Height:               height,
		NextValidatorSetHash: nextValidatorSetHash,
		ValidatorSet: &types.ValidatorSet{
			Validators: validatorSet,
		},
	}

	return consensusState, nil
}

// input:
// consensus state length | consensus state | light block |
// 32 bytes               |                 |             |
func DecodeLightBlockValidationInput(input []byte) (*ConsensusState, *types.LightBlock, error) {
	if uint64(len(input)) <= consensusStateLengthBytesLength {
		return nil, nil, errors.New("invalid input")
	}

	csLen := binary.BigEndian.Uint64(input[consensusStateLengthBytesLength-uint64TypeLength : consensusStateLengthBytesLength])
	if uint64(len(input)) <= consensusStateLengthBytesLength+csLen {
		return nil, nil, fmt.Errorf("expected payload size %d, actual size: %d", consensusStateLengthBytesLength+csLen, len(input))
	}

	cs, err := DecodeConsensusState(input[consensusStateLengthBytesLength : consensusStateLengthBytesLength+csLen])
	if err != nil {
		return nil, nil, err
	}

	var lbpb tmproto.LightBlock
	err = lbpb.Unmarshal(input[consensusStateLengthBytesLength+csLen:])
	if err != nil {
		return nil, nil, err
	}
	block, err := types.LightBlockFromProto(&lbpb)
	if err != nil {
		return nil, nil, err
	}

	return &cs, block, nil
}

// output:
// | validatorSetChanged | empty      | consensusStateBytesLength |  new consensusState |
// | 1 byte              | 23 bytes   | 8 bytes                   |                     |
func EncodeLightBlockValidationResult(validatorSetChanged bool, consensusStateBytes []byte) []byte {
	lengthBytes := make([]byte, validateResultMetaDataLength)
	if validatorSetChanged {
		copy(lengthBytes[:1], []byte{0x01})
	}

	consensusStateBytesLength := uint64(len(consensusStateBytes))
	binary.BigEndian.PutUint64(lengthBytes[validateResultMetaDataLength-uint64TypeLength:], consensusStateBytesLength)

	result := append(lengthBytes, consensusStateBytes...)
	return result
}
