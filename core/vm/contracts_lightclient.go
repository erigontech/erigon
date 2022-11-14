package vm

import (
	"encoding/binary"
	"fmt"
	"github.com/ledgerwatch/erigon/core/vm/lightclient/iavl"
	cmn "github.com/tendermint/tendermint/libs/common"

	"github.com/ledgerwatch/erigon/core/vm/lightclient"
	"github.com/ledgerwatch/erigon/params"
	"github.com/tendermint/tendermint/crypto/merkle"
)

const (
	uint64TypeLength                      uint64 = 8
	precompileContractInputMetaDataLength uint64 = 32
	consensusStateLengthBytesLength       uint64 = 32

	tmHeaderValidateResultMetaDataLength uint64 = 32
	merkleProofValidateResultLength      uint64 = 32
)

// input:
// consensus state length | consensus state | tendermint header |
// 32 bytes               |                 |                   |
func decodeTendermintHeaderValidationInput(input []byte) (*lightclient.ConsensusState, *lightclient.Header, error) {
	csLen := binary.BigEndian.Uint64(input[consensusStateLengthBytesLength-uint64TypeLength : consensusStateLengthBytesLength])
	if uint64(len(input)) <= consensusStateLengthBytesLength+csLen {
		return nil, nil, fmt.Errorf("expected payload size %d, actual size: %d", consensusStateLengthBytesLength+csLen, len(input))
	}

	cs, err := lightclient.DecodeConsensusState(input[consensusStateLengthBytesLength : consensusStateLengthBytesLength+csLen])
	if err != nil {
		return nil, nil, err
	}
	header, err := lightclient.DecodeHeader(input[consensusStateLengthBytesLength+csLen:])
	if err != nil {
		return nil, nil, err
	}

	return &cs, header, nil
}

// tmHeaderValidate implemented as a native contract.
type tmHeaderValidate struct{}

func (c *tmHeaderValidate) RequiredGas(input []byte) uint64 {
	return params.TendermintHeaderValidateGas
}

func (c *tmHeaderValidate) Run(input []byte) (result []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("internal error: %v", r)
		}
	}()

	if uint64(len(input)) <= precompileContractInputMetaDataLength {
		return nil, fmt.Errorf("invalid input")
	}

	payloadLength := binary.BigEndian.Uint64(input[precompileContractInputMetaDataLength-uint64TypeLength : precompileContractInputMetaDataLength])
	if uint64(len(input)) != payloadLength+precompileContractInputMetaDataLength {
		return nil, fmt.Errorf("invalid input: input size should be %d, actual the size is %d", payloadLength+precompileContractInputMetaDataLength, len(input))
	}

	cs, header, err := decodeTendermintHeaderValidationInput(input[precompileContractInputMetaDataLength:])
	if err != nil {
		return nil, err
	}

	validatorSetChanged, err := cs.ApplyHeader(header)
	if err != nil {
		return nil, err
	}

	consensusStateBytes, err := cs.EncodeConsensusState()
	if err != nil {
		return nil, err
	}

	// result
	// | validatorSetChanged | empty      | consensusStateBytesLength |  new consensusState |
	// | 1 byte              | 23 bytes   | 8 bytes                   |                     |
	lengthBytes := make([]byte, tmHeaderValidateResultMetaDataLength)
	if validatorSetChanged {
		copy(lengthBytes[:1], []byte{0x01})
	}
	consensusStateBytesLength := uint64(len(consensusStateBytes))
	binary.BigEndian.PutUint64(lengthBytes[tmHeaderValidateResultMetaDataLength-uint64TypeLength:], consensusStateBytesLength)

	result = append(lengthBytes, consensusStateBytes...)

	return result, nil
}

//------------------------------------------------------------------------------------------------------------------------------------------------

// iavlMerkleProofValidate implemented as a native contract.
type iavlMerkleProofValidate struct {
	basicIavlMerkleProofValidate
}

func (c *iavlMerkleProofValidate) RequiredGas(input []byte) uint64 {
	return params.IAVLMerkleProofValidateGas
}

// input:
// | payload length | payload    |
// | 32 bytes       |            |
func (c *iavlMerkleProofValidate) Run(input []byte) (result []byte, err error) {
	return c.basicIavlMerkleProofValidate.Run(input)
}

// tmHeaderValidate implemented as a native contract.
type tmHeaderValidateNano struct{}

func (c *tmHeaderValidateNano) RequiredGas(input []byte) uint64 {
	return params.TendermintHeaderValidateGas
}

func (c *tmHeaderValidateNano) Run(input []byte) (result []byte, err error) {
	return nil, fmt.Errorf("suspend")
}

type iavlMerkleProofValidateNano struct{}

func (c *iavlMerkleProofValidateNano) RequiredGas(_ []byte) uint64 {
	return params.IAVLMerkleProofValidateGas
}

func (c *iavlMerkleProofValidateNano) Run(_ []byte) (result []byte, err error) {
	return nil, fmt.Errorf("suspend")
}

// ------------------------------------------------------------------------------------------------------------------------------------------------
type iavlMerkleProofValidateMoran struct {
	basicIavlMerkleProofValidate
}

func (c *iavlMerkleProofValidateMoran) RequiredGas(_ []byte) uint64 {
	return params.IAVLMerkleProofValidateGas
}

func (c *iavlMerkleProofValidateMoran) Run(input []byte) (result []byte, err error) {
	c.basicIavlMerkleProofValidate.verifiers = []merkle.ProofOpVerifier{
		forbiddenAbsenceOpVerifier,
		singleValueOpVerifier,
		multiStoreOpVerifier,
		forbiddenSimpleValueOpVerifier,
	}
	return c.basicIavlMerkleProofValidate.Run(input)
}

type basicIavlMerkleProofValidate struct {
	verifiers []merkle.ProofOpVerifier
}

func (c *basicIavlMerkleProofValidate) Run(input []byte) (result []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("internal error: %v", r)
		}
	}()

	if uint64(len(input)) <= precompileContractInputMetaDataLength {
		return nil, fmt.Errorf("invalid input: input should include %d bytes payload length and payload", precompileContractInputMetaDataLength)
	}

	payloadLength := binary.BigEndian.Uint64(input[precompileContractInputMetaDataLength-uint64TypeLength : precompileContractInputMetaDataLength])
	if uint64(len(input)) != payloadLength+precompileContractInputMetaDataLength {
		return nil, fmt.Errorf("invalid input: input size should be %d, actual the size is %d", payloadLength+precompileContractInputMetaDataLength, len(input))
	}

	kvmp, err := lightclient.DecodeKeyValueMerkleProof(input[precompileContractInputMetaDataLength:])
	if err != nil {
		return nil, err
	}

	kvmp.SetVerifiers(c.verifiers)
	valid := kvmp.Validate()
	if !valid {
		return nil, fmt.Errorf("invalid merkle proof")
	}

	result = make([]byte, merkleProofValidateResultLength)
	binary.BigEndian.PutUint64(result[merkleProofValidateResultLength-uint64TypeLength:], 0x01)
	return result, nil
}
func forbiddenAbsenceOpVerifier(op merkle.ProofOperator) error {
	if op == nil {
		return nil
	}
	if _, ok := op.(iavl.IAVLAbsenceOp); ok {
		return cmn.NewError("absence proof suspend")
	}
	return nil
}

func forbiddenSimpleValueOpVerifier(op merkle.ProofOperator) error {
	if op == nil {
		return nil
	}
	if _, ok := op.(merkle.SimpleValueOp); ok {
		return cmn.NewError("simple value proof suspend")
	}
	return nil
}

func multiStoreOpVerifier(op merkle.ProofOperator) error {
	if op == nil {
		return nil
	}
	if mop, ok := op.(lightclient.MultiStoreProofOp); ok {
		storeNames := make(map[string]bool, len(mop.Proof.StoreInfos))
		for _, store := range mop.Proof.StoreInfos {
			if exist := storeNames[store.Name]; exist {
				return cmn.NewError("duplicated store")
			} else {
				storeNames[store.Name] = true
			}
		}
	}
	return nil
}

func singleValueOpVerifier(op merkle.ProofOperator) error {
	if op == nil {
		return nil
	}
	if valueOp, ok := op.(iavl.IAVLValueOp); ok {
		if len(valueOp.Proof.Leaves) != 1 {
			return cmn.NewError("range proof suspended")
		}
		for _, innerNode := range valueOp.Proof.LeftPath {
			if len(innerNode.Right) > 0 && len(innerNode.Left) > 0 {
				return cmn.NewError("both right and left hash exit!")
			}
		}
	}
	return nil
}
