package iavl

import (
	"fmt"

	"github.com/tendermint/tendermint/crypto/merkle"
	cmn "github.com/tendermint/tendermint/libs/common"
)

const ProofOpIAVLValue = "iavl:v"

// IAVLValueOp takes a key and a single value as argument and
// produces the root hash.
//
// If the produced root hash matches the expected hash, the proof
// is good.
type IAVLValueOp struct {
	// Encoded in ProofOp.Key.
	key []byte

	// To encode in ProofOp.Data.
	// Proof is nil for an empty tree.
	// The hash of an empty tree is nil.
	Proof *RangeProof `json:"proof"`
}

var _ merkle.ProofOperator = IAVLValueOp{}

func NewIAVLValueOp(key []byte, proof *RangeProof) IAVLValueOp {
	return IAVLValueOp{
		key:   key,
		Proof: proof,
	}
}

func IAVLValueOpDecoder(pop merkle.ProofOp) (merkle.ProofOperator, error) {
	if pop.Type != ProofOpIAVLValue {
		return nil, cmn.NewError("unexpected ProofOp.Type; got %v, want %v", pop.Type, ProofOpIAVLValue)
	}
	var op IAVLValueOp // a bit strange as we'll discard this, but it works.
	err := cdc.UnmarshalBinaryLengthPrefixed(pop.Data, &op)
	if err != nil {
		return nil, cmn.ErrorWrap(err, "decoding ProofOp.Data into IAVLValueOp")
	}
	return NewIAVLValueOp(pop.Key, op.Proof), nil
}

func (op IAVLValueOp) ProofOp() merkle.ProofOp {
	bz := cdc.MustMarshalBinaryLengthPrefixed(op)
	return merkle.ProofOp{
		Type: ProofOpIAVLValue,
		Key:  op.key,
		Data: bz,
	}
}

func (op IAVLValueOp) String() string {
	return fmt.Sprintf("IAVLValueOp{%v}", op.GetKey())
}

func (op IAVLValueOp) Run(args [][]byte) ([][]byte, error) {
	if len(args) != 1 {
		return nil, cmn.NewError("Value size is not 1")
	}
	value := args[0]

	// Compute the root hash and assume it is valid.
	// The caller checks the ultimate root later.
	root := op.Proof.ComputeRootHash()
	err := op.Proof.Verify(root)
	if err != nil {
		return nil, cmn.ErrorWrap(err, "computing root hash")
	}
	// XXX What is the encoding for keys?
	// We should decode the key depending on whether it's a string or hex,
	// maybe based on quotes and 0x prefix?
	err = op.Proof.VerifyItem([]byte(op.key), value)
	if err != nil {
		return nil, cmn.ErrorWrap(err, "verifying value")
	}
	return [][]byte{root}, nil
}

func (op IAVLValueOp) GetKey() []byte {
	return op.key
}
