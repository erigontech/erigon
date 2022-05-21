package iavl

import (
	"fmt"

	"github.com/tendermint/tendermint/crypto/merkle"
	cmn "github.com/tendermint/tendermint/libs/common"
)

const ProofOpIAVLAbsence = "iavl:a"

// IAVLAbsenceOp takes a key as its only argument
//
// If the produced root hash matches the expected hash, the proof
// is good.
type IAVLAbsenceOp struct {
	// Encoded in ProofOp.Key.
	key []byte

	// To encode in ProofOp.Data.
	// Proof is nil for an empty tree.
	// The hash of an empty tree is nil.
	Proof *RangeProof `json:"proof"`
}

var _ merkle.ProofOperator = IAVLAbsenceOp{}

func NewIAVLAbsenceOp(key []byte, proof *RangeProof) IAVLAbsenceOp {
	return IAVLAbsenceOp{
		key:   key,
		Proof: proof,
	}
}

func IAVLAbsenceOpDecoder(pop merkle.ProofOp) (merkle.ProofOperator, error) {
	if pop.Type != ProofOpIAVLAbsence {
		return nil, cmn.NewError("unexpected ProofOp.Type; got %v, want %v", pop.Type, ProofOpIAVLAbsence)
	}
	var op IAVLAbsenceOp // a bit strange as we'll discard this, but it works.
	err := cdc.UnmarshalBinaryLengthPrefixed(pop.Data, &op)
	if err != nil {
		return nil, cmn.ErrorWrap(err, "decoding ProofOp.Data into IAVLAbsenceOp")
	}
	return NewIAVLAbsenceOp(pop.Key, op.Proof), nil
}

func (op IAVLAbsenceOp) ProofOp() merkle.ProofOp {
	bz := cdc.MustMarshalBinaryLengthPrefixed(op)
	return merkle.ProofOp{
		Type: ProofOpIAVLAbsence,
		Key:  op.key,
		Data: bz,
	}
}

func (op IAVLAbsenceOp) String() string {
	return fmt.Sprintf("IAVLAbsenceOp{%v}", op.GetKey())
}

func (op IAVLAbsenceOp) Run(args [][]byte) ([][]byte, error) {
	if len(args) != 0 {
		return nil, cmn.NewError("expected 0 args, got %v", len(args))
	}
	// If the tree is nil, the proof is nil, and all keys are absent.
	if op.Proof == nil {
		return [][]byte{[]byte(nil)}, nil
	}
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
	err = op.Proof.VerifyAbsence([]byte(op.key))
	if err != nil {
		return nil, cmn.ErrorWrap(err, "verifying absence")
	}
	return [][]byte{root}, nil
}

func (op IAVLAbsenceOp) GetKey() []byte {
	return op.key
}
