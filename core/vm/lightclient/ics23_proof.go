package lightclient

import (
	"fmt"

	"github.com/bnb-chain/ics23"
	"github.com/tendermint/tendermint/crypto/merkle"
)

const (
	ProofOpIAVLCommitment         = "ics23:iavl"
	ProofOpSimpleMerkleCommitment = "ics23:simple"
)

// CommitmentOp implements merkle.ProofOperator by wrapping an ics23 CommitmentProof
// It also contains a Key field to determine which key the proof is proving.
// NOTE: CommitmentProof currently can either be ExistenceProof or NonexistenceProof
//
// Type and Spec are classified by the kind of merkle proof it represents allowing
// the code to be reused by more types. Spec is never on the wire, but mapped from type in the code.
type CommitmentOp struct {
	Type  string
	Spec  *ics23.ProofSpec
	Key   []byte
	Proof *ics23.CommitmentProof
}

var _ merkle.ProofOperator = CommitmentOp{}

// CommitmentOpDecoder takes a merkle.ProofOp and attempts to decode it into a CommitmentOp ProofOperator
// The proofOp.Data is just a marshalled CommitmentProof. The Key of the CommitmentOp is extracted
// from the unmarshalled proof.
func CommitmentOpDecoder(pop merkle.ProofOp) (merkle.ProofOperator, error) {
	var spec *ics23.ProofSpec
	switch pop.Type {
	case ProofOpIAVLCommitment:
		spec = ics23.IavlSpec
	case ProofOpSimpleMerkleCommitment:
		spec = ics23.TendermintSpec
	default:
		return nil, fmt.Errorf("unexpected ProofOp.Type; got %s, want supported ics23 subtypes 'ProofOpIAVLCommitment' or 'ProofOpSimpleMerkleCommitment'", pop.Type)
	}

	proof := &ics23.CommitmentProof{}
	err := proof.Unmarshal(pop.Data)
	if err != nil {
		return nil, err
	}

	op := CommitmentOp{
		Type:  pop.Type,
		Key:   pop.Key,
		Spec:  spec,
		Proof: proof,
	}
	return op, nil
}

func (op CommitmentOp) GetKey() []byte {
	return op.Key
}

// Run takes in a list of arguments and attempts to run the proof op against these arguments.
// Returns the root wrapped in [][]byte if the proof op succeeds with given args. If not,
// it will return an error.
//
// CommitmentOp will accept args of length 1 or length 0
// If length 1 args is passed in, then CommitmentOp will attempt to prove the existence of the key
// with the value provided by args[0] using the embedded CommitmentProof and return the CommitmentRoot of the proof.
// If length 0 args is passed in, then CommitmentOp will attempt to prove the absence of the key
// in the CommitmentOp and return the CommitmentRoot of the proof.
func (op CommitmentOp) Run(args [][]byte) ([][]byte, error) {
	if _, ok := op.Proof.Proof.(*ics23.CommitmentProof_Exist); !ok {
		return nil, fmt.Errorf("only exist proof supported")
	}

	// calculate root from proof
	root, err := op.Proof.Calculate()
	if err != nil {
		return nil, fmt.Errorf("could not calculate root for proof: %v", err)
	}
	if len(args) != 1 {
		return nil, fmt.Errorf("args must be length 1, got: %d", len(args))
	}

	// Args is length 1, verify existence of key with value args[0]
	if !ics23.VerifyMembership(op.Spec, root, op.Proof, op.Key, args[0]) {
		return nil, fmt.Errorf("proof did not verify existence of key %s with given value %x", op.Key, args[0])
	}

	return [][]byte{root}, nil
}

// ProofOp implements ProofOperator interface and converts a CommitmentOp
// into a merkle.ProofOp format that can later be decoded by CommitmentOpDecoder
// back into a CommitmentOp for proof verification
func (op CommitmentOp) ProofOp() merkle.ProofOp {
	bz, err := op.Proof.Marshal()
	if err != nil {
		panic(err.Error())
	}
	return merkle.ProofOp{
		Type: op.Type,
		Key:  op.Key,
		Data: bz,
	}
}
