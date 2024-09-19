package smt

import (
	"bytes"
	"context"
	"fmt"
	"math/big"

	"github.com/gateway-fm/cdk-erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"
	"github.com/ledgerwatch/erigon/turbo/trie"
)

type SMTProofElement struct {
	Path  []byte
	Proof []byte
}

// FilterProofs filters the proofs to only include the ones that match the given key
func FilterProofs(proofs []*SMTProofElement, key utils.NodeKey) []hexutility.Bytes {
	filteredProofs := make([]hexutility.Bytes, 0)
	keyPath := key.GetPath()

	keyPathInBytes := make([]byte, len(keyPath))
	for i, v := range keyPath {
		keyPathInBytes[i] = byte(v)
	}

	for _, proof := range proofs {
		if bytes.HasPrefix(keyPathInBytes, proof.Path) {
			proofClone := make([]byte, len(proof.Proof))
			copy(proofClone, proof.Proof)
			filteredProofs = append(filteredProofs, proofClone)
		}
	}

	return filteredProofs
}

// BuildProofs builds proofs for multiple accounts and storage slots by traversing the SMT once.
// It efficiently generates proofs for all requested keys in a single pass.
//
// s: The read-only SMT to traverse
// rd: The retain decider that determines which nodes to include in the proof
// ctx: Context for cancellation
//
// Returns a slice of SMTProofElement containing the proof for each retained node,
// or an error if the traversal fails.
func BuildProofs(s *RoSMT, rd trie.RetainDecider, ctx context.Context) ([]*SMTProofElement, error) {
	proofs := make([]*SMTProofElement, 0)

	root, err := s.DbRo.GetLastRoot()
	if err != nil {
		return nil, err
	}

	action := func(prefix []byte, k utils.NodeKey, v utils.NodeValue12) (bool, error) {
		retain := rd.Retain(prefix)

		if !retain {
			return false, nil
		}

		nodeBytes := make([]byte, 64)
		utils.ArrayToScalar(v.Get0to4()[:]).FillBytes(nodeBytes[:32])
		utils.ArrayToScalar(v.Get4to8()[:]).FillBytes(nodeBytes[32:])

		if v.IsFinalNode() {
			nodeBytes = append(nodeBytes, 1)
		}

		proofs = append(proofs, &SMTProofElement{
			Path:  prefix,
			Proof: nodeBytes,
		})

		if v.IsFinalNode() {
			valHash := v.Get4to8()
			v, err := s.DbRo.Get(*valHash)
			if err != nil {
				return false, err
			}

			vInBytes := utils.ArrayBigToScalar(utils.BigIntArrayFromNodeValue8(v.GetNodeValue8())).Bytes()

			proofs = append(proofs, &SMTProofElement{
				Path:  prefix,
				Proof: vInBytes,
			})

			return false, nil
		}

		return true, nil
	}

	err = s.Traverse(ctx, root, action)
	if err != nil {
		return nil, err
	}

	return proofs, nil
}

// VerifyAndGetVal verifies a proof against a given state root and key, and returns the associated value if valid.
//
// Parameters:
//   - stateRoot: The root node key to verify the proof against.
//   - proof: A slice of byte slices representing the proof elements.
//   - key: The node key for which the proof is being verified.
//
// Returns:
//   - []byte: The value associated with the key. If the key does not exist in the proof, the value returned will be nil.
//   - error: An error if the proof is invalid or verification fails.
//
// This function walks through the provided proof, verifying each step against the expected
// state root. It handles both branch and leaf nodes in the Sparse Merkle Tree. If the proof
// is valid and and value exists, it returns the value associated with the given key. If the proof is valid and
// the value does not exist, the value returned will be nil. If the proof is invalid at any point, an error is returned explaining where the verification failed.
//
// The function expects the proof to be in a specific format, with each element being either
// 64 bytes (for branch nodes) or 65 bytes (for leaf nodes, with the last byte indicating finality).
// It uses the utils package for various operations like hashing and key manipulation.
func VerifyAndGetVal(stateRoot utils.NodeKey, proof []hexutility.Bytes, key utils.NodeKey) ([]byte, error) {
	if len(proof) == 0 {
		return nil, fmt.Errorf("proof is empty")
	}

	path := key.GetPath()
	curRoot := stateRoot
	foundValue := false
	for i := 0; i < len(proof); i++ {
		isFinalNode := len(proof[i]) == 65

		capacity := utils.BranchCapacity

		if isFinalNode {
			capacity = utils.LeafCapacity
		}

		leftChild := utils.ScalarToArray(big.NewInt(0).SetBytes(proof[i][:32]))
		rightChild := utils.ScalarToArray(big.NewInt(0).SetBytes(proof[i][32:64]))

		leftChildNode := [4]uint64{leftChild[0], leftChild[1], leftChild[2], leftChild[3]}
		rightChildNode := [4]uint64{rightChild[0], rightChild[1], rightChild[2], rightChild[3]}

		h := utils.Hash(utils.ConcatArrays4(leftChildNode, rightChildNode), capacity)
		if curRoot != h {
			return nil, fmt.Errorf("root mismatch at level %d, expected %d, got %d", i, curRoot, h)
		}

		if !isFinalNode {
			if path[i] == 0 {
				curRoot = leftChildNode
			} else {
				curRoot = rightChildNode
			}

			// If the current root is zero, non-existence has been proven and we can return nil from here
			if curRoot.IsZero() {
				return nil, nil
			}
		} else {
			joinedKey := utils.JoinKey(path[:i], leftChildNode)
			if joinedKey.IsEqualTo(key) {
				foundValue = true
				curRoot = rightChildNode
				break
			} else {
				// If the joined key is not equal to the input key, the proof is sufficient to verify the non-existence of the value, so we return nil from here
				return nil, nil
			}
		}
	}

	// If we've made it through the loop without finding the value, the proof is insufficient to verify the non-existence of the value
	if !foundValue {
		return nil, fmt.Errorf("proof is insufficient to verify the non-existence of the value")
	}

	v := new(big.Int).SetBytes(proof[len(proof)-1])
	x := utils.ScalarToArrayBig(v)
	nodeValue, err := utils.NodeValue8FromBigIntArray(x)
	if err != nil {
		return nil, err
	}

	h := utils.Hash(nodeValue.ToUintArray(), utils.BranchCapacity)
	if h != curRoot {
		return nil, fmt.Errorf("root mismatch at level %d, expected %d, got %d", len(proof)-1, curRoot, h)
	}

	return proof[len(proof)-1], nil
}
