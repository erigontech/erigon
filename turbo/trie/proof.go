package trie

import (
	"bytes"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/rlp"
)

// Prove constructs a merkle proof for key. The result contains all encoded nodes
// on the path to the value at key. The value itself is also included in the last
// node and can be retrieved by verifying the proof.
//
// If the trie does not contain a value for key, the returned proof contains all
// nodes of the longest existing prefix of the key (at least the root node), ending
// with the node that proves the absence of the key.
func (t *Trie) Prove(key []byte, fromLevel int, storage bool) ([][]byte, error) {
	var proof [][]byte
	hasher := newHasher(t.valueNodesRLPEncoded)
	defer returnHasherToPool(hasher)
	// Collect all nodes on the path to key.
	key = keybytesToHex(key)
	key = key[:len(key)-1] // Remove terminator
	tn := t.root
	for len(key) > 0 && tn != nil {
		switch n := tn.(type) {
		case *shortNode:
			if fromLevel == 0 {
				if rlp, err := hasher.hashChildren(n, 0); err == nil {
					proof = append(proof, libcommon.CopyBytes(rlp))
				} else {
					return nil, err
				}
			}
			nKey := n.Key
			if nKey[len(nKey)-1] == 16 {
				nKey = nKey[:len(nKey)-1]
			}
			if len(key) < len(nKey) || !bytes.Equal(nKey, key[:len(nKey)]) {
				// The trie doesn't contain the key.
				tn = nil
			} else {
				tn = n.Val
				key = key[len(nKey):]
			}
			if fromLevel > 0 {
				fromLevel -= len(nKey)
			}
		case *duoNode:
			if fromLevel == 0 {
				if rlp, err := hasher.hashChildren(n, 0); err == nil {
					proof = append(proof, libcommon.CopyBytes(rlp))
				} else {
					return nil, err
				}
			}
			i1, i2 := n.childrenIdx()
			switch key[0] {
			case i1:
				tn = n.child1
				key = key[1:]
			case i2:
				tn = n.child2
				key = key[1:]
			default:
				tn = nil
			}
			if fromLevel > 0 {
				fromLevel--
			}
		case *fullNode:
			if fromLevel == 0 {
				if rlp, err := hasher.hashChildren(n, 0); err == nil {
					proof = append(proof, libcommon.CopyBytes(rlp))
				} else {
					return nil, err
				}
			}
			tn = n.Children[key[0]]
			key = key[1:]
			if fromLevel > 0 {
				fromLevel--
			}
		case *accountNode:
			if storage {
				tn = n.storage
			} else {
				tn = nil
			}
		case valueNode:
			tn = nil
		case hashNode:
			return nil, fmt.Errorf("encountered hashNode unexpectedly, key %x, fromLevel %d", key, fromLevel)
		default:
			panic(fmt.Sprintf("%T: invalid node: %v", tn, tn))
		}
	}
	return proof, nil
}

func decodeRef(buf []byte) (node, []byte, error) {
	kind, val, rest, err := rlp.Split(buf)
	if err != nil {
		return nil, nil, err
	}
	switch {
	case kind == rlp.List:
		if len(buf)-len(rest) >= length.Hash {
			return nil, nil, fmt.Errorf("embedded nodes must be less than hash size")
		}
		n, err := decodeNode(buf)
		if err != nil {
			return nil, nil, err
		}
		return n, rest, nil
	case kind == rlp.String && len(val) == 0:
		return nil, rest, nil
	case kind == rlp.String && len(val) == 32:
		return hashNode{hash: val}, rest, nil
	default:
		return nil, nil, fmt.Errorf("invalid RLP string size %d (want 0 through 32)", len(val))
	}
}

func decodeFull(elems []byte) (*fullNode, error) {
	n := &fullNode{}
	for i := 0; i < 16; i++ {
		var err error
		n.Children[i], elems, err = decodeRef(elems)
		if err != nil {
			return nil, err
		}

	}
	val, _, err := rlp.SplitString(elems)
	if err != nil {
		return nil, err
	}
	if len(val) > 0 {
		n.Children[16] = valueNode(val)
	}
	return n, nil
}

func decodeShort(elems []byte) (*shortNode, error) {
	kbuf, rest, err := rlp.SplitString(elems)
	if err != nil {
		return nil, err
	}
	kb := CompactToKeybytes(kbuf)
	if kb.Terminating {
		val, _, err := rlp.SplitString(rest)
		if err != nil {
			return nil, err
		}
		return &shortNode{
			Key: kb.ToHex(),
			Val: valueNode(val),
		}, nil
	}

	val, _, err := decodeRef(rest)
	if err != nil {
		return nil, err
	}
	return &shortNode{
		Key: kb.ToHex(),
		Val: val,
	}, nil
}

func decodeNode(encoded []byte) (node, error) {
	if len(encoded) == 0 {
		return nil, fmt.Errorf("nodes must not be zero length")
	}
	elems, _, err := rlp.SplitList(encoded)
	if err != nil {
		return nil, err
	}
	switch c, _ := rlp.CountValues(elems); c {
	case 2:
		return decodeShort(elems)
	case 17:
		return decodeFull(elems)
	default:
		return nil, fmt.Errorf("invalid number of list elements: %v", c)
	}
}

type rawProofElement struct {
	index int
	value []byte
}

// proofMap creates a map from hash to proof node
func proofMap(proof []hexutility.Bytes) (map[libcommon.Hash]node, map[libcommon.Hash]rawProofElement, error) {
	res := map[libcommon.Hash]node{}
	raw := map[libcommon.Hash]rawProofElement{}
	for i, proofB := range proof {
		hash := crypto.Keccak256Hash(proofB)
		var err error
		res[hash], err = decodeNode(proofB)
		if err != nil {
			return nil, nil, err
		}
		raw[hash] = rawProofElement{
			index: i,
			value: proofB,
		}
	}
	return res, raw, nil
}

func verifyProof(root libcommon.Hash, key []byte, proofs map[libcommon.Hash]node, used map[libcommon.Hash]rawProofElement) ([]byte, error) {
	nextIndex := 0
	key = keybytesToHex(key)
	var node node = hashNode{hash: root[:]}
	for {
		switch nt := node.(type) {
		case *fullNode:
			if len(key) == 0 {
				return nil, fmt.Errorf("full nodes should not have values")
			}
			node, key = nt.Children[key[0]], key[1:]
			if node == nil {
				return nil, nil
			}
		case *shortNode:
			shortHex := nt.Key
			if len(shortHex) > len(key) {
				return nil, fmt.Errorf("len(shortHex)=%d must be leq len(key)=%d", len(shortHex), len(key))
			}
			if !bytes.Equal(shortHex, key[:len(shortHex)]) {
				return nil, nil
			}
			node, key = nt.Val, key[len(shortHex):]
		case hashNode:
			var ok bool
			h := libcommon.BytesToHash(nt.hash)
			node, ok = proofs[h]
			if !ok {
				return nil, fmt.Errorf("missing hash %s", nt)
			}
			raw, ok := used[h]
			if !ok {
				return nil, fmt.Errorf("missing hash %s", nt)
			}
			if nextIndex != raw.index {
				return nil, fmt.Errorf("proof elements present but not in expected order, expected %d at index %d", raw.index, nextIndex)
			}
			nextIndex++
			delete(used, h)
		case valueNode:
			if len(key) != 0 {
				return nil, fmt.Errorf("value node should have zero length remaining in key %x", key)
			}
			for hash, raw := range used {
				return nil, fmt.Errorf("not all proof elements were used hash=%x index=%d value=%x decoded=%#v", hash, raw.index, raw.value, proofs[hash])
			}
			return nt, nil
		default:
			return nil, fmt.Errorf("unexpected type: %T", node)
		}
	}
}

func VerifyAccountProof(stateRoot libcommon.Hash, proof *accounts.AccProofResult) error {
	accountKey := crypto.Keccak256Hash(proof.Address[:])
	return VerifyAccountProofByHash(stateRoot, accountKey, proof)
}

// VerifyAccountProofByHash will verify an account proof under the assumption
// that the pre-image of the accountKey hashes to the provided accountKey.
// Consequently, the Address of the proof is ignored in the validation.
func VerifyAccountProofByHash(stateRoot libcommon.Hash, accountKey libcommon.Hash, proof *accounts.AccProofResult) error {
	pm, used, err := proofMap(proof.AccountProof)
	if err != nil {
		return fmt.Errorf("could not construct proofMap: %w", err)
	}
	value, err := verifyProof(stateRoot, accountKey[:], pm, used)
	if err != nil {
		return fmt.Errorf("could not verify proof: %w", err)
	}

	if value == nil {
		// A nil value proves the account does not exist.
		switch {
		case proof.Nonce != 0:
			return fmt.Errorf("account is not in state, but has non-zero nonce")
		case proof.Balance.ToInt().Sign() != 0:
			return fmt.Errorf("account is not in state, but has balance")
		case proof.StorageHash != libcommon.Hash{}:
			return fmt.Errorf("account is not in state, but has non-empty storage hash")
		case proof.CodeHash != libcommon.Hash{}:
			return fmt.Errorf("account is not in state, but has non-empty code hash")
		default:
			return nil
		}
	}

	expected, err := rlp.EncodeToBytes([]any{
		uint64(proof.Nonce),
		proof.Balance.ToInt().Bytes(),
		proof.StorageHash,
		proof.CodeHash,
	})
	if err != nil {
		return err
	}

	if !bytes.Equal(expected, value) {
		return fmt.Errorf("account bytes from proof (%x) do not match expected (%x)", value, expected)
	}

	return nil
}

func VerifyStorageProof(storageRoot libcommon.Hash, proof accounts.StorProofResult) error {
	storageKey := crypto.Keccak256Hash(proof.Key[:])
	return VerifyStorageProofByHash(storageRoot, storageKey, proof)
}

// VerifyAccountProofByHash will verify a storage proof under the assumption
// that the pre-image of the storage key hashes to the provided keyHash.
// Consequently, the Key of the proof is ignored in the validation.
func VerifyStorageProofByHash(storageRoot libcommon.Hash, keyHash libcommon.Hash, proof accounts.StorProofResult) error {
	if storageRoot == EmptyRoot || storageRoot == (libcommon.Hash{}) {
		if proof.Value.ToInt().Sign() != 0 {
			return fmt.Errorf("empty storage root cannot have non-zero values")
		}
		// The spec here is a bit unclear.  The yellow paper makes it clear that the
		// EmptyRoot hash is a special case where the trie is empty.  Since the trie
		// is empty there are no proof elements to collect.  But, EIP-1186 also
		// clearly states that the proof must be "starting with the
		// storageHash-Node", which could imply an RLP encoded `[]byte(nil)` (the
		// pre-image of the EmptyRoot) should be included.  This implementation
		// chooses to require the proof be empty.
		if len(proof.Proof) > 0 {
			return fmt.Errorf("empty storage root should not have proof nodes")
		}
		return nil
	}
	pm, used, err := proofMap(proof.Proof)
	if err != nil {
		return fmt.Errorf("could not construct proofMap: %w", err)
	}
	value, err := verifyProof(storageRoot, keyHash[:], pm, used)
	if err != nil {
		return fmt.Errorf("could not verify proof: %w", err)
	}

	var expected []byte
	if value != nil {
		// A non-nil value proves the storage does exist.
		expected, err = rlp.EncodeToBytes(proof.Value.ToInt().Bytes())
		if err != nil {
			return err
		}
	}

	if !bytes.Equal(expected, value) {
		return fmt.Errorf("storage value from proof (%x) does not match expected (%x)", value, expected)
	}

	return nil
}
