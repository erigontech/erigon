package trie

import (
	"fmt"
	"math/big"

	"github.com/erigontech/erigon-lib/common"
)

type HashNodeFunc func(Node, bool, []byte) (int, error)

type MerklePathLimiter struct {
	RetainDecider RetainDecider
	HashFunc      HashNodeFunc
}

type WitnessBuilder struct {
	root     Node
	trace    bool
	operands []WitnessOperator
}

func NewWitnessBuilder(root Node, trace bool) *WitnessBuilder {
	return &WitnessBuilder{
		root:     root,
		trace:    trace,
		operands: make([]WitnessOperator, 0),
	}
}

func (b *WitnessBuilder) Build(limiter *MerklePathLimiter) (*Witness, error) {
	err := b.makeBlockWitness(b.root, []byte{}, limiter, true)
	witness := NewWitness(b.operands)
	b.operands = nil
	return witness, err
}

func (b *WitnessBuilder) addLeafOp(key []byte, value []byte) error {
	if b.trace {
		fmt.Printf("LEAF_VALUE: k %x v:%x\n", key, value)
	}

	var op OperatorLeafValue

	op.Key = make([]byte, len(key))
	copy(op.Key, key)
	if value != nil {
		op.Value = make([]byte, len(value))
		copy(op.Value, value)
	}

	b.operands = append(b.operands, &op)
	return nil
}

func (b *WitnessBuilder) addAccountLeafOp(key []byte, accountNode *AccountNode, codeSize int) error {
	if b.trace {
		fmt.Printf("LEAF_ACCOUNT: k %x acc:%v\n", key, accountNode)
	}

	var op OperatorLeafAccount
	op.Key = make([]byte, len(key))
	copy(op.Key, key)

	op.Nonce = accountNode.Nonce
	op.Balance = big.NewInt(0)
	op.Balance.SetBytes(accountNode.Balance.Bytes())

	if !accountNode.IsEmptyRoot() || !accountNode.IsEmptyCodeHash() {
		op.HasCode = true
		op.HasStorage = true
	}

	op.CodeSize = uint64(codeSize)

	b.operands = append(b.operands, &op)

	return nil
}

func (b *WitnessBuilder) addExtensionOp(key []byte) error {
	if b.trace {
		fmt.Printf("EXTENSION: k %x\n", key)
	}

	var op OperatorExtension
	op.Key = make([]byte, len(key))
	copy(op.Key, key)

	b.operands = append(b.operands, &op)
	return nil
}

func (b *WitnessBuilder) makeHashNode(n Node, force bool, hashNodeFunc HashNodeFunc) (HashNode, error) {
	switch n := n.(type) {
	case HashNode:
		return n, nil
	default:
		var hash common.Hash
		if _, err := hashNodeFunc(n, force, hash[:]); err != nil {
			return HashNode{}, err
		}
		return HashNode{hash: hash[:]}, nil
	}
}

func (b *WitnessBuilder) addHashOp(n *HashNode) error {
	if b.trace {
		fmt.Printf("HASH: type: %T v %s\n", n, n)
	}

	var op OperatorHash
	op.Hash = common.BytesToHash(n.hash)

	b.operands = append(b.operands, &op)
	return nil
}

func (b *WitnessBuilder) addBranchOp(mask uint32) error {
	if b.trace {
		fmt.Printf("BRANCH: mask=%b\n", mask)
	}

	var op OperatorBranch
	op.Mask = mask

	b.operands = append(b.operands, &op)
	return nil
}

func (b *WitnessBuilder) addCodeOp(code []byte) error {
	if b.trace {
		fmt.Printf("CODE: len=%d\n", len(code))
	}

	var op OperatorCode
	op.Code = make([]byte, len(code))
	copy(op.Code, code)

	b.operands = append(b.operands, &op)
	return nil
}

func (b *WitnessBuilder) addEmptyRoot() error {
	if b.trace {
		fmt.Printf("EMPTY ROOT\n")
	}

	b.operands = append(b.operands, &OperatorEmptyRoot{})
	return nil
}

func (b *WitnessBuilder) processAccountCode(n *AccountNode, retainDec RetainDecider) (int, error) {
	if n.IsEmptyRoot() && n.IsEmptyCodeHash() {
		return 0, nil
	}

	if n.Code == nil || (retainDec != nil && !retainDec.IsCodeTouched(n.CodeHash)) {
		codeSize := n.CodeSize
		if n.Code != nil {
			codeSize = len(n.Code)
		}
		return codeSize, b.addHashOp(&HashNode{hash: n.CodeHash[:]})
	}

	return len(n.Code), b.addCodeOp(n.Code)
}

func (b *WitnessBuilder) processAccountStorage(n *AccountNode, hex []byte, limiter *MerklePathLimiter) error {
	if n.IsEmptyRoot() && n.IsEmptyCodeHash() {
		return nil
	}

	if n.Storage == nil {
		return b.addEmptyRoot()
	}

	// Here we substitute rs parameter for storageRs, because it needs to become the default
	return b.makeBlockWitness(n.Storage, hex, limiter, true)
}

func (b *WitnessBuilder) makeBlockWitness(
	nd Node, hex []byte, limiter *MerklePathLimiter, force bool) error {

	processAccountNode := func(key []byte, storageKey []byte, n *AccountNode) error {
		var retainDec RetainDecider
		if limiter != nil {
			retainDec = limiter.RetainDecider
		}
		var codeSize int
		var err error
		if codeSize, err = b.processAccountCode(n, retainDec); err != nil {
			return err
		}
		if err := b.processAccountStorage(n, storageKey, limiter); err != nil {
			return err
		}
		return b.addAccountLeafOp(key, n, codeSize)
	}

	switch n := nd.(type) {
	case nil:
		return nil
	case ValueNode:
		return b.addLeafOp(hex, n)
	case *AccountNode:
		return processAccountNode(hex, hex, n)
	case *ShortNode:
		h := n.Key
		// Remove terminator
		if h[len(h)-1] == 16 {
			h = h[:len(h)-1]
		}
		hexVal := concat(hex, h...)
		switch v := n.Val.(type) {
		case ValueNode:
			return b.addLeafOp(n.Key, v[:])
		case *AccountNode:
			return processAccountNode(n.Key, hexVal, v)
		default:
			if err := b.makeBlockWitness(n.Val, hexVal, limiter, false); err != nil {
				return err
			}

			return b.addExtensionOp(n.Key)
		}
	case *DuoNode:
		hashOnly := limiter != nil && !limiter.RetainDecider.Retain(hex) // Save this because rl can move on to other keys during the recursive invocation
		if b.trace {
			fmt.Printf("b.retainDec.Retain(%x) -> %v\n", hex, !hashOnly)
		}
		if hashOnly {
			hn, err := b.makeHashNode(n, force, limiter.HashFunc)
			if err != nil {
				return err
			}
			return b.addHashOp(&hn)
		}

		i1, i2 := n.childrenIdx()

		if err := b.makeBlockWitness(n.child1, expandKeyHex(hex, i1), limiter, false); err != nil {
			return err
		}
		if err := b.makeBlockWitness(n.child2, expandKeyHex(hex, i2), limiter, false); err != nil {
			return err
		}
		return b.addBranchOp(n.mask)

	case *FullNode:
		hashOnly := limiter != nil && !limiter.RetainDecider.Retain(hex) // Save this because rs can move on to other keys during the recursive invocation
		if hashOnly {
			hn, err := b.makeHashNode(n, force, limiter.HashFunc)
			if err != nil {
				return err
			}
			return b.addHashOp(&hn)
		}

		var mask uint32
		for i, child := range n.Children {
			if child != nil {
				if err := b.makeBlockWitness(child, expandKeyHex(hex, byte(i)), limiter, false); err != nil {
					return err
				}
				mask |= uint32(1) << uint(i)
			}
		}
		return b.addBranchOp(mask)

	case *HashNode:
		hashOnly := limiter == nil || !limiter.RetainDecider.Retain(hex)
		if hashOnly {
			return b.addHashOp(n)
		}
		return fmt.Errorf("unexpected hashNode: %s, at hex: %x, (%d), hashOnly: %t", n, hex, len(hex), hashOnly)
	default:
		return fmt.Errorf("unexpected node type: %T", nd)
	}
}

func expandKeyHex(hex []byte, nibble byte) []byte {
	result := make([]byte, len(hex)+1)
	copy(result, hex)
	result[len(hex)] = nibble
	return result
}
