package trie

import (
	"fmt"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/common"
)

type HashNodeFunc func(node, bool, []byte) (int, error)

type CodeMap map[common.Hash][]byte

type WitnessBuilder struct {
	t        *Trie
	blockNr  uint64
	trace    bool
	rs       *ResolveSet
	codeMap  CodeMap
	operands []WitnessOperator
}

func NewWitnessBuilder(t *Trie, blockNr uint64, trace bool, rs *ResolveSet, codeMap CodeMap) *WitnessBuilder {
	return &WitnessBuilder{
		t:        t,
		blockNr:  blockNr,
		trace:    trace,
		rs:       rs,
		codeMap:  codeMap,
		operands: make([]WitnessOperator, 0),
	}
}

func (b *WitnessBuilder) Build(hashNodeFunc HashNodeFunc) (*Witness, error) {
	err := b.makeBlockWitness(b.t.root, []byte{}, hashNodeFunc, true)
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
	copy(op.Key[:], key)
	if value != nil {
		op.Value = make([]byte, len(value))
		copy(op.Value[:], value)
	}

	b.operands = append(b.operands, &op)
	return nil
}

func (b *WitnessBuilder) addAccountLeafOp(key []byte, accountNode *accountNode) error {
	if b.trace {
		fmt.Printf("LEAF_ACCOUNT: k %x acc:%v\n", key, accountNode)
	}

	var op OperatorLeafAccount
	op.Key = make([]byte, len(key))
	copy(op.Key[:], key)

	op.Nonce = accountNode.Nonce
	op.Balance = big.NewInt(0)
	op.Balance.SetBytes(accountNode.Balance.Bytes())

	if !accountNode.IsEmptyRoot() || !accountNode.IsEmptyCodeHash() {
		op.HasCode = true
		op.HasStorage = true
	}

	b.operands = append(b.operands, &op)

	return nil
}

func (b *WitnessBuilder) addExtensionOp(key []byte) error {
	if b.trace {
		fmt.Printf("EXTENSION: k %x\n", key)
	}

	var op OperatorExtension
	op.Key = make([]byte, len(key))
	copy(op.Key[:], key)

	b.operands = append(b.operands, &op)
	return nil
}

func (b *WitnessBuilder) addHashOp(n node, force bool, hashNodeFunc HashNodeFunc) error {
	var hash common.Hash
	switch n := n.(type) {
	case hashNode:
		copy(hash[:], n[:])
	default:
		if _, err := hashNodeFunc(n, force, hash[:]); err != nil {
			return err
		}
	}

	if b.trace {
		fmt.Printf("HASH: type: %T v %x\n", n, hash)
	}

	var op OperatorHash
	op.Hash = hash

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

func (b *WitnessBuilder) processAccountCode(n *accountNode) error {
	if n.IsEmptyRoot() && n.IsEmptyCodeHash() {
		return nil
	}

	code, ok := b.codeMap[n.CodeHash]
	if !ok {
		// FIXME: these parameters aren't harmful, but probably addHashOp does too much
		return b.addHashOp(hashNode(n.CodeHash[:]), false, nil)
	}

	return b.addCodeOp(code)
}

func (b *WitnessBuilder) processAccountStorage(n *accountNode, hex []byte, hashNodeFunc HashNodeFunc) error {
	if n.IsEmptyRoot() && n.IsEmptyCodeHash() {
		return nil
	}

	if n.storage == nil {
		return b.addEmptyRoot()
	}

	// Here we substitute rs parameter for storageRs, because it needs to become the default
	return b.makeBlockWitness(n.storage, hex, hashNodeFunc, true)
}

func (b *WitnessBuilder) makeBlockWitness(
	nd node, hex []byte, hashNodeFunc HashNodeFunc, force bool) error {

	processAccountNode := func(key []byte, storageKey []byte, n *accountNode) error {
		if err := b.processAccountCode(n); err != nil {
			return err
		}
		if err := b.processAccountStorage(n, storageKey, hashNodeFunc); err != nil {
			return err
		}
		return b.addAccountLeafOp(key, n)
	}

	switch n := nd.(type) {
	case nil:
		return nil
	case valueNode:
		return b.addLeafOp(hex, n)
	case *accountNode:
		return processAccountNode(hex, hex, n)
	case *shortNode:
		h := n.Key
		// Remove terminator
		if h[len(h)-1] == 16 {
			h = h[:len(h)-1]
		}
		hexVal := concat(hex, h...)
		switch v := n.Val.(type) {
		case valueNode:
			return b.addLeafOp(n.Key, v[:])
		case *accountNode:
			return processAccountNode(n.Key, hexVal, v)
		default:
			if err := b.makeBlockWitness(n.Val, hexVal, hashNodeFunc, false); err != nil {
				return err
			}

			return b.addExtensionOp(n.Key)
		}
	case *duoNode:
		hashOnly := b.rs.HashOnly(hex) // Save this because rs can move on to other keys during the recursive invocation
		if b.trace {
			fmt.Printf("b.rs.HashOnly(%x) -> %v\n", hex, hashOnly)
		}
		if hashOnly {
			return b.addHashOp(n, force, hashNodeFunc)
		}

		i1, i2 := n.childrenIdx()

		if err := b.makeBlockWitness(n.child1, expandKeyHex(hex, i1), hashNodeFunc, false); err != nil {
			return err
		}
		if err := b.makeBlockWitness(n.child2, expandKeyHex(hex, i2), hashNodeFunc, false); err != nil {
			return err
		}
		return b.addBranchOp(n.mask)

	case *fullNode:
		hashOnly := b.rs.HashOnly(hex) // Save this because rs can move on to other keys during the recursive invocation
		if hashOnly {
			return b.addHashOp(n, force, hashNodeFunc)
		}

		var mask uint32
		for i, child := range n.Children {
			if child != nil {
				if err := b.makeBlockWitness(child, expandKeyHex(hex, byte(i)), hashNodeFunc, false); err != nil {
					return err
				}
				mask |= (uint32(1) << uint(i))
			}
		}
		return b.addBranchOp(mask)

	case hashNode:
		hashOnly := b.rs.HashOnly(hex)
		if !hashOnly {
			if c := b.rs.Current(); len(c) == len(hex)+1 && c[len(c)-1] == 16 {
				hashOnly = true
			}
		}
		if hashOnly {
			return b.addHashOp(n, force, hashNodeFunc)
		}
		return fmt.Errorf("unexpected hashNode: %s, at hex: %x, rs.Current: %x (%d)", n, hex, b.rs.Current(), len(hex))
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
