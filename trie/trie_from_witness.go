package trie

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/trie/rlphacks"
)

func BuildTrieFromWitness(witness *Witness, isBinary bool, trace bool) (*Trie, error) {
	hb := NewHashBuilder(trace)
	_, err := buildTrie(hb, witness.Operators, 0, trace)
	if err != nil {
		return nil, err
	}

	if !hb.hasRoot() {
		return nil, errors.New("no root in the trie")
	}

	h, err := hb.RootHash()
	if err != nil {
		return nil, err
	}

	var tr *Trie
	if isBinary {
		tr = NewBinary(h)
	} else {
		tr = New(h)
	}

	tr.root = hb.root()
	return tr, nil
}

func buildTrie(hb *HashBuilder, operators []WitnessOperator, i int, trace bool) (int, error) {
	if trace {
		fmt.Printf("idx=%d:", i)
	}
	operator := operators[i]
	switch op := operator.(type) {
	case *OperatorLeafValue:
		if trace {
			fmt.Printf("LEAF %x ", op.Value)
		}
		keyHex := op.Key
		val := op.Value
		return i + 1, hb.leaf(len(op.Key), keyHex, rlphacks.RlpSerializableBytes(val))

	case *OperatorExtension:
		if trace {
			fmt.Printf("EXTENSION ")
		}
		var err error
		if i, err = buildTrie(hb, operators, i+1, trace); err != nil {
			return i, err
		}
		return i, hb.extension(op.Key)
	case *OperatorBranch:
		if trace {
			fmt.Printf("BRANCH %b ", op.Mask)
		}
		i++

		// To make a prefix witness, we invert the witness
		// that has a side effect: children of a branch node are in reverse order
		// so this code accounts for that

		indexes := make([]uint32, 0)
		var err error
		for j := uint32(0); j < 16; j++ {
			if op.Mask&(uint32(1)<<j) != 0 {
				indexes = append(indexes, j)
			}
		}

		for zz := len(indexes) - 1; zz >= 0; zz-- {
			if i, err = buildTrie(hb, operators, i, trace); err != nil {
				return i, err
			}
		}
		// TODO: support branching in reverse
		return i, hb.branchReversed(uint16(op.Mask), indexes)

	case *OperatorHash:
		if trace {
			fmt.Printf("HASH ")
		}
		return i + 1, hb.hash(op.Hash[:])
	case *OperatorCode:
		if trace {
			fmt.Printf("CODE ")
		}

		return i + 1, hb.code(op.Code)

	case *OperatorLeafAccount:
		if trace {
			fmt.Printf("ACCOUNTLEAF(code=%v storage=%v) ", op.HasCode, op.HasStorage)
		}

		balance := big.NewInt(0)
		balance.SetBytes(op.Balance.Bytes())
		nonce := op.Nonce
		incarnaton := uint64(0)

		// FIXME: probably not needed, fix hb.accountLeaf
		fieldSet := uint32(3)
		if op.HasCode && op.HasStorage {
			fieldSet = 15
		}

		i++

		var err error

		if op.HasCode {
			if i, err = buildTrie(hb, operators, i, trace); err != nil {
				return i, err
			}
		}

		if op.HasStorage {
			if i, err = buildTrie(hb, operators, i, trace); err != nil {
				return i, err
			}
		}

		return i, hb.accountLeafReversed(len(op.Key), op.Key, 0, balance, nonce, incarnaton, fieldSet)

	case *OperatorEmptyRoot:
		if trace {
			fmt.Printf("EMPTYROOT ")
		}
		hb.emptyRoot()
		return i + 1, nil

	default:
		return i + 1, fmt.Errorf("unknown operand type: %T", operator)
	}

}
