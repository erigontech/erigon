package trie

import (
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/rlp"
)

func BuildTrieFromWitness(witness *Witness, trace bool) (*Trie, error) {
	hb := NewHashBuilder(false)
	for _, operator := range witness.Operators {
		switch op := operator.(type) {
		case *OperatorLeafValue:
			if trace {
				fmt.Printf("LEAF ")
			}
			keyHex := op.Key
			val := op.Value
			if err := hb.leaf(len(op.Key), keyHex, rlp.RlpSerializableBytes(val)); err != nil {
				return nil, err
			}
		case *OperatorExtension:
			if trace {
				fmt.Printf("EXTENSION ")
			}
			if err := hb.extension(op.Key); err != nil {
				return nil, err
			}
		case *OperatorBranch:
			if trace {
				fmt.Printf("BRANCH ")
			}
			if err := hb.branch(uint16(op.Mask)); err != nil {
				return nil, err
			}
		case *OperatorHash:
			if trace {
				fmt.Printf("HASH ")
			}
			if err := hb.hash(op.Hash[:]); err != nil {
				return nil, err
			}
		case *OperatorCode:
			if trace {
				fmt.Printf("CODE ")
			}

			if err := hb.code(op.Code); err != nil {
				return nil, err
			}

		case *OperatorLeafAccount:
			if trace {
				fmt.Printf("ACCOUNTLEAF(code=%v storage=%v) ", op.HasCode, op.HasStorage)
			}
			balance := uint256.NewInt(0)
			balance.SetBytes(op.Balance.Bytes())
			nonce := op.Nonce

			// FIXME: probably not needed, fix hb.accountLeaf
			fieldSet := uint32(3)
			if op.HasCode && op.HasStorage {
				fieldSet = 15
			}

			// Incarnation is always needed for a hashbuilder.
			// but it is just our implementation detail needed for contract self-descruction support with our
			// db structure. Stateless clients don't access the DB so we can just pass 0 here.
			incarnation := uint64(0)

			if err := hb.accountLeaf(len(op.Key), op.Key, balance, nonce, incarnation, fieldSet, int(op.CodeSize)); err != nil {
				return nil, err
			}
		case *OperatorEmptyRoot:
			if trace {
				fmt.Printf("EMPTYROOT ")
			}
			hb.emptyRoot()
		default:
			return nil, fmt.Errorf("unknown operand type: %T", operator)
		}
	}
	if trace {
		fmt.Printf("\n")
	}
	if !hb.hasRoot() {
		return New(EmptyRoot), nil
	}
	r := hb.root()
	tr := New(hb.rootHash())
	tr.RootNode = r
	return tr, nil
}
