package trie

import (
	"fmt"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/trie/rlphacks"
)

func BuildTrieFromWitness(witness *Witness, isBinary bool, trace bool) (*Trie, CodeMap, error) {
	codeMap := make(map[common.Hash][]byte)
	hb := NewHashBuilder(false)
	for _, operand := range witness.Operands {
		switch op := operand.(type) {
		case *OperatorLeafValue:
			if trace {
				fmt.Printf("LEAF ")
			}
			keyHex := op.Key
			val := op.Value
			if err := hb.leaf(len(op.Key), keyHex, rlphacks.RlpSerializableBytes(val)); err != nil {
				return nil, nil, err
			}
		case *OperatorExtension:
			if trace {
				fmt.Printf("EXTENSION ")
			}
			if err := hb.extension(op.Key); err != nil {
				return nil, nil, err
			}
		case *OperatorBranch:
			if trace {
				fmt.Printf("BRANCH ")
			}
			if err := hb.branch(uint16(op.Mask)); err != nil {
				return nil, nil, err
			}
		case *OperatorHash:
			if trace {
				fmt.Printf("HASH ")
			}
			if err := hb.hash(op.Hash); err != nil {
				return nil, nil, err
			}
		case *OperatorCode:
			if trace {
				fmt.Printf("CODE ")
			}

			if codeHash, err := hb.code(op.Code); err == nil {
				codeMap[codeHash] = op.Code
			} else {
				return nil, nil, err
			}

		case *OperatorLeafAccount:
			if trace {
				fmt.Printf("ACCOUNTLEAF(code=%v storage=%v) ", op.HasCode, op.HasStorage)
			}
			balance := big.NewInt(0)
			balance.SetBytes(op.Balance.Bytes())
			nonce := op.Nonce

			// FIXME: probably not needed, fix hb.accountLeaf
			fieldSet := uint32(3)
			if op.HasCode && op.HasStorage {
				fieldSet = 15
			}

			if err := hb.accountLeaf(len(op.Key), op.Key, 0, balance, nonce, fieldSet); err != nil {
				return nil, nil, err
			}
		case *OperatorEmptyRoot:
			if trace {
				fmt.Printf("EMPTYROOT ")
			}
			hb.emptyRoot()
		default:
			return nil, nil, fmt.Errorf("unknown operand type: %T", operand)
		}
	}
	if trace {
		fmt.Printf("\n")
	}
	r := hb.root()
	var tr *Trie
	if isBinary {
		tr = NewBinary(hb.rootHash())
	} else {
		tr = New(hb.rootHash())
	}
	tr.root = r
	return tr, codeMap, nil
}
