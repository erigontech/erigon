package trie

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
)

func BuildTrieFromWitness(witness *Witness, isBinary bool, trace bool) (*Trie, error) {
	r, h, _, err := buildTrie(witness.Operators, 0, trace)
	if err != nil {
		return nil, err
	}

	var tr *Trie
	if isBinary {
		tr = NewBinary(h)
	} else {
		tr = New(h)
	}
	tr.root = r
	return tr, nil
}

// TODO: hashes
func buildTrie(operators []WitnessOperator, i int, trace bool) (node, common.Hash, int, error) {
	if trace {
		fmt.Printf("idx=%d", i)
	}
	operator := operators[i]
	switch op := operator.(type) {
	case *OperatorLeafValue:
		if trace {
			fmt.Printf("LEAF ")
		}
		keyHex := op.Key
		val := op.Value
		return &shortNode{Key: keyHex, Val: valueNode(val)}, common.Hash{}, i + 1, nil

	case *OperatorExtension:
		if trace {
			fmt.Printf("EXTENSION ")
		}
		val, h, newi, err := buildTrie(operators, i+1, trace)
		return &shortNode{Key: op.Key, Val: val}, h, newi, err
	case *OperatorBranch:
		if trace {
			fmt.Printf("BRANCH ")
		}
		// FIXME: support duoNode

		branchNode := &fullNode{}
		i++

		var err error
		for j := 0; i <= 16; i++ {
			if op.Mask&(1<<j) > 0 {
				var child node
				child, _, i, err = buildTrie(operators, i, trace)
				branchNode.Children[j] = child
			}
		}

		return branchNode, common.Hash{}, i, err
	case *OperatorHash:
		if trace {
			fmt.Printf("HASH ")
		}
		hn := hashNode(op.Hash[:])
		return hn, op.Hash, i + 1, nil
	case *OperatorCode:
		if trace {
			fmt.Printf("CODE ")
		}

		return codeNode(op.Code), common.Hash{}, i + 1, nil

	case *OperatorLeafAccount:
		if trace {
			fmt.Printf("ACCOUNTLEAF(code=%v storage=%v) ", op.HasCode, op.HasStorage)
		}

		account := &accounts.Account{}

		account.Nonce = op.Nonce
		account.Incarnation = uint64(0)

		balance := big.NewInt(0)
		balance.SetBytes(op.Balance.Bytes())
		account.Balance = *balance
		account.Initialised = true

		var err error
		var code node

		i++
		if op.HasCode {
			code, _, i, err = buildTrie(operators, i, trace)
		}

		var storage node
		if op.HasStorage {
			storage, h, i, err = buildTrie(operators, i, trace)
			account.StorageSize = 0
			account.HasStorageSize = true
			account.Root = h
		} else {
			account.Root = EmptyRoot
		}

		var cn codeNode
		if code != nil {
			ok := false
			cn, ok = code.(codeNode)
			if !ok {
				return nil, common.Hash{}, i, errors.New("broken witness")
			}
		}

		accountNode := &accountNode{*account, storage, true, cn, len(cn)}
		return accountNode, common.Hash{}, i, err

	case *OperatorEmptyRoot:
		if trace {
			fmt.Printf("EMPTYROOT ")
		}
		return nil, EmptyRoot, i + 1, nil
	default:
		return nil, common.Hash{}, i + 1, fmt.Errorf("unknown operand type: %T", operator)
	}

}
