package trie

import (
	"fmt"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
)

func BuildTrieFromWitness(witness *Witness, isBinary bool, trace bool) (*Trie, error) {
	r, _, err := buildTrie(witness.Operators, 0, trace)
	if err != nil {
		return nil, err
	}

	hasher := newHasher(false)
	defer returnHasherToPool(hasher)

	var h common.Hash
	_, err = hasher.hash(r, true, h[:])
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

func buildTrie(operators []WitnessOperator, i int, trace bool) (node, int, error) {
	if trace {
		fmt.Printf("idx=%d:", i)
	}
	operator := operators[i]
	switch op := operator.(type) {
	case *OperatorLeafValue:
		if trace {
			fmt.Printf("LEAF ")
		}
		keyHex := op.Key
		val := op.Value
		return &shortNode{Key: common.CopyBytes(keyHex), Val: valueNode(val)}, i + 1, nil

	case *OperatorExtension:
		if trace {
			fmt.Printf("EXTENSION ")
		}
		val, newi, err := buildTrie(operators, i+1, trace)
		return &shortNode{Key: common.CopyBytes(op.Key), Val: val}, newi, err
	case *OperatorBranch:
		if trace {
			fmt.Printf("BRANCH %b ", op.Mask)
		}
		// FIXME: support duoNode

		branchNode := &fullNode{}
		i++

		// To make a prefix witness, we invert the witness
		// that has a side effect: children of a branch node are in reverse order
		// so this code accounts for that

		var err error
		indexes := make([]uint32, 0)
		for j := uint32(0); j < 16; j++ {
			if op.Mask&(uint32(1)<<j) != 0 {
				indexes = append(indexes, j)
			}
		}

		for zz := len(indexes) - 1; zz >= 0; zz-- {
			var child node
			child, i, err = buildTrie(operators, i, trace)
			branchNode.Children[indexes[zz]] = child
		}

		return branchNode, i, err
	case *OperatorHash:
		if trace {
			fmt.Printf("HASH ")
		}
		hn := hashNode(op.Hash[:])
		return hn, i + 1, nil
	case *OperatorCode:
		if trace {
			fmt.Printf("CODE ")
		}

		return codeNode(op.Code), i + 1, nil

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
		account.CodeHash = EmptyCodeHash

		var err error
		var code node

		i++

		// storage and code subtries now are also inversed
		var storage node
		if op.HasStorage {
			storage, i, err = buildTrie(operators, i, trace)
			if storage == nil {
				account.Root = EmptyRoot
			} else {
				account.StorageSize = 0
				account.HasStorageSize = true
				hasher := newHasher(false)
				defer returnHasherToPool(hasher)
				var h common.Hash
				_, err := hasher.hash(storage, true, h[:])
				if err != nil {
					panic(err)
				}
				account.Root = h
			}
		} else {
			account.Root = EmptyRoot
		}

		if op.HasCode {
			code, i, err = buildTrie(operators, i, trace)
			hasher := newHasher(false)
			defer returnHasherToPool(hasher)
			var h common.Hash
			_, err := hasher.hash(code, true, h[:])
			if err != nil {
				panic(err)
			}
			account.CodeHash = h
		}

		var cn codeNode
		if code != nil {
			cn, _ = code.(codeNode)
		}

		accountNode := &accountNode{*account, storage, true, cn, len(cn)}
		return &shortNode{Key: common.CopyBytes(op.Key), Val: accountNode}, i, err

	case *OperatorEmptyRoot:
		if trace {
			fmt.Printf("EMPTYROOT ")
		}
		return nil, i + 1, nil
	default:
		return nil, i + 1, fmt.Errorf("unknown operand type: %T", operator)
	}

}
