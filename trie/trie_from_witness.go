package trie

import (
	"fmt"
	"math/big"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/crypto"
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
			fmt.Printf("LEAF %x ", op.Value)
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

		return codeNode(common.CopyBytes(op.Code)), i + 1, nil

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
		account.Root = EmptyRoot
		account.HasStorageSize = false

		var err error

		i++

		// storage and code subtries now are also inversed
		var storage node
		if op.HasStorage {
			storage, i, err = buildTrie(operators, i, trace)
			if trace {
				fmt.Printf("\n >>> account.storage = %v\n", storage.fstring(""))
			}
			if storage != nil {
				account.StorageSize = 0
				hasher := newHasher(false)
				defer returnHasherToPool(hasher)
				var h common.Hash
				_, err := hasher.hash(storage, true, h[:])
				if err != nil {
					panic(err)
				}
				if trace {
					fmt.Printf("\n >>> account.root = %x\n", h)
				}
				account.Root = h
			}
		}

		var cnode codeNode
		if op.HasCode {
			var code node
			code, i, err = buildTrie(operators, i, trace)
			if trace {
				fmt.Printf("got code! %v\n", code)
			}
			switch cn := code.(type) {
			case codeNode:
				hval := crypto.Keccak256Hash(cn[:])
				account.CodeHash = hval
				cnode = cn
			case hashNode:
				account.CodeHash = common.BytesToHash(cn[:])
			default:
				panic("kaboom!")
			}
			if trace {
				fmt.Printf("codeHash = %x\n", account.CodeHash)
			}
		}

		accountNode := &accountNode{*account, storage, true, cnode, len(cnode)}
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
