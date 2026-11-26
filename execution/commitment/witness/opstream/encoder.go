// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package opstream

import (
	"fmt"
	"math"
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type CodeWithHash struct {
	Code     []byte
	CodeHash accounts.CodeHash
}

func Encode(nodes [][]byte, codeReads map[common.Hash]CodeWithHash) (*Witness, error) {
	if len(nodes) == 0 {
		return NewWitness(nil), nil
	}

	byHash := make(map[common.Hash][]byte, len(nodes))
	for _, node := range nodes {
		if len(node) == 0 {
			return nil, fmt.Errorf("witness node is empty")
		}
		if len(node) == 1 && node[0] == 0x80 {
			continue
		}
		byHash[crypto.Keccak256Hash(node)] = node
	}

	d := nodeEncoder{byHash: byHash, codeReads: codeReads}
	operators, err := d.node(nodes[0], nil, false)
	if err != nil {
		return nil, err
	}
	return NewWitness(operators), nil
}

type nodeEncoder struct {
	byHash    map[common.Hash][]byte
	codeReads map[common.Hash]CodeWithHash
}

func (d *nodeEncoder) node(encoded []byte, path []byte, storage bool) ([]WitnessOperator, error) {
	elements, rest, err := rlp.SplitList(encoded)
	if err != nil {
		return nil, err
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("trie node has trailing data")
	}
	count, err := rlp.CountValues(elements)
	if err != nil {
		return nil, err
	}
	switch count {
	case 2:
		return d.short(elements, path, storage)
	case 17:
		return d.branch(elements, path, storage)
	default:
		return nil, fmt.Errorf("invalid number of trie node elements: %d", count)
	}
}

func (d *nodeEncoder) short(elements []byte, path []byte, storage bool) ([]WitnessOperator, error) {
	compactKey, rest, err := rlp.SplitString(elements)
	if err != nil {
		return nil, err
	}
	key := nibbles.CompactToHex(compactKey)
	terminating := len(compactKey) > 0 && compactKey[0]&0x20 != 0
	if terminating {
		value, trailing, err := rlp.SplitString(rest)
		if err != nil {
			return nil, err
		}
		if len(trailing) != 0 {
			return nil, fmt.Errorf("leaf has trailing data")
		}
		if !storage {
			account := new(accounts.Account)
			if err := account.DecodeForHashing(value); err == nil {
				return d.account(key, slices.Concat(path, key[:len(key)-1]), account)
			}
		}
		return []WitnessOperator{&OperatorLeafValue{Key: key, Value: decodeValue(value)}}, nil
	}

	child, trailing, err := splitReference(rest)
	if err != nil {
		return nil, err
	}
	if len(trailing) != 0 {
		return nil, fmt.Errorf("extension has trailing data")
	}
	childPath := slices.Concat(path, key)
	operators, err := d.reference(child, childPath, storage)
	if err != nil {
		return nil, err
	}
	return append(operators, &OperatorExtension{Key: key}), nil
}

func (d *nodeEncoder) branch(elements []byte, path []byte, storage bool) ([]WitnessOperator, error) {
	operators := make([]WitnessOperator, 0)
	var mask uint32
	for i := range 16 {
		item, rest, err := splitReference(elements)
		if err != nil {
			return nil, err
		}
		elements = rest
		childPath := slices.Concat(path, []byte{byte(i)})
		childOperators, err := d.reference(item, childPath, storage)
		if err != nil {
			return nil, err
		}
		if len(childOperators) == 0 {
			continue
		}
		operators = append(operators, childOperators...)
		mask |= uint32(1) << uint(i)
	}

	value, rest, err := rlp.SplitString(elements)
	if err != nil {
		return nil, err
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("branch has trailing data")
	}
	if len(value) != 0 {
		operators = append(operators, &OperatorLeafValue{Key: append([]byte(nil), path...), Value: decodeValue(value)})
		mask |= 1 << 16
	}
	return append(operators, &OperatorBranch{Mask: mask}), nil
}

func (d *nodeEncoder) reference(encoded []byte, path []byte, storage bool) ([]WitnessOperator, error) {
	kind, value, rest, err := rlp.Split(encoded)
	if err != nil {
		return nil, err
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("trie reference has trailing data")
	}
	switch kind {
	case rlp.List:
		return d.node(encoded, path, storage)
	case rlp.String:
		if len(value) == 0 {
			return nil, nil
		}
		if len(value) != len(common.Hash{}) {
			return nil, fmt.Errorf("invalid trie reference length: %d", len(value))
		}
		hash := common.BytesToHash(value)
		child, ok := d.byHash[hash]
		if !ok {
			return []WitnessOperator{&OperatorHash{Hash: hash}}, nil
		}
		return d.node(child, path, storage)
	default:
		return nil, fmt.Errorf("invalid trie reference kind: %d", kind)
	}
}

func (d *nodeEncoder) account(key, path []byte, account *accounts.Account) ([]WitnessOperator, error) {
	operators := make([]WitnessOperator, 0)
	emptyAccount := account.Root == empty.RootHash || account.Root == (common.Hash{})
	emptyCode := account.IsEmptyCodeHash()
	codeSize := uint64(0)
	accountHashBytes := nibbles.HexToKeybytes(path)
	accountHash := common.BytesToHash(accountHashBytes)
	codeRead, hasCodeRead := d.codeReads[accountHash]
	if hasCodeRead && len(codeRead.Code) > 0 && crypto.Keccak256Hash(codeRead.Code) != account.CodeHash.Value() {
		return nil, fmt.Errorf("inserted code mismatch account hash (acc.CodeHash=%x codeHash=%x)", account.CodeHash.Value(), crypto.Keccak256Hash(codeRead.Code))
	}
	if emptyAccount && emptyCode {
		return []WitnessOperator{accountLeaf(key, account, codeSize)}, nil
	}

	if !emptyCode {
		if hasCodeRead && len(codeRead.Code) > 0 {
			operators = append(operators, &OperatorCode{Code: append([]byte(nil), codeRead.Code...)})
			codeSize = uint64(len(codeRead.Code))
		} else {
			operators = append(operators, &OperatorHash{Hash: account.CodeHash.Value()})
			codeSize = math.MaxUint64
		}
	} else {
		operators = append(operators, &OperatorHash{Hash: account.CodeHash.Value()})
	}

	if emptyAccount {
		operators = append(operators, &OperatorEmptyRoot{})
	} else if storageNode, ok := d.byHash[account.Root]; ok {
		storageOperators, err := d.node(storageNode, path, true)
		if err != nil {
			return nil, err
		}
		operators = append(operators, storageOperators...)
	} else {
		operators = append(operators, &OperatorHash{Hash: account.Root})
	}
	return append(operators, accountLeaf(key, account, codeSize)), nil
}

func accountLeaf(key []byte, account *accounts.Account, codeSize uint64) *OperatorLeafAccount {
	return &OperatorLeafAccount{
		Key:        append([]byte(nil), key...),
		Nonce:      account.Nonce,
		Balance:    account.Balance.ToBig(),
		HasCode:    account.Root != empty.RootHash && account.Root != (common.Hash{}) || !account.IsEmptyCodeHash(),
		HasStorage: account.Root != empty.RootHash && account.Root != (common.Hash{}) || !account.IsEmptyCodeHash(),
		CodeSize:   codeSize,
	}
}

func splitReference(encoded []byte) ([]byte, []byte, error) {
	_, _, rest, err := rlp.Split(encoded)
	if err != nil {
		return nil, nil, err
	}
	return encoded[:len(encoded)-len(rest)], rest, nil
}

func decodeValue(encoded []byte) []byte {
	value, rest, err := rlp.SplitString(encoded)
	if err == nil && len(rest) == 0 {
		return value
	}
	return encoded
}
