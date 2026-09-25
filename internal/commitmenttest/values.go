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

package commitmenttest

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/holiman/uint256"
)

type accountFields uint8

const (
	BalanceField accountFields = 1 << iota
	NonceField
	CodeField
	allFields = BalanceField | NonceField | CodeField
)

type AccountValue struct {
	Fields   accountFields
	Nonce    uint64
	Balance  uint256.Int
	CodeHash common.Hash
}

type AccountSpec struct {
	Kind     string
	Number   int
	Nonce    uint64
	Balance  uint64
	CodeHash common.Hash
}

func Account(spec AccountSpec) AccountValue {
	value := AccountValue{Fields: allFields, Nonce: spec.Nonce, Balance: *uint256.NewInt(spec.Balance), CodeHash: spec.CodeHash}
	switch spec.Kind {
	case "parity":
		value.Nonce = uint64(spec.Number + 1)
		value.Balance.SetUint64(uint64(spec.Number + 1))
		value.CodeHash = common.HexToHash(fmt.Sprintf("0x%064x", spec.Number+1))
	case "plain":
		value.Nonce = uint64(spec.Number + 1)
		value.Balance.SetUint64(uint64(spec.Number + 1))
		value.CodeHash = empty.CodeHash
	case "fold":
		value.Nonce = uint64(spec.Number)
		value.Balance.SetUint64(uint64(spec.Number * 3))
		value.CodeHash = empty.CodeHash
	case "", "full":
	default:
		panic("unknown account kind: " + spec.Kind)
	}
	return value
}

func RandomAccount(rng *rand.Rand) AccountValue {
	value := AccountValue{Fields: allFields, Nonce: uint64(rng.Intn(1 << 20)), Balance: *uint256.NewInt(rng.Uint64()), CodeHash: empty.CodeHash}
	if rng.Intn(3) == 0 {
		value.CodeHash = common.BigToHash(uint256.NewInt(rng.Uint64()).ToBig())
	}
	return value
}

func SizedAccount(number int, contract bool, rng *rand.Rand) (AccountValue, []byte) {
	value := AccountValue{Fields: BalanceField | NonceField, CodeHash: empty.CodeHash}
	if !contract {
		value.Nonce = uint64(rng.Intn(500))
		value.Balance.SetUint64(uint64(rng.Int63n(4e18)))
		return value, bytes.Clone(empty.RootHash[:])
	}
	value.Fields = allFields
	value.Nonce = 1
	value.Balance.SetUint64(uint64(rng.Int63n(1e15)))
	value.CodeHash = common.HexToHash(fmt.Sprintf("0x%064x", number+7))
	root := make([]byte, 32)
	_, _ = rng.Read(root)
	return value, root
}

type StorageSpec struct {
	Number int
	Value  []byte
	Path   []byte
}

func Storage(spec StorageSpec) []byte {
	if spec.Value != nil {
		return bytes.Clone(spec.Value)
	}
	if spec.Path != nil {
		return []byte{spec.Path[0] + 1, spec.Path[1] + 1, spec.Path[2] + 1, spec.Path[3] + 1}
	}
	return []byte{byte(spec.Number), byte(spec.Number >> 8)}
}

func RandomStorage(rng *rand.Rand) []byte {
	value := make([]byte, 1+rng.Intn(32))
	_, _ = rng.Read(value)
	value[0] |= 1
	return value
}

func Read(tb testing.TB, value []byte, read func([]byte) (int, error)) {
	tb.Helper()
	n, err := read(value)
	require.NoError(tb, err)
	require.Equal(tb, len(value), n)
}
