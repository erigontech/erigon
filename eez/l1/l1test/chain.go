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
package l1test

import (
	"crypto/ecdsa"
	"encoding/binary"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/recording"
	"github.com/erigontech/erigon/execution/types"
)

// Sample log emitters and topics.
var (
	EmitterA = common.HexToAddress("0x00000000000000000000000000000000000000aa")
	EmitterB = common.HexToAddress("0x00000000000000000000000000000000000000bb")
	TopicX   = crypto.Keccak256Hash([]byte("X(uint256)"))
	TopicY   = crypto.Keccak256Hash([]byte("Y(uint256)"))
)

// SampleFilters are the queries Conformance issues against SampleChain.
var SampleFilters = []Filter{
	{Address: EmitterA},
	{Address: EmitterA, Topics: [][]common.Hash{{TopicX}}},
	{Address: EmitterB, Topics: [][]common.Hash{{TopicX, TopicY}}},
}

// Registry answers rollups(uint64) for any rollup with RegisteredRollup,
// the last word stored in it and a zero balance, and reverts a 4-byte input
// with RevertReason; any other call stores its first input word.
var (
	Registry         = common.HexToAddress("0x00000000000000000000000000000000000000cc")
	RegisteredRollup = common.HexToAddress("0x00000000000000000000000000000000000000c1")
	// NoCode is an account without code.
	NoCode = common.HexToAddress("0x000000000000000000000000000000000000dead")
	// RollupsCall is rollups(1) on Registry.
	RollupsCall = l1.Call{To: Registry, Data: hexutil.MustDecode("0xef678d270000000000000000000000000000000000000000000000000000000000000001")}
	// RevertingCall is a call Registry reverts.
	RevertingCall = l1.Call{To: Registry, Data: []byte{0xde, 0xad, 0xbe, 0xef}}
	// RevertReason is Error("rollup not found"): `cast calldata "Error(string)" "rollup not found"`.
	RevertReason = hexutil.MustDecode("0x08c379a0" +
		"0000000000000000000000000000000000000000000000000000000000000020" +
		"0000000000000000000000000000000000000000000000000000000000000010" +
		"726f6c6c7570206e6f7420666f756e6400000000000000000000000000000000")
)

// registryCode is Registry's code; a 36-byte input is a rollups call.
var registryCode = append([]byte{
	0x36, 0x60, 0x04, 0x14, 0x60, 0x26, 0x57, // CALLDATASIZE PUSH1 4 EQ PUSH1 revert JUMPI
	0x36, 0x60, 0x24, 0x14, 0x60, 0x15, 0x57, // CALLDATASIZE PUSH1 36 EQ PUSH1 view JUMPI
	0x60, 0x00, 0x35, 0x60, 0x00, 0x55, 0x00, // sstore(0, calldataload(0)) STOP
	0x5b, 0x60, 0xc1, 0x60, 0x00, 0x52, // view: JUMPDEST mstore(0, 0xc1)
	0x60, 0x00, 0x54, 0x60, 0x20, 0x52, // mstore(32, sload(0))
	0x60, 0x60, 0x60, 0x00, 0xf3, // return(0, 96)
	0x5b, 0x60, 0x64, 0x60, 0x33, 0x60, 0x00, 0x39, // revert: JUMPDEST codecopy(0, reason, 100)
	0x60, 0x64, 0x60, 0x00, 0xfd, // revert(0, 100)
}, RevertReason...)

// Created is the contract the sender's transaction of nonce 12 deploys in
// block CreatedAt; its code is createdCode.
var (
	Created     = types.CreateAddress(sampleSender, 12)
	createdCode = []byte{0x60, 0x00, 0x60, 0x00, 0xf3} // return(0, 0)
	// createCode deploys createdCode.
	createCode = append([]byte{0x60, 0x05, 0x60, 0x0c, 0x60, 0x00, 0x39, 0x60, 0x05, 0x60, 0x00, 0xf3}, createdCode...)
)

// CreatedAt is the block whose fourth transaction creates Created.
const CreatedAt = 4

// sampleKey signs the transactions of both sample chains.
const sampleKey = "8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a"

var sampleSender = func() common.Address {
	key, err := crypto.HexToECDSA(sampleKey)
	if err != nil {
		panic(err)
	}
	return crypto.PubkeyToAddress(key.PublicKey)
}()

// RollupsReturn is Registry's answer to a rollups call while it holds root.
func RollupsReturn(root common.Hash) []byte {
	out := make([]byte, 96)
	copy(out[12:32], RegisteredRollup[:])
	copy(out[32:64], root[:])
	return out
}

// withState gives b the answers of the state reads Conformance makes on a
// block after which Registry holds root.
func withState(b *recording.Block, root common.Hash) *recording.Block {
	b.Code = map[common.Address][]byte{Registry: registryCode, EmitterA: emitterACode, NoCode: {}, Created: {}}
	if b.Header.Number >= CreatedAt {
		b.Code[Created] = createdCode
	}
	b.Calls = []recording.CallResult{
		{Call: RollupsCall, Return: RollupsReturn(root)},
		{Call: l1.Call{To: NoCode, Data: RollupsCall.Data}, Return: []byte{}},
		{Call: RevertingCall, Return: RevertReason, Reverted: true},
	}
	return b
}

const sampleChainID = 7331

// SampleChain returns a chain of 11 blocks whose last three replaced an
// earlier branch, with block 4 finalized. Every block carries transactions
// and logs of both emitters, and the state reads Conformance makes.
func SampleChain(t *testing.T) *recording.MemoryChain {
	t.Helper()
	key, err := crypto.HexToECDSA(sampleKey)
	require.NoError(t, err)
	genesis := withState(&recording.Block{Header: l1.Header{Hash: sampleHash(0, 0)}}, common.Hash{})
	c, err := recording.NewMemoryChain(sampleChainID, genesis)
	require.NoError(t, err)

	nonce := uint64(0)
	block := func(parent l1.Header, branch byte) *recording.Block {
		n := parent.Number + 1
		b := &recording.Block{Header: l1.Header{Number: n, Hash: sampleHash(n, branch), ParentHash: parent.Hash, Time: 12 * n}}
		withState(b, crypto.Keccak256Hash(b.Header.Hash[:]))
		for i := range 2 {
			b.Txs = append(b.Txs, sampleTx(t, key, nonce))
			nonce++
			value := common.BigToHash(new(uint256.Int).SetUint64(n*10 + uint64(i)).ToBig())
			b.Logs = append(b.Logs,
				&types.Log{Address: EmitterA, Topics: []common.Hash{TopicX, value}, Data: []byte{branch, byte(i)}, TxIndex: hexutil.Uint(i)},
				&types.Log{Address: EmitterB, Topics: []common.Hash{TopicY}, Data: []byte{byte(n)}, TxIndex: hexutil.Uint(i)},
			)
		}
		return b
	}
	parent := genesis.Header
	replaced := make([]*recording.Block, 0, 10)
	for range 10 {
		b := block(parent, 0)
		require.NoError(t, c.Append(b))
		replaced = append(replaced, b)
		parent = b.Header
	}
	require.NoError(t, c.Finalize(4))
	parent = replaced[6].Header
	branch := make([]*recording.Block, 0, 4)
	for range 4 {
		b := block(parent, 1)
		branch = append(branch, b)
		parent = b.Header
	}
	require.NoError(t, c.Reorg(7, branch...))
	return c
}

func sampleHash(number uint64, branch byte) common.Hash {
	var b [9]byte
	binary.BigEndian.PutUint64(b[:8], number)
	b[8] = branch
	return crypto.Keccak256Hash(b[:])
}

func sampleTx(t *testing.T, key *ecdsa.PrivateKey, nonce uint64) types.Transaction {
	tx, err := types.SignTx(types.NewTransaction(nonce, common.Address{1}, uint256.NewInt(1), 21_000, uint256.NewInt(1), nil),
		*types.LatestSignerForChainID(uint256.NewInt(sampleChainID)), key)
	require.NoError(t, err)
	return tx
}
