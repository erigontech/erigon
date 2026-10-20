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

const sampleChainID = 7331

// SampleChain returns a chain of 11 blocks whose last three replaced an
// earlier branch, with block 4 finalized. Every block carries transactions
// and logs of both emitters.
func SampleChain(t *testing.T) *recording.MemoryChain {
	t.Helper()
	key, err := crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
	require.NoError(t, err)
	genesis := &recording.Block{Header: l1.Header{Hash: sampleHash(0, 0)}}
	c, err := recording.NewMemoryChain(sampleChainID, genesis)
	require.NoError(t, err)

	nonce := uint64(0)
	block := func(parent l1.Header, branch byte) *recording.Block {
		n := parent.Number + 1
		b := &recording.Block{Header: l1.Header{Number: n, Hash: sampleHash(n, branch), ParentHash: parent.Hash, Time: 12 * n}}
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
