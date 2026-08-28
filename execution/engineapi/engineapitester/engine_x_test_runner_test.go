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

package engineapitester

import (
	"context"
	"encoding/json"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/commitment/trie"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/requests"
)

func TestEngineXTestDefinitionV_20_02(t *testing.T) {
	input := []byte(`{
		"network":"Osaka",
		"preHash":"0x0123456789abcdef",
		"lastblockhash":"0x0000000000000000000000000000000000000000000000000000000000000011",
		"postStateDiff":{
			"0x0000000000000000000000000000000000000001":{
				"nonce":"0x02",
				"balance":"0x07",
				"code":"0x6000",
				"storage":{"0x01":"0x02"}
			},
			"0x0000000000000000000000000000000000000002":null
		},
		"engineNewPayloads":[]
	}`)
	var definition EngineXTestDefinition
	require.NoError(t, json.Unmarshal(input, &definition))
	require.Equal(t, PreAllocHash("0x0123456789abcdef"), definition.PreAllocHash)
	require.Equal(t, common.HexToHash("0x11"), *definition.LastBlockHash)
	require.Len(t, *definition.PostStateDiff, 2)
	account := (*definition.PostStateDiff)[common.HexToAddress("0x01")]
	require.NotNil(t, account)
	require.Equal(t, uint64(2), account.Nonce)
	require.Zero(t, account.Balance.Cmp(big.NewInt(7)))
	require.Equal(t, []byte{0x60, 0x00}, account.Code)
	require.Equal(t, common.HexToHash("0x02"), account.Storage[common.HexToHash("0x01")])
	require.Nil(t, (*definition.PostStateDiff)[common.HexToAddress("0x02")])
}

func TestEngineXStorageRoot(t *testing.T) {
	key := common.HexToHash("0x01")
	tests := []struct {
		name  string
		value common.Hash
		root  common.Hash
	}{
		{
			name:  "single byte RLP boundary",
			value: common.HexToHash("0x80"),
			root:  common.HexToHash("0xb831f0192d547189cd866964971e8e82b70b38570c785dc2c1f675f34a29f702"),
		},
		{
			name:  "multi byte value",
			value: common.HexToHash("0x0100"),
			root:  common.HexToHash("0x6a27488a57b630cf22a08ab0b0671e32ff037ec907c56896dc8282f6be5cf9ed"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.root, engineXStorageRoot(map[common.Hash]common.Hash{key: test.value}))
		})
	}
}

func TestVerifyEngineXResult(t *testing.T) {
	head := common.HexToHash("0x11")
	accountAddress := common.HexToAddress("0x01")
	deletedAddress := common.HexToAddress("0x02")
	code := []byte{0x60, 0x00}
	diff := EngineXPostStateDiff{
		accountAddress: {
			Balance: big.NewInt(7),
			Nonce:   2,
			Code:    code,
			Storage: map[common.Hash]common.Hash{},
		},
		deletedAddress: nil,
	}
	definition := EngineXTestDefinition{LastBlockHash: &head, PostStateDiff: &diff}
	newReader := func() *engineXResultReaderStub {
		return &engineXResultReaderStub{
			block: &requests.Block{BlockWithTxHashes: requests.BlockWithTxHashes{Hash: head}},
			proofs: map[common.Address]*accounts.AccProofResult{
				accountAddress: engineXAccountProof(7, 2, crypto.Keccak256Hash(code), trie.EmptyRoot),
				deletedAddress: engineXAccountProof(0, 0, common.Hash{}, common.Hash{}),
			},
		}
	}
	require.NoError(t, verifyEngineXResult(t.Context(), newReader(), definition))
	tests := []struct {
		name   string
		mutate func(*engineXResultReaderStub)
		want   string
	}{
		{
			name: "head",
			mutate: func(reader *engineXResultReaderStub) {
				reader.block.Hash = common.HexToHash("0x22")
			},
			want: "final head mismatch",
		},
		{
			name: "balance",
			mutate: func(reader *engineXResultReaderStub) {
				reader.proofs[accountAddress].Balance = engineXProofBalance(8)
			},
			want: "balance mismatch",
		},
		{
			name: "nonce",
			mutate: func(reader *engineXResultReaderStub) {
				reader.proofs[accountAddress].Nonce = 3
			},
			want: "nonce mismatch",
		},
		{
			name: "code hash",
			mutate: func(reader *engineXResultReaderStub) {
				reader.proofs[accountAddress].CodeHash = common.HexToHash("0x33")
			},
			want: "code hash mismatch",
		},
		{
			name: "storage root",
			mutate: func(reader *engineXResultReaderStub) {
				reader.proofs[accountAddress].StorageHash = common.HexToHash("0x44")
			},
			want: "storage root mismatch",
		},
		{
			name: "deleted account",
			mutate: func(reader *engineXResultReaderStub) {
				reader.proofs[deletedAddress].CodeHash = accounts.EmptyCodeHash.Value()
			},
			want: "expected account to be absent",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader := newReader()
			test.mutate(reader)
			err := verifyEngineXResult(t.Context(), reader, definition)
			require.ErrorContains(t, err, test.want)
		})
	}
}

type engineXResultReaderStub struct {
	block  *requests.Block
	proofs map[common.Address]*accounts.AccProofResult
}

func (r *engineXResultReaderStub) GetBlockByNumber(context.Context, rpc.BlockNumber, bool) (*requests.Block, error) {
	return r.block, nil
}

func (r *engineXResultReaderStub) GetProof(_ context.Context, address common.Address, _ []common.Hash, _ rpc.BlockReference) (*accounts.AccProofResult, error) {
	return r.proofs[address], nil
}

func engineXAccountProof(balance, nonce uint64, codeHash, storageHash common.Hash) *accounts.AccProofResult {
	return &accounts.AccProofResult{
		Balance:     engineXProofBalance(balance),
		Nonce:       hexutil.Uint64(nonce),
		CodeHash:    codeHash,
		StorageHash: storageHash,
	}
}

func engineXProofBalance(balance uint64) *hexutil.U256 {
	value := hexutil.U256(*uint256.NewInt(balance))
	return &value
}
