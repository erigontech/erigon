// Copyright 2024 The Erigon Authors
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

package rpctest

import (
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/stretchr/testify/require"
)

func MockRequestGenerator(reqId int) *RequestGenerator {
	r := &RequestGenerator{}
	r.reqID.Store(int64(reqId))
	return r
}

func TestRequestGenerator_blockNumber(t *testing.T) {
	testCases := []struct {
		reqId    int
		expected string
	}{
		{1, `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`},
		{2, `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":2}`},
		{3, `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":3}`},
	}

	reqGen := MockRequestGenerator(0)
	for _, testCase := range testCases {
		got := reqGen.blockNumber()
		require.Equal(t, testCase.expected, got)
	}
}

func TestRequestGenerator_getBlockByNumber(t *testing.T) {
	testCases := []struct {
		reqId    int
		blockNum uint64
		withTxs  bool
		expected string
	}{
		{
			1,
			12,
			false,
			`{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0xc",false],"id":1}`,
		},
		{
			2,
			12144356,
			false,
			`{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0xb94ee4",false],"id":2}`,
		},
		{
			3,
			0,
			true,
			`{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x0",true],"id":3}`,
		},
		{
			4,
			424713,
			true,
			`{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x67b09",true],"id":4}`,
		},
	}

	reqGen := MockRequestGenerator(0)
	for _, testCase := range testCases {
		got := reqGen.getBlockByNumber(testCase.blockNum, testCase.withTxs)
		require.Equal(t, testCase.expected, got)
	}
}

func TestRequestGenerator_storageRangeAt(t *testing.T) {
	testCases := []struct {
		reqId    int
		hash     common.Hash
		i        int
		to       common.Address
		nextKey  common.Hash
		expected string
	}{
		{
			1,
			common.HexToHash("0x1cd73c7adf5b31f3cf94c67b9e251e699559d91c27664463fb5978b97f8b2d1b"),
			1,
			common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7"),
			common.HexToHash("0x6f9e34c00812a80fa87df26208bbe69411e36d6a9f00b35444ef4181f6c483ca"),
			`{"jsonrpc":"2.0","method":"debug_storageRangeAt","params":["0x1cd73c7adf5b31f3cf94c67b9e251e699559d91c27664463fb5978b97f8b2d1b", 1,"0x71562b71999873db5b286df957af199ec94617f7","0x6f9e34c00812a80fa87df26208bbe69411e36d6a9f00b35444ef4181f6c483ca",1024],"id":1}`,
		},
		{
			2,
			common.HexToHash("0x6f9e34c00812a80fa87df26208bbe69411e36d6a9f00b35444ef4181f6c483ca"),
			2,
			common.HexToAddress("0x67b1d87101671b127f5f8714789c7192f7ad340e"),
			common.HexToHash("0x1cfe7ce95a1694d8969365cb472ce4a0d3eed812c540fd7708bbe6941e34c4de"),
			`{"jsonrpc":"2.0","method":"debug_storageRangeAt","params":["0x6f9e34c00812a80fa87df26208bbe69411e36d6a9f00b35444ef4181f6c483ca", 2,"0x67b1d87101671b127f5f8714789c7192f7ad340e","0x1cfe7ce95a1694d8969365cb472ce4a0d3eed812c540fd7708bbe6941e34c4de",1024],"id":2}`,
		},
		{
			3,
			common.HexToHash("0x1cfe7ce95a1694d8969365cb472ce4a0d3eed812c540fd7708bbe6941e34c4de"),
			3,
			common.HexToAddress("0x1b5fd2fed153fa7fac43300273c70c068bfa406a"),
			common.HexToHash("0x1cd73c7adf5b31f3cf94c67b9e251e699559d91c27664463fb5978b97f8b2d1b"),
			`{"jsonrpc":"2.0","method":"debug_storageRangeAt","params":["0x1cfe7ce95a1694d8969365cb472ce4a0d3eed812c540fd7708bbe6941e34c4de", 3,"0x1b5fd2fed153fa7fac43300273c70c068bfa406a","0x1cd73c7adf5b31f3cf94c67b9e251e699559d91c27664463fb5978b97f8b2d1b",1024],"id":3}`,
		},
	}

	reqGen := MockRequestGenerator(0)
	for _, testCase := range testCases {
		got := reqGen.storageRangeAt(testCase.hash, testCase.i, &testCase.to, testCase.nextKey)
		require.Equal(t, testCase.expected, got)
	}
}

func TestRequestGenerator_traceBlockByHash(t *testing.T) {
	testCases := []struct {
		reqId    int
		hash     string
		expected string
	}{
		{
			1,
			"0x1cd73c7adf5b31f3cf94c67b9e251e699559d91c27664463fb5978b97f8b2d1b",
			`{"jsonrpc":"2.0","method":"debug_traceBlockByHash","params":["0x1cd73c7adf5b31f3cf94c67b9e251e699559d91c27664463fb5978b97f8b2d1b"],"id":1}`,
		},
		{
			2,
			"0x6f9e34c00812a80fa87df26208bbe69411e36d6a9f00b35444ef4181f6c483ca",
			`{"jsonrpc":"2.0","method":"debug_traceBlockByHash","params":["0x6f9e34c00812a80fa87df26208bbe69411e36d6a9f00b35444ef4181f6c483ca"],"id":2}`,
		},
		{
			3,
			"0x1cfe7ce95a1694d8969365cb472ce4a0d3eed812c540fd7708bbe6941e34c4de",
			`{"jsonrpc":"2.0","method":"debug_traceBlockByHash","params":["0x1cfe7ce95a1694d8969365cb472ce4a0d3eed812c540fd7708bbe6941e34c4de"],"id":3}`,
		},
	}

	reqGen := MockRequestGenerator(0)
	for _, testCase := range testCases {
		got := reqGen.traceBlockByHash(testCase.hash)
		require.Equal(t, testCase.expected, got)
	}
}

func TestRequestGenerator_traceTransaction(t *testing.T) {
	testCases := []struct {
		reqId    int
		hash     string
		expected string
	}{
		{
			1,
			"0x1cd73c7adf5b31f3cf94c67b9e251e699559d91c27664463fb5978b97f8b2d1b",
			`{"jsonrpc":"2.0","method":"debug_traceTransaction","params":["0x1cd73c7adf5b31f3cf94c67b9e251e699559d91c27664463fb5978b97f8b2d1b"],"id":1}`,
		},
		{
			2,
			"0x6f9e34c00812a80fa87df26208bbe69411e36d6a9f00b35444ef4181f6c483ca",
			`{"jsonrpc":"2.0","method":"debug_traceTransaction","params":["0x6f9e34c00812a80fa87df26208bbe69411e36d6a9f00b35444ef4181f6c483ca"],"id":2}`,
		},
		{
			3,
			"0x1cfe7ce95a1694d8969365cb472ce4a0d3eed812c540fd7708bbe6941e34c4de",
			`{"jsonrpc":"2.0","method":"debug_traceTransaction","params":["0x1cfe7ce95a1694d8969365cb472ce4a0d3eed812c540fd7708bbe6941e34c4de"],"id":3}`,
		},
	}

	reqGen := MockRequestGenerator(0)
	for _, testCase := range testCases {
		got := reqGen.debugTraceTransaction(testCase.hash, "")
		require.Equal(t, testCase.expected, got)
	}
}

func TestRequestGenerator_getTransactionReceipt(t *testing.T) {
	testCases := []struct {
		reqId    int
		hash     string
		expected string
	}{
		{
			1,
			"0x1cd73c7adf5b31f3cf94c67b9e251e699559d91c27664463fb5978b97f8b2d1b",
			`{"jsonrpc":"2.0","method":"eth_getTransactionReceipt","params":["0x1cd73c7adf5b31f3cf94c67b9e251e699559d91c27664463fb5978b97f8b2d1b"],"id":1}`,
		},
		{
			2,
			"0x6f9e34c00812a80fa87df26208bbe69411e36d6a9f00b35444ef4181f6c483ca",
			`{"jsonrpc":"2.0","method":"eth_getTransactionReceipt","params":["0x6f9e34c00812a80fa87df26208bbe69411e36d6a9f00b35444ef4181f6c483ca"],"id":2}`,
		},
		{
			3,
			"0x1cfe7ce95a1694d8969365cb472ce4a0d3eed812c540fd7708bbe6941e34c4de",
			`{"jsonrpc":"2.0","method":"eth_getTransactionReceipt","params":["0x1cfe7ce95a1694d8969365cb472ce4a0d3eed812c540fd7708bbe6941e34c4de"],"id":3}`,
		},
	}

	reqGen := MockRequestGenerator(0)
	for _, testCase := range testCases {
		got := reqGen.getTransactionReceipt(testCase.hash)
		require.Equal(t, testCase.expected, got)
	}
}

func TestRequestGenerator_getBalance(t *testing.T) {
	testCases := []struct {
		reqId    int
		miner    common.Address
		blockNum uint64
		expected string
	}{
		{
			1,
			common.HexToAddress("0x67b1d87101671b127f5f8714789c7192f7ad340e"),
			4756372,
			`{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x67b1d87101671b127f5f8714789c7192f7ad340e", "0x489394"],"id":1}`,
		},
		{
			2,
			common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7"),
			0,
			`{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x71562b71999873db5b286df957af199ec94617f7", "0x0"],"id":2}`,
		},
		{
			3,
			common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7"),
			123456,
			`{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x71562b71999873db5b286df957af199ec94617f7", "0x1e240"],"id":3}`,
		},
	}

	reqGen := MockRequestGenerator(0)
	for _, testCase := range testCases {
		got := reqGen.getBalance(testCase.miner, testCase.blockNum)
		require.Equal(t, testCase.expected, got)
	}
}

func TestRequestGenerator_getModifiedAccountsByNumber(t *testing.T) {
	testCases := []struct {
		reqId        int
		prevBlockNum uint64
		blockNum     uint64
		expected     string
	}{
		{
			1,
			4756370,
			4756372,
			`{"jsonrpc":"2.0","method":"debug_getModifiedAccountsByNumber","params":[4756370, 4756372],"id":1}`,
		},
		{
			2,
			0,
			4,
			`{"jsonrpc":"2.0","method":"debug_getModifiedAccountsByNumber","params":[0, 4],"id":2}`,
		},
		{
			3,
			123452,
			123457,
			`{"jsonrpc":"2.0","method":"debug_getModifiedAccountsByNumber","params":[123452, 123457],"id":3}`,
		},
	}

	reqGen := MockRequestGenerator(0)
	for _, testCase := range testCases {
		got := reqGen.getModifiedAccountsByNumber(testCase.prevBlockNum, testCase.blockNum)
		require.Equal(t, testCase.expected, got)
	}
}

func TestRequestGenerator_getLogs(t *testing.T) {
	testCases := []struct {
		reqId        int
		prevBlockNum uint64
		blockNum     uint64
		account      common.Address
		expected     string
	}{
		{
			1,
			4756370,
			4756372,
			common.HexToAddress("0x67b1d87101671b127f5f8714789c7192f7ad340e"),
			`{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock": "0x489392", "toBlock": "0x489394", "address": "0x67b1d87101671b127f5f8714789c7192f7ad340e"}],"id":1}`,
		},
		{
			2,
			0,
			4,
			common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7"),
			`{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock": "0x0", "toBlock": "0x4", "address": "0x71562b71999873db5b286df957af199ec94617f7"}],"id":2}`,
		},
		{
			3,
			123452,
			123457,
			common.HexToAddress("0x1b5fd2fed153fa7fac43300273c70c068bfa406a"),
			`{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock": "0x1e23c", "toBlock": "0x1e241", "address": "0x1b5fd2fed153fa7fac43300273c70c068bfa406a"}],"id":3}`,
		},
	}

	reqGen := MockRequestGenerator(0)
	for _, testCase := range testCases {
		got := reqGen.getLogs(testCase.prevBlockNum, testCase.blockNum, testCase.account)
		require.Equal(t, testCase.expected, got)
	}
}

func TestRequestGenerator_getLogs1(t *testing.T) {
	testCases := []struct {
		reqId        int
		prevBlockNum uint64
		blockNum     uint64
		account      common.Address
		topic        common.Hash
		expected     string
	}{
		{
			1,
			4756370,
			4756372,
			common.HexToAddress("0x67b1d87101671b127f5f8714789c7192f7ad340e"),
			common.HexToHash("0x6f9e34c00812a80fa87df26208bbe69411e36d6a9f00b35444ef4181f6c483ca"),
			`{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock": "0x489392", "toBlock": "0x489394", "address": "0x67b1d87101671b127f5f8714789c7192f7ad340e", "topics": ["0x6f9e34c00812a80fa87df26208bbe69411e36d6a9f00b35444ef4181f6c483ca"]}],"id":1}`,
		},
		{
			2,
			0,
			4,
			common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7"),
			common.HexToHash("0x1cfe7ce95a1694d8969365cb472ce4a0d3eed812c540fd7708bbe6941e34c4de"),
			`{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock": "0x0", "toBlock": "0x4", "address": "0x71562b71999873db5b286df957af199ec94617f7", "topics": ["0x1cfe7ce95a1694d8969365cb472ce4a0d3eed812c540fd7708bbe6941e34c4de"]}],"id":2}`,
		},
		{
			3,
			123452,
			123457,
			common.HexToAddress("0x1b5fd2fed153fa7fac43300273c70c068bfa406a"),
			common.HexToHash("0x1cd73c7adf5b31f3cf94c67b9e251e699559d91c27664463fb5978b97f8b2d1b"),
			`{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock": "0x1e23c", "toBlock": "0x1e241", "address": "0x1b5fd2fed153fa7fac43300273c70c068bfa406a", "topics": ["0x1cd73c7adf5b31f3cf94c67b9e251e699559d91c27664463fb5978b97f8b2d1b"]}],"id":3}`,
		},
	}

	reqGen := MockRequestGenerator(0)
	for _, testCase := range testCases {
		got := reqGen.getLogs1(testCase.prevBlockNum, testCase.blockNum, testCase.account, testCase.topic)
		require.Equal(t, testCase.expected, got)
	}
}

func TestRequestGenerator_getLogs2(t *testing.T) {
	testCases := []struct {
		reqId        int
		prevBlockNum uint64
		blockNum     uint64
		account      common.Address
		topic1       common.Hash
		topic2       common.Hash
		expected     string
	}{
		{
			1,
			4756370,
			4756372,
			common.HexToAddress("0x67b1d87101671b127f5f8714789c7192f7ad340e"),
			common.HexToHash("0x6f9e34c00812a80fa87df26208bbe69411e36d6a9f00b35444ef4181f6c483ca"),
			common.HexToHash("0x1cfe7ce95a1694d8969365cb472ce4a0d3eed812c540fd7708bbe6941e34c4de"),
			`{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock": "0x489392", "toBlock": "0x489394", "address": "0x67b1d87101671b127f5f8714789c7192f7ad340e", "topics": ["0x6f9e34c00812a80fa87df26208bbe69411e36d6a9f00b35444ef4181f6c483ca", "0x1cfe7ce95a1694d8969365cb472ce4a0d3eed812c540fd7708bbe6941e34c4de"]}],"id":1}`,
		},
		{
			2,
			0,
			4,
			common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7"),
			common.HexToHash("0x1cfe7ce95a1694d8969365cb472ce4a0d3eed812c540fd7708bbe6941e34c4de"),
			common.HexToHash("0x1cd73c7adf5b31f3cf94c67b9e251e699559d91c27664463fb5978b97f8b2d1b"),
			`{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock": "0x0", "toBlock": "0x4", "address": "0x71562b71999873db5b286df957af199ec94617f7", "topics": ["0x1cfe7ce95a1694d8969365cb472ce4a0d3eed812c540fd7708bbe6941e34c4de", "0x1cd73c7adf5b31f3cf94c67b9e251e699559d91c27664463fb5978b97f8b2d1b"]}],"id":2}`,
		},
		{
			3,
			123452,
			123457,
			common.HexToAddress("0x1b5fd2fed153fa7fac43300273c70c068bfa406a"),
			common.HexToHash("0x1cd73c7adf5b31f3cf94c67b9e251e699559d91c27664463fb5978b97f8b2d1b"),
			common.HexToHash("0x6f9e34c00812a80fa87df26208bbe69411e36d6a9f00b35444ef4181f6c483ca"),
			`{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock": "0x1e23c", "toBlock": "0x1e241", "address": "0x1b5fd2fed153fa7fac43300273c70c068bfa406a", "topics": ["0x1cd73c7adf5b31f3cf94c67b9e251e699559d91c27664463fb5978b97f8b2d1b", "0x6f9e34c00812a80fa87df26208bbe69411e36d6a9f00b35444ef4181f6c483ca"]}],"id":3}`,
		},
	}

	reqGen := MockRequestGenerator(0)
	for _, testCase := range testCases {
		got := reqGen.getLogs2(testCase.prevBlockNum, testCase.blockNum, testCase.account, testCase.topic1, testCase.topic2)
		require.Equal(t, testCase.expected, got)
	}
}

func TestRequestGenerator_accountRange(t *testing.T) {
	testCases := []struct {
		reqId    int
		blockNum uint64
		page     []byte
		num      int
		expected string
	}{
		{
			1,
			4756370,
			common.HexToHash("0x6f9e34c00812a80fa87df26208bbe69411e36d6a9f00b35444ef4181f6c483ca").Bytes(),
			1,
			`{ "jsonrpc": "2.0", "method": "debug_accountRange", "params": ["0x489392", "b540wAgSqA+offJiCLvmlBHjbWqfALNURO9BgfbEg8o=", 1, false, false], "id":1}`,
		},
		{
			2,
			0,
			common.HexToHash("0x1cfe7ce95a1694d8969365cb472ce4a0d3eed812c540fd7708bbe6941e34c4de").Bytes(),
			2,
			`{ "jsonrpc": "2.0", "method": "debug_accountRange", "params": ["0x0", "HP586VoWlNiWk2XLRyzkoNPu2BLFQP13CLvmlB40xN4=", 2, false, false], "id":2}`,
		},
		{
			3,
			1234567,
			common.HexToHash("0x1cd73c7adf5b31f3cf94c67b9e251e699559d91c27664463fb5978b97f8b2d1b").Bytes(),
			3,
			`{ "jsonrpc": "2.0", "method": "debug_accountRange", "params": ["0x12d687", "HNc8et9bMfPPlMZ7niUeaZVZ2RwnZkRj+1l4uX+LLRs=", 3, false, false], "id":3}`,
		},
	}

	reqGen := MockRequestGenerator(0)
	for _, testCase := range testCases {
		got := reqGen.accountRange(testCase.blockNum, testCase.page, testCase.num)
		require.Equal(t, testCase.expected, got)
	}
}

func TestRequestGenerator_getProof(t *testing.T) {
	testCases := []struct {
		reqId       int
		blockNum    uint64
		account     common.Address
		storageList []common.Hash
		expected    string
	}{
		{
			1,
			3425865,
			common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7"),
			[]common.Hash{
				common.HexToHash("0x6f9e34c00812a80fa87df26208bbe69411e36d6a9f00b35444ef4181f6c483ca"),
				common.HexToHash("0x1cfe7ce95a1694d8969365cb472ce4a0d3eed812c540fd7708bbe6941e34c4de"),
			},
			`{ "jsonrpc": "2.0", "method": "eth_getProof", "params": ["0x71562b71999873db5b286df957af199ec94617f7", ["0x6f9e34c00812a80fa87df26208bbe69411e36d6a9f00b35444ef4181f6c483ca","0x1cfe7ce95a1694d8969365cb472ce4a0d3eed812c540fd7708bbe6941e34c4de"], "0x344649"], "id":1}`,
		},
		{
			2,
			103,
			common.HexToAddress("0x67b1d87101671b127f5f8714789c7192f7ad340e"),
			[]common.Hash{
				common.HexToHash("0x1cfe7ce95a1694d8969365cb472ce4a0d3eed812c540fd7708bbe6941e34c4de"),
				common.HexToHash("0x2599b236b455dd0081516c7f2f82dab3af89a68d5ea5e7601181cbd2a7fdf13c"),
			},
			`{ "jsonrpc": "2.0", "method": "eth_getProof", "params": ["0x67b1d87101671b127f5f8714789c7192f7ad340e", ["0x1cfe7ce95a1694d8969365cb472ce4a0d3eed812c540fd7708bbe6941e34c4de","0x2599b236b455dd0081516c7f2f82dab3af89a68d5ea5e7601181cbd2a7fdf13c"], "0x67"], "id":2}`,
		},
	}

	reqGen := MockRequestGenerator(0)
	for _, testCase := range testCases {
		got := reqGen.getProof(testCase.blockNum, testCase.account, testCase.storageList)
		require.Equal(t, testCase.expected, got)
	}
}
