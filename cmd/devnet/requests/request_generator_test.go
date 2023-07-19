package requests

import (
	"math/big"
	"testing"

	ethereum "github.com/ledgerwatch/erigon"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/require"
)

func MockRequestGenerator(reqId int) *requestGenerator {
	return &requestGenerator{
		reqID:  reqId,
		client: nil,
	}
}

func TestRequestGenerator_GetAdminNodeInfo(t *testing.T) {
	testCases := []struct {
		reqId    int
		expected string
	}{
		{1, `{"jsonrpc":"2.0","method":"admin_nodeInfo","id":1}`},
		{2, `{"jsonrpc":"2.0","method":"admin_nodeInfo","id":2}`},
		{3, `{"jsonrpc":"2.0","method":"admin_nodeInfo","id":3}`},
	}

	for _, testCase := range testCases {
		reqGen := MockRequestGenerator(testCase.reqId)
		_, got := reqGen.adminNodeInfo()
		require.EqualValues(t, testCase.expected, got)
	}
}

func TestRequestGenerator_GetBalance(t *testing.T) {
	testCases := []struct {
		reqId    int
		address  libcommon.Address
		blockNum BlockNumber
		expected string
	}{
		{
			1,
			libcommon.HexToAddress("0x67b1d87101671b127f5f8714789c7192f7ad340e"),
			BlockNumbers.Latest,
			`{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x67b1d87101671b127f5f8714789c7192f7ad340e","latest"],"id":1}`,
		},
		{
			2,
			libcommon.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7"),
			BlockNumbers.Earliest,
			`{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x71562b71999873db5b286df957af199ec94617f7","earliest"],"id":2}`,
		},
		{
			3,
			libcommon.HexToAddress("0x1b5fd2fed153fa7fac43300273c70c068bfa406a"),
			BlockNumbers.Pending,
			`{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x1b5fd2fed153fa7fac43300273c70c068bfa406a","pending"],"id":3}`,
		},
	}

	for _, testCase := range testCases {
		reqGen := MockRequestGenerator(testCase.reqId)
		_, got := reqGen.getBalance(testCase.address, testCase.blockNum)
		require.EqualValues(t, testCase.expected, got)
	}
}

func TestRequestGenerator_GetBlockByNumber(t *testing.T) {
	testCases := []struct {
		reqId    int
		blockNum uint64
		withTxs  bool
		expected string
	}{
		{
			1,
			2,
			false,
			`{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x2",false],"id":1}`,
		},
		{
			2,
			16,
			false,
			`{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x10",false],"id":2}`,
		},
		{
			3,
			100,
			true,
			`{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x64",true],"id":3}`,
		},
	}

	for _, testCase := range testCases {
		reqGen := MockRequestGenerator(testCase.reqId)
		_, got := reqGen.getBlockByNumber(testCase.blockNum, testCase.withTxs)
		require.EqualValues(t, testCase.expected, got)
	}
}

func TestRequestGenerator_GetLogs(t *testing.T) {
	testCases := []struct {
		reqId     int
		fromBlock uint64
		toBlock   uint64
		address   libcommon.Address
		expected  string
	}{
		{
			1,
			1843,
			1848,
			libcommon.HexToAddress("0x67b1d87101671b127f5f8714789c7192f7ad340e"),
			`{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock":"0x733","toBlock":"0x738","address":"0x67b1d87101671b127f5f8714789c7192f7ad340e"}],"id":1}`,
		},
		{
			2,
			12,
			12,
			libcommon.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7"),
			`{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock":"0xc","toBlock":"0xc","address":"0x71562b71999873db5b286df957af199ec94617f7"}],"id":2}`,
		},
		{
			3,
			0,
			123456789,
			libcommon.HexToAddress("0x1b5fd2fed153fa7fac43300273c70c068bfa406a"),
			`{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock":"0x0","toBlock":"0x75bcd15","address":"0x1b5fd2fed153fa7fac43300273c70c068bfa406a"}],"id":3}`,
		},
	}

	for _, testCase := range testCases {
		reqGen := MockRequestGenerator(testCase.reqId)
		_, got := reqGen.getLogs(ethereum.FilterQuery{
			FromBlock: big.NewInt(int64(testCase.fromBlock)),
			ToBlock:   big.NewInt(int64(testCase.toBlock)),
			Addresses: []libcommon.Address{testCase.address},
		})
		require.EqualValues(t, testCase.expected, got)
	}
}

func TestRequestGenerator_GetTransactionCount(t *testing.T) {
	testCases := []struct {
		reqId    int
		address  libcommon.Address
		blockNum BlockNumber
		expected string
	}{
		{
			1,
			libcommon.HexToAddress("0x67b1d87101671b127f5f8714789c7192f7ad340e"),
			BlockNumbers.Latest,
			`{"jsonrpc":"2.0","method":"eth_getTransactionCount","params":["0x67b1d87101671b127f5f8714789c7192f7ad340e","latest"],"id":1}`,
		},
		{
			2,
			libcommon.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7"),
			BlockNumbers.Earliest,
			`{"jsonrpc":"2.0","method":"eth_getTransactionCount","params":["0x71562b71999873db5b286df957af199ec94617f7","earliest"],"id":2}`,
		},
		{
			3,
			libcommon.HexToAddress("0x1b5fd2fed153fa7fac43300273c70c068bfa406a"),
			BlockNumbers.Pending,
			`{"jsonrpc":"2.0","method":"eth_getTransactionCount","params":["0x1b5fd2fed153fa7fac43300273c70c068bfa406a","pending"],"id":3}`,
		},
	}

	for _, testCase := range testCases {
		reqGen := MockRequestGenerator(testCase.reqId)
		_, got := reqGen.getTransactionCount(testCase.address, testCase.blockNum)
		require.EqualValues(t, testCase.expected, got)
	}
}

func TestRequestGenerator_SendRawTransaction(t *testing.T) {
	testCases := []struct {
		reqId    int
		signedTx []byte
		expected string
	}{
		{
			1,
			libcommon.HexToHash("0x1cd73c7adf5b31f3cf94c67b9e251e699559d91c27664463fb5978b97f8b2d1b").Bytes(),
			`{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0x1cd73c7adf5b31f3cf94c67b9e251e699559d91c27664463fb5978b97f8b2d1b"],"id":1}`,
		},
		{
			2,
			libcommon.HexToHash("0x1cfe7ce95a1694d8969365cb472ce4a0d3eed812c540fd7708bbe6941e34c4de").Bytes(),
			`{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0x1cfe7ce95a1694d8969365cb472ce4a0d3eed812c540fd7708bbe6941e34c4de"],"id":2}`,
		},
		{
			3,
			libcommon.HexToHash("0x6f9e34c00812a80fa87df26208bbe69411e36d6a9f00b35444ef4181f6c483ca").Bytes(),
			`{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0x6f9e34c00812a80fa87df26208bbe69411e36d6a9f00b35444ef4181f6c483ca"],"id":3}`,
		},
	}

	for _, testCase := range testCases {
		reqGen := MockRequestGenerator(testCase.reqId)
		_, got := reqGen.sendRawTransaction(testCase.signedTx)
		require.EqualValues(t, testCase.expected, got)
	}
}

func TestRequestGenerator_TxpoolContent(t *testing.T) {
	testCases := []struct {
		reqId    int
		expected string
	}{
		{1, `{"jsonrpc":"2.0","method":"txpool_content","params":[],"id":1}`},
		{2, `{"jsonrpc":"2.0","method":"txpool_content","params":[],"id":2}`},
		{3, `{"jsonrpc":"2.0","method":"txpool_content","params":[],"id":3}`},
	}

	for _, testCase := range testCases {
		reqGen := MockRequestGenerator(testCase.reqId)
		_, got := reqGen.txpoolContent()
		require.EqualValues(t, testCase.expected, got)
	}
}

func TestParseResponse(t *testing.T) {
	type Person struct {
		Name string
		Age  int
	}

	testCases := []struct {
		input    interface{}
		expected string
	}{
		{
			Person{
				Name: "Leonard",
				Age:  10,
			},
			`{"Name":"Leonard","Age":10}`,
		},
		{
			struct {
				Person struct {
					Name string
					Age  int
				}
				WorkID string
			}{
				Person: Person{
					Name: "Uzi",
					Age:  23,
				},
				WorkID: "123456",
			},
			`{"Person":{"Name":"Uzi","Age":23},"WorkID":"123456"}`,
		},
	}

	for _, testCase := range testCases {
		got, _ := parseResponse(testCase.input)
		require.EqualValues(t, testCase.expected, got)
	}
}
