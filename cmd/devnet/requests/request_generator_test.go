package requests

import (
	"testing"

	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/common"
	"github.com/stretchr/testify/require"
)

func MockRequestGenerator(reqId int) *RequestGenerator {
	return &RequestGenerator{
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
		got := reqGen.GetAdminNodeInfo()
		require.EqualValues(t, testCase.expected, got)
	}
}

func TestRequestGenerator_GetBalance(t *testing.T) {
	testCases := []struct {
		reqId    int
		address  common.Address
		blockNum models.BlockNumber
		expected string
	}{
		{
			1,
			common.HexToAddress("0x67b1d87101671b127f5f8714789c7192f7ad340e"),
			models.Latest,
			`{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x67b1d87101671b127f5f8714789c7192f7ad340e","latest"],"id":1}`,
		},
		{
			2,
			common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7"),
			models.Earliest,
			`{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x71562b71999873db5b286df957af199ec94617f7","earliest"],"id":2}`,
		},
		{
			3,
			common.HexToAddress("0x1b5fd2fed153fa7fac43300273c70c068bfa406a"),
			models.Pending,
			`{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x1b5fd2fed153fa7fac43300273c70c068bfa406a","pending"],"id":3}`,
		},
	}

	for _, testCase := range testCases {
		reqGen := MockRequestGenerator(testCase.reqId)
		got := reqGen.GetBalance(testCase.address, testCase.blockNum)
		require.EqualValues(t, testCase.expected, got)
	}
}

func TestRequestGenerator_GetTransactionCount(t *testing.T) {
	testCases := []struct {
		reqId    int
		address  common.Address
		blockNum models.BlockNumber
		expected string
	}{
		{
			1,
			common.HexToAddress("0x67b1d87101671b127f5f8714789c7192f7ad340e"),
			models.Latest,
			`{"jsonrpc":"2.0","method":"eth_getTransactionCount","params":["0x67b1d87101671b127f5f8714789c7192f7ad340e","latest"],"id":1}`,
		},
		{
			2,
			common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7"),
			models.Earliest,
			`{"jsonrpc":"2.0","method":"eth_getTransactionCount","params":["0x71562b71999873db5b286df957af199ec94617f7","earliest"],"id":2}`,
		},
		{
			3,
			common.HexToAddress("0x1b5fd2fed153fa7fac43300273c70c068bfa406a"),
			models.Pending,
			`{"jsonrpc":"2.0","method":"eth_getTransactionCount","params":["0x1b5fd2fed153fa7fac43300273c70c068bfa406a","pending"],"id":3}`,
		},
	}

	for _, testCase := range testCases {
		reqGen := MockRequestGenerator(testCase.reqId)
		got := reqGen.GetTransactionCount(testCase.address, testCase.blockNum)
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
			common.HexToHash("0x1cd73c7adf5b31f3cf94c67b9e251e699559d91c27664463fb5978b97f8b2d1b").Bytes(),
			`{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0x1cd73c7adf5b31f3cf94c67b9e251e699559d91c27664463fb5978b97f8b2d1b"],"id":1}`,
		},
		{
			2,
			common.HexToHash("0x1cfe7ce95a1694d8969365cb472ce4a0d3eed812c540fd7708bbe6941e34c4de").Bytes(),
			`{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0x1cfe7ce95a1694d8969365cb472ce4a0d3eed812c540fd7708bbe6941e34c4de"],"id":2}`,
		},
		{
			3,
			common.HexToHash("0x6f9e34c00812a80fa87df26208bbe69411e36d6a9f00b35444ef4181f6c483ca").Bytes(),
			`{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0x6f9e34c00812a80fa87df26208bbe69411e36d6a9f00b35444ef4181f6c483ca"],"id":3}`,
		},
	}

	for _, testCase := range testCases {
		reqGen := MockRequestGenerator(testCase.reqId)
		got := reqGen.SendRawTransaction(testCase.signedTx)
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
		got := reqGen.TxpoolContent()
		require.EqualValues(t, testCase.expected, got)
	}
}
