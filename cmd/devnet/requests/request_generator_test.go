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
		blockNum string
		expected string
	}{
		{
			1,
			common.HexToAddress("0x67b1d87101671b127f5f8714789c7192f7ad340e"),
			models.BlockNumLatest, // TODO: change to models.Latest once former PR merge happens
			`{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x67b1d87101671b127f5f8714789c7192f7ad340e","latest"],"id":1}`,
		},
		{
			2,
			common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7"),
			"earliest", // TODO: change to models.Earliest once former PR merge happens
			`{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x71562b71999873db5b286df957af199ec94617f7","earliest"],"id":2}`,
		},
		{
			3,
			common.HexToAddress("0x1b5fd2fed153fa7fac43300273c70c068bfa406a"),
			"pending", // TODO: change to models.Pending once former PR merge happens
			`{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x1b5fd2fed153fa7fac43300273c70c068bfa406a","pending"],"id":3}`,
		},
	}

	for _, testCase := range testCases {
		reqGen := MockRequestGenerator(testCase.reqId)
		got := reqGen.GetBalance(testCase.address, testCase.blockNum)
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

// TODO: Add tests for GetTransactionCount
