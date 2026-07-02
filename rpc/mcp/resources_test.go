package mcp

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/jsonrpc"
)

type fakeEthAPI struct {
	jsonrpc.EthAPI
	blockNumber hexutil.Uint64
	syncing     any
	balance     *hexutil.Big
	nonce       hexutil.Uint64
	code        hexutil.Bytes
	blocks      map[rpc.BlockNumber]map[string]any
	tx          *ethapi.RPCTransaction
	receipt     map[string]any

	lastAddress     common.Address
	lastBlockNumber rpc.BlockNumber
	lastTxHash      common.Hash
}

func (f *fakeEthAPI) BlockNumber(ctx context.Context) (hexutil.Uint64, error) {
	return f.blockNumber, nil
}

func (f *fakeEthAPI) Syncing(ctx context.Context) (any, error) {
	return f.syncing, nil
}

func (f *fakeEthAPI) GetBalance(ctx context.Context, address common.Address, blockNrOrHash *rpc.BlockNumberOrHash) (*hexutil.Big, error) {
	f.lastAddress = address
	return f.balance, nil
}

func (f *fakeEthAPI) GetTransactionCount(ctx context.Context, address common.Address, blockNrOrHash *rpc.BlockNumberOrHash) (*hexutil.Uint64, error) {
	return &f.nonce, nil
}

func (f *fakeEthAPI) GetCode(ctx context.Context, address common.Address, blockNrOrHash *rpc.BlockNumberOrHash) (hexutil.Bytes, error) {
	return f.code, nil
}

func (f *fakeEthAPI) GetBlockByNumber(ctx context.Context, number rpc.BlockNumber, fullTx bool) (map[string]any, error) {
	f.lastBlockNumber = number
	return f.blocks[number], nil
}

func (f *fakeEthAPI) GetTransactionByHash(ctx context.Context, hash common.Hash) (*ethapi.RPCTransaction, error) {
	f.lastTxHash = hash
	return f.tx, nil
}

func (f *fakeEthAPI) GetTransactionReceipt(ctx context.Context, hash common.Hash) (map[string]any, error) {
	f.lastTxHash = hash
	return f.receipt, nil
}

type fakeErigonAPI struct {
	jsonrpc.ErigonAPI
}

func (f *fakeErigonAPI) NodeInfo(ctx context.Context) ([]p2p.NodeInfo, error) {
	return nil, nil
}

func readResourceRequest(uri string) mcp.ReadResourceRequest {
	var req mcp.ReadResourceRequest
	req.Params.URI = uri
	return req
}

func resourceJSON(t *testing.T, contents []mcp.ResourceContents) map[string]any {
	t.Helper()
	require.Len(t, contents, 1)
	text, ok := contents[0].(mcp.TextResourceContents)
	require.True(t, ok)
	var m map[string]any
	require.NoError(t, json.Unmarshal([]byte(text.Text), &m))
	return m
}

func TestExtractURIParam(t *testing.T) {
	tests := []struct {
		uri, prefix, suffix, want string
	}{
		{"erigon://address/0xabc/summary", "erigon://address/", "/summary", "0xabc"},
		{"erigon://block/latest/summary", "erigon://block/", "/summary", "latest"},
		{"erigon://node/info", "erigon://node/", "", "info"},
		{"erigon://address//summary", "erigon://address/", "/summary", ""},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, extractURIParam(tt.uri, tt.prefix, tt.suffix), "uri=%s", tt.uri)
	}
}

func TestResourceNetworkStatusReportsSyncProgress(t *testing.T) {
	progress := map[string]any{"currentBlock": "0x10", "highestBlock": "0x20"}
	e := &ErigonMCPServer{
		ethAPI:    &fakeEthAPI{syncing: progress},
		erigonAPI: &fakeErigonAPI{},
	}

	contents, err := e.handleResourceNetworkStatus(context.Background(), readResourceRequest("erigon://network/status"))
	require.NoError(t, err)

	status := resourceJSON(t, contents)
	require.Equal(t, progress, status["syncing"])
}

func TestResourceNetworkStatusWhenSynced(t *testing.T) {
	e := &ErigonMCPServer{
		ethAPI:    &fakeEthAPI{syncing: false},
		erigonAPI: &fakeErigonAPI{},
	}

	contents, err := e.handleResourceNetworkStatus(context.Background(), readResourceRequest("erigon://network/status"))
	require.NoError(t, err)

	status := resourceJSON(t, contents)
	require.Equal(t, false, status["syncing"])
}

func TestResourceAddressSummaryExtractsAddress(t *testing.T) {
	addr := "0x00000000000000000000000000000000deadbeef"
	fake := &fakeEthAPI{
		balance: (*hexutil.Big)(hexutil.MustDecodeBig("0x2a")),
		nonce:   7,
		code:    hexutil.Bytes{0x60, 0x00},
	}
	e := &ErigonMCPServer{ethAPI: fake}

	contents, err := e.handleResourceAddressSummary(context.Background(), readResourceRequest("erigon://address/"+addr+"/summary"))
	require.NoError(t, err)

	summary := resourceJSON(t, contents)
	require.Equal(t, addr, summary["address"])
	require.Equal(t, common.HexToAddress(addr), fake.lastAddress)
	require.Equal(t, true, summary["is_contract"])
}

func TestResourceAddressSummaryMissingAddress(t *testing.T) {
	e := &ErigonMCPServer{ethAPI: &fakeEthAPI{}}

	_, err := e.handleResourceAddressSummary(context.Background(), readResourceRequest("erigon://address//summary"))
	require.Error(t, err)
}

func TestResourceBlockSummaryExtractsNumber(t *testing.T) {
	fake := &fakeEthAPI{
		blocks: map[rpc.BlockNumber]map[string]any{5: {"number": "0x5"}},
	}
	e := &ErigonMCPServer{ethAPI: fake}

	contents, err := e.handleResourceBlockSummary(context.Background(), readResourceRequest("erigon://block/0x5/summary"))
	require.NoError(t, err)
	require.Equal(t, rpc.BlockNumber(5), fake.lastBlockNumber)

	block := resourceJSON(t, contents)
	require.Equal(t, "0x5", block["number"])
}

func TestResourceTransactionAnalysisDerivesStatus(t *testing.T) {
	txHash := common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")
	uri := "erigon://transaction/" + txHash.Hex() + "/analysis"

	tests := []struct {
		name    string
		receipt map[string]any
		want    string
	}{
		{"reverted", map[string]any{"status": hexutil.Uint64(0)}, "reverted"},
		{"success", map[string]any{"status": hexutil.Uint64(1)}, "success"},
		{"pre-byzantium", map[string]any{"root": hexutil.Bytes{0x01}}, "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &fakeEthAPI{receipt: tt.receipt}
			e := &ErigonMCPServer{ethAPI: fake}

			contents, err := e.handleResourceTransactionAnalysis(context.Background(), readResourceRequest(uri))
			require.NoError(t, err)
			require.Equal(t, txHash, fake.lastTxHash)

			analysis := resourceJSON(t, contents)
			require.Equal(t, tt.want, analysis["status"])
		})
	}
}
