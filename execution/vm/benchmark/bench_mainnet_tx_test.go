package benchmark

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/chain"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// txFixturesDir holds what `evm fetchtx` writes; it is gitignored.
const txFixturesDir = "testdata/txs"

// BenchmarkMainnetTx replays each mainnet tx fetched into testdata/txs, as
// eth_call runs it: a fresh state over the tx's prestate per iteration, on the
// materializing and the noMaterialize path.
func BenchmarkMainnetTx(b *testing.B) {
	paths, err := filepath.Glob(filepath.Join(txFixturesDir, "*.json"))
	require.NoError(b, err)
	for _, path := range paths {
		name := strings.TrimSuffix(filepath.Base(path), ".json")
		for _, noMaterialize := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/nm=%v", name, noMaterialize), func(b *testing.B) {
				r := newTxReplay(b, path)
				res := r.run(b, noMaterialize)
				// A replay that drifts from mainnet (a BLOCKHASH it cannot see, an
				// unsupported tx type) would bench another program.
				require.Equal(b, r.gasUsed, res.ReceiptGasUsed, "gas used vs the receipt")
				require.Equal(b, r.failed, res.Failed(), "outcome vs the receipt")
				b.ReportAllocs()
				for b.Loop() {
					r.run(b, noMaterialize)
				}
			})
		}
	}
}

type txFixture struct {
	Tx struct {
		From                 common.Address            `json:"from"`
		To                   *common.Address           `json:"to"`
		Input                hexutil.Bytes             `json:"input"`
		Value                hexutil.U256              `json:"value"`
		Gas                  hexutil.Uint64            `json:"gas"`
		GasPrice             *hexutil.U256             `json:"gasPrice"`
		MaxFeePerGas         *hexutil.U256             `json:"maxFeePerGas"`
		MaxPriorityFeePerGas *hexutil.U256             `json:"maxPriorityFeePerGas"`
		Nonce                hexutil.Uint64            `json:"nonce"`
		Type                 hexutil.Uint64            `json:"type"`
		AccessList           types.AccessList          `json:"accessList"`
		AuthorizationList    []types.JsonAuthorization `json:"authorizationList"`
	} `json:"tx"`
	Receipt struct {
		GasUsed hexutil.Uint64 `json:"gasUsed"`
		Status  hexutil.Uint64 `json:"status"`
	} `json:"receipt"`
	Block    types.Header `json:"block"`
	Prestate map[common.Address]struct {
		Balance *hexutil.U256               `json:"balance"`
		Nonce   uint64                      `json:"nonce"`
		Code    hexutil.Bytes               `json:"code"`
		Storage map[common.Hash]common.Hash `json:"storage"`
	} `json:"prestate"`
}

type txReplay struct {
	cfg      *chain.Config
	blockCtx evmtypes.BlockContext
	reader   state.StateReader
	fixture  *txFixture
	gasUsed  uint64
	failed   bool
}

// newTxReplay commits the fixture's prestate to a fresh DB that every run reads.
func newTxReplay(tb testing.TB, path string) *txReplay {
	tb.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(tb, err)
	f := &txFixture{}
	require.NoError(tb, json.Unmarshal(raw, f))
	if f.Tx.Type == types.BlobTxType || f.Tx.Type > types.SetCodeTxType {
		tb.Fatalf("%s: tx type %d is not replayed", path, f.Tx.Type)
	}

	cfg := chainspec.Mainnet.Config
	noHashes := func(uint64) (common.Hash, error) { return common.Hash{}, nil }
	blockCtx := protocol.NewEVMBlockContext(&f.Block, noHashes, nil, accounts.InternAddress(f.Block.Coinbase), cfg)

	db := temporaltest.NewTestDB(tb, datadir.New(tb.TempDir()))
	tx, domains := temporaltest.NewTestTxSD(tb, db)
	require.NoError(tb, rawdbv3.TxNums.Append(tx, 1, 1))
	reader := state.NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{}))
	seeded := state.New(reader)
	for a, acc := range f.Prestate {
		addr := accounts.InternAddress(a)
		require.NoError(tb, seeded.CreateAccount(addr, len(acc.Code) > 0))
		if acc.Balance != nil {
			require.NoError(tb, seeded.SetBalance(addr, uint256.Int(*acc.Balance), tracing.BalanceChangeUnspecified))
		}
		require.NoError(tb, seeded.SetNonce(addr, acc.Nonce, tracing.NonceChangeUnspecified))
		if len(acc.Code) > 0 {
			require.NoError(tb, seeded.SetCode(addr, acc.Code, tracing.CodeChangeUnspecified))
		}
		for k, v := range acc.Storage {
			var val uint256.Int
			val.SetBytes32(v[:])
			require.NoError(tb, seeded.SetState(addr, accounts.InternKey(k), val))
		}
	}
	require.NoError(tb, seeded.CommitBlock(blockCtx.Rules(cfg), state.NewWriter(domains.AsPutDel(tx), nil, 1)))

	return &txReplay{
		cfg:      cfg,
		blockCtx: blockCtx,
		reader:   reader,
		fixture:  f,
		gasUsed:  uint64(f.Receipt.GasUsed),
		failed:   f.Receipt.Status == 0,
	}
}

// run executes the tx once on a fresh state, as DoCall sets one up.
func (r *txReplay) run(tb testing.TB, noMaterialize bool) *evmtypes.ExecutionResult {
	ibs := state.New(r.reader)
	if noMaterialize {
		ibs.SetVersionMap(state.NewVersionMap(nil))
		ibs.SetNoMaterialize(true)
		ibs.SetTxContext(0, 0)
		ibs.SetNoConflictDetection()
	}
	msg := r.message()
	evm := vm.NewEVM(r.blockCtx, protocol.NewEVMTxContext(msg), ibs, r.cfg, vm.Config{})
	res, err := protocol.ApplyMessage(evm, msg, protocol.NewGasPool(r.fixture.Block.GasLimit, 0), true, false, nil)
	if err != nil {
		tb.Fatal(err)
	}
	return res
}

func (r *txReplay) message() *types.Message {
	t := &r.fixture.Tx
	to := accounts.NilAddress
	if t.To != nil {
		to = accounts.InternAddress(*t.To)
	}
	price := (*uint256.Int)(t.GasPrice)
	feeCap, tipCap := price, price
	if t.MaxFeePerGas != nil {
		feeCap, tipCap = (*uint256.Int)(t.MaxFeePerGas), (*uint256.Int)(t.MaxPriorityFeePerGas)
	}
	value := uint256.Int(t.Value)
	msg := types.NewMessage(accounts.InternAddress(t.From), to, uint64(t.Nonce), &value, uint64(t.Gas),
		price, feeCap, tipCap, t.Input, t.AccessList, true, true, true, false, nil)
	if len(t.AuthorizationList) > 0 {
		auths := make([]types.Authorization, len(t.AuthorizationList))
		for i := range t.AuthorizationList {
			var err error
			if auths[i], err = t.AuthorizationList[i].ToAuthorization(); err != nil {
				panic(err)
			}
		}
		msg.SetAuthorizations(auths)
	}
	return msg
}

// The replay runs the prestate's code over its storage on both paths: a call
// returning a seeded slot proves both reach the state the fixture describes.
func TestMainnetTxReplayUsesThePrestate(t *testing.T) {
	const legacy = `"gasPrice": "0x1", "type": "0x0"`
	// The authorization's signature does not recover, so EIP-7702 skips it and
	// the call still runs.
	const setCode = `"maxFeePerGas": "0x1", "maxPriorityFeePerGas": "0x0", "type": "0x4",
		"authorizationList": [{"chainId": "0x1", "address": "0x00000000000000000000000000000000000000d1",
			"nonce": "0x0", "yParity": "0x0", "r": "0x1", "s": "0x1"}]`
	for name, txFields := range map[string]string{"legacy": legacy, "setCode": setCode} {
		t.Run(name, func(t *testing.T) { testReplayUsesThePrestate(t, txFields) })
	}
}

func testReplayUsesThePrestate(t *testing.T, txFields string) {
	// PUSH1 0 SLOAD PUSH1 0 MSTORE PUSH1 32 PUSH1 0 RETURN
	const returnSlot0 = "0x60005460005260206000f3"
	const slot0 = "0x00000000000000000000000000000000000000000000000000000000000000aa"
	path := filepath.Join(t.TempDir(), "tx.json")
	require.NoError(t, os.WriteFile(path, []byte(`{
		"tx": {"from": "0x00000000000000000000000000000000000000f1", "to": "0x00000000000000000000000000000000000000c1",
			"input": "0x", "value": "0x0", "gas": "0x10000", "nonce": "0x0", `+txFields+`},
		"receipt": {"gasUsed": "0x0", "status": "0x1"},
		"block": {"parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
			"sha3Uncles": "0x0000000000000000000000000000000000000000000000000000000000000000",
			"miner": "0x00000000000000000000000000000000000000cb",
			"stateRoot": "0x0000000000000000000000000000000000000000000000000000000000000000",
			"transactionsRoot": "0x0000000000000000000000000000000000000000000000000000000000000000",
			"receiptsRoot": "0x0000000000000000000000000000000000000000000000000000000000000000",
			"logsBloom": "0x`+strings.Repeat("00", 256)+`",
			"difficulty": "0x0", "number": "0x1000000", "gasLimit": "0x2000000", "gasUsed": "0x0",
			"timestamp": "0x6800000000", "extraData": "0x", "baseFeePerGas": "0x1",
			"mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
			"nonce": "0x0000000000000000"},
		"prestate": {
			"0x00000000000000000000000000000000000000f1": {"balance": "0x10000000000"},
			"0x00000000000000000000000000000000000000c1": {"code": "`+returnSlot0+`", "nonce": 1,
				"storage": {"0x0000000000000000000000000000000000000000000000000000000000000000": "`+slot0+`"}}}
	}`), 0o644))

	r := newTxReplay(t, path)
	for _, noMaterialize := range []bool{false, true} {
		res := r.run(t, noMaterialize)
		require.False(t, res.Failed(), "noMaterialize=%v: %v", noMaterialize, res.Err)
		require.Equal(t, common.FromHex(slot0), res.ReturnData, "noMaterialize=%v", noMaterialize)
		require.Greater(t, res.ReceiptGasUsed, uint64(21000), "noMaterialize=%v", noMaterialize)
	}
}
