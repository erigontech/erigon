package benchmark

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/chain"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/testutil"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// txFixturesDir holds what `evm fixture` writes; it is gitignored.
const txFixturesDir = "testdata/txs"

// BenchmarkTxReplay replays each tx fetched into testdata/txs, as
// eth_call runs it: a fresh state over the tx's prestate per iteration, on the
// materializing and the noMaterialize path.
func BenchmarkTxReplay(b *testing.B) {
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
	Tx      json.RawMessage `json:"tx"`
	Receipt struct {
		GasUsed hexutil.Uint64 `json:"gasUsed"`
		Status  hexutil.Uint64 `json:"status"`
	} `json:"receipt"`
	Block    types.Header       `json:"block"`
	Prestate types.GenesisAlloc `json:"prestate"`
}

type txReplay struct {
	cfg      *chain.Config
	rules    *chain.Rules
	blockCtx evmtypes.BlockContext
	reader   state.StateReader
	txn      types.Transaction
	signer   types.Signer
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
	txn, err := types.UnmarshalTransactionFromJSON(f.Tx)
	require.NoError(tb, err)
	var sender struct {
		From common.Address `json:"from"`
	}
	require.NoError(tb, json.Unmarshal(f.Tx, &sender))
	txn.SetSender(accounts.InternAddress(sender.From))

	cfg := chainspec.Mainnet.Config
	noHashes := func(uint64) (common.Hash, error) { return common.Hash{}, nil }
	blockCtx := protocol.NewEVMBlockContext(&f.Block, noHashes, nil, accounts.InternAddress(f.Block.Coinbase), cfg)
	rules := blockCtx.Rules(cfg)

	db := temporaltest.NewTestDB(tb, datadir.New(tb.TempDir()))
	tx, domains := temporaltest.NewTestTxSD(tb, db)
	_, err = testutil.MakePreStateInto(rules, domains, tx, f.Prestate, 1)
	require.NoError(tb, err)

	return &txReplay{
		cfg:      cfg,
		rules:    rules,
		blockCtx: blockCtx,
		reader:   state.NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{})),
		txn:      txn,
		signer:   *types.MakeSigner(cfg, f.Block.Number.Uint64(), f.Block.Time),
		gasUsed:  uint64(f.Receipt.GasUsed),
		failed:   f.Receipt.Status == 0,
	}
}

// run executes the tx once on a fresh state, as DoCall sets one up.
func (r *txReplay) run(tb testing.TB, noMaterialize bool) *evmtypes.ExecutionResult {
	ibs := state.New(r.reader)
	defer ibs.Close()
	if noMaterialize {
		ibs.SetVersionMap(state.NewVersionMap(nil))
		ibs.SetNoMaterialize(true)
		ibs.SetTxContext(0, 0)
		ibs.SetNoConflictDetection()
	}
	msg, err := r.txn.AsMessage(r.signer, &r.blockCtx.BaseFee, r.rules)
	require.NoError(tb, err)
	msg.SetCheckNonce(false)
	msg.SetCheckTransaction(false)
	msg.SetCheckGas(false)
	txCtx := protocol.NewEVMTxContext(msg)
	vmConfig := vm.Config{NoBaseFee: true, NoReceipts: true, NoBAL: true}
	evm := vm.NewEVM(vm.ZeroUnpricedBaseFee(r.blockCtx, txCtx, vmConfig), txCtx, ibs, r.cfg, vmConfig)
	gp := new(protocol.GasPool).AddGas(msg.Gas()).AddBlobGas(msg.BlobGas())
	res, err := protocol.ApplyMessage(evm, msg, gp, true, false, nil)
	require.NoError(tb, err)
	return res
}

// The replay runs the prestate's code over its storage on both paths: a call
// returning a seeded slot proves both reach the state the fixture describes.
func TestTxReplayUsesThePrestate(t *testing.T) {
	const legacy = `"gasPrice": "0x1", "type": "0x0", "v": "0x0", "r": "0x0", "s": "0x0"`
	// The authorization's signature does not recover, so EIP-7702 skips it and
	// the call still runs.
	const setCode = `"chainId": "0x1", "maxFeePerGas": "0x1", "maxPriorityFeePerGas": "0x0", "type": "0x4",
		"accessList": [], "authorizationList": [{"chainId": "0x1", "address": "0x00000000000000000000000000000000000000d1",
			"nonce": "0x0", "yParity": "0x0", "r": "0x1", "s": "0x1"}], "yParity": "0x0", "v": "0x0", "r": "0x0", "s": "0x0"`
	// 21000 + PUSH1 SLOAD(cold) PUSH1 MSTORE(+1 word) PUSH1 PUSH1 RETURN = 23118,
	// plus 25000 per authorization.
	t.Run("legacy", func(t *testing.T) { testReplayUsesThePrestate(t, legacy, 23118) })
	t.Run("setCode", func(t *testing.T) { testReplayUsesThePrestate(t, setCode, 48118) })
}

func testReplayUsesThePrestate(t *testing.T, txFields string, gasUsed uint64) {
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
			"0x00000000000000000000000000000000000000c1": {"balance": "0x0", "code": "`+returnSlot0+`", "nonce": 1,
				"storage": {"0x0000000000000000000000000000000000000000000000000000000000000000": "`+slot0+`"}}}
	}`), 0o644))

	r := newTxReplay(t, path)
	for _, noMaterialize := range []bool{false, true} {
		res := r.run(t, noMaterialize)
		require.False(t, res.Failed(), "noMaterialize=%v: %v", noMaterialize, res.Err)
		require.Equal(t, common.FromHex(slot0), res.ReturnData, "noMaterialize=%v", noMaterialize)
		require.Equal(t, gasUsed, res.ReceiptGasUsed, "noMaterialize=%v", noMaterialize)
	}
}
