// Copyright 2021 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package tracetest

import (
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/execution/chain"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/tests/testutil"
	"github.com/erigontech/erigon/execution/tracing/tracers"
	debugtracer "github.com/erigontech/erigon/execution/tracing/tracers/debug"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// prestateTrace is the result of a prestateTrace run.
type prestateTrace = map[accounts.Address]*account

type account struct {
	Balance string                      `json:"balance"`
	Code    string                      `json:"code"`
	Nonce   uint64                      `json:"nonce"`
	Storage map[common.Hash]common.Hash `json:"storage"`
}

// testcase defines a single test to check the stateDiff tracer against.
type testcase struct {
	Genesis      *types.Genesis  `json:"genesis"`
	Context      *callContext    `json:"context"`
	Input        string          `json:"input"`
	TracerConfig json.RawMessage `json:"tracerConfig"`
	Result       any             `json:"result"`
}

func TestPrestateTracerLegacy(t *testing.T) {
	testPrestateTracer("prestateTracerLegacy", "prestate_tracer_legacy", t)
}

func TestPrestateTracer(t *testing.T) {
	testPrestateTracer("prestateTracer", "prestate_tracer", t)
}

func TestPrestateWithDiffModeTracer(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	testPrestateTracer("prestateTracer", "prestate_tracer_with_diff_mode", t)
}

func testPrestateTracer(tracerName string, dirPath string, t *testing.T) {
	files, err := dir.ReadDir(filepath.Join("testdata", dirPath))
	if err != nil {
		t.Fatalf("failed to retrieve tracer test suite: %v", err)
	}
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".json") {
			continue
		}
		file := file // capture range variable
		t.Run(camel(strings.TrimSuffix(file.Name(), ".json")), func(t *testing.T) {
			t.Parallel()

			test := new(testcase)
			// Call tracer test found, read if from disk
			if blob, err := os.ReadFile(filepath.Join("testdata", dirPath, file.Name())); err != nil {
				t.Fatalf("failed to read testcase: %v", err)
			} else if err := json.Unmarshal(blob, test); err != nil {
				t.Fatalf("failed to parse testcase: %v", err)
			}
			tx, err := types.UnmarshalTransactionFromBinary(common.FromHex(test.Input), false /* blobTxnsAreWrappedWithBlobs */)
			if err != nil {
				t.Fatalf("failed to parse testcase input: %v", err)
			}
			// Configure a blockchain with the given prestate
			signer := types.MakeSigner(test.Genesis.Config, uint64(test.Context.Number), uint64(test.Context.Time))
			context := evmtypes.BlockContext{
				CanTransfer: protocol.CanTransfer,
				Transfer:    misc.Transfer,
				Coinbase:    accounts.InternAddress(test.Context.Miner),
				BlockNumber: uint64(test.Context.Number),
				Time:        uint64(test.Context.Time),
				GasLimit:    uint64(test.Context.GasLimit),
			}
			if test.Context.Difficulty != nil {
				context.Difficulty = *test.Context.Difficulty
			}
			if test.Context.BaseFee != nil {
				baseFee := test.Context.BaseFee
				context.BaseFee = *baseFee
			}
			rules := context.Rules(test.Genesis.Config)
			m := execmoduletester.New(t)
			dbTx, err := m.DB.BeginTemporalRw(m.Ctx)
			require.NoError(t, err)
			defer dbTx.Rollback()
			statedb, err := testutil.MakePreState(rules, m.DB, dbTx, test.Genesis.Alloc, context.BlockNumber)
			require.NoError(t, err)
			tracer, err := tracers.New(tracerName, new(tracers.Context), test.TracerConfig)
			if err != nil {
				t.Fatalf("failed to create call tracer: %v", err)
			}
			if outputDir, ok := os.LookupEnv("PRESTATE_TRACER_TEST_DEBUG_TRACER_OUTPUT_DIR"); ok {
				recordOptions := debugtracer.RecordOptions{
					DisableOnOpcodeMemoryRecording:    true,
					DisableOnOpcodeStackRecording:     true,
					DisableOnBlockchainInitRecording:  true,
					DisableOnBlockStartRecording:      true,
					DisableOnBlockEndRecording:        true,
					DisableOnGenesisBlockRecording:    true,
					DisableOnSystemCallStartRecording: true,
					DisableOnSystemCallEndRecording:   true,
					DisableOnBalanceChangeRecording:   true,
					DisableOnNonceChangeRecording:     true,
					DisableOnStorageChangeRecording:   true,
					DisableOnLogRecording:             true,
				}
				tracer = debugtracer.New(
					outputDir,
					debugtracer.WithWrappedTracer(tracer),
					debugtracer.WithRecordOptions(recordOptions),
					debugtracer.WithFlushMode(debugtracer.FlushModeTxn),
				)
			}
			statedb.SetHooks(tracer.Hooks)
			msg, err := tx.AsMessage(*signer, test.Context.BaseFee, rules)
			if err != nil {
				t.Fatalf("failed to prepare transaction for tracing: %v", err)
			}
			txContext := protocol.NewEVMTxContext(msg)
			evm := vm.NewEVM(context, txContext, statedb, test.Genesis.Config, vm.Config{Tracer: tracer.Hooks})
			tracer.OnTxStart(evm.GetVMContext(), tx, msg.From())
			st := protocol.NewTxnExecutor(evm, msg, new(protocol.GasPool).AddGas(tx.GetGasLimit()).AddBlobGas(tx.GetBlobGas()))
			vmRet, err := st.Execute(true /* refunds */, false /* gasBailout */)
			if err != nil {
				t.Fatalf("failed to execute transaction: %v", err)
			}
			tracer.EmitTxEnd(&types.Receipt{GasUsed: vmRet.ReceiptGasUsed}, vmRet.TxnGasUsage, nil)
			// Retrieve the trace result and compare against the expected
			res, err := tracer.GetResult()
			if err != nil {
				t.Fatalf("failed to retrieve trace result: %v", err)
			}
			// The legacy javascript calltracer marshals json in js, which
			// is not deterministic (as opposed to the golang json encoder).
			if strings.HasSuffix(dirPath, "_legacy") {
				// This is a tweak to make it deterministic. Can be removed when
				// we remove the legacy tracer.
				var x prestateTrace
				if err := json.Unmarshal(res, &x); err != nil {
					t.Fatalf("prestateTrace tweak Unmarshal: %v", err)
				}
				if res, err = json.Marshal(x); err != nil {
					t.Fatalf("prestateTrace tweak Marshal: %v", err)
				}
			}
			want, err := json.Marshal(test.Result)
			if err != nil {
				t.Fatalf("failed to marshal test: %v", err)
			}
			if string(want) != string(res) {
				t.Fatalf("trace mismatch\n have: %v\n want: %v\n", string(res), string(want))
			}
		})
	}
}

func tracePrestateDiff(t *testing.T, config *chain.Config, alloc types.GenesisAlloc, to common.Address) (pre, post map[common.Address]json.RawMessage) {
	t.Helper()
	privkey, err := crypto.HexToECDSA("0000000000000000deadbeef00000000000000000000000000000000deadbeef")
	require.NoError(t, err)
	signer := types.LatestSigner(config)
	tx, err := types.SignNewTx(privkey, *signer, &types.LegacyTx{
		GasPrice: *uint256.NewInt(0),
		CommonTx: types.CommonTx{GasLimit: 5000000, To: &to},
	})
	require.NoError(t, err)
	origin, _ := signer.Sender(tx)
	alloc[origin.Value()] = types.GenesisAccount{Balance: big.NewInt(500000000000000)}
	context := evmtypes.BlockContext{
		CanTransfer: protocol.CanTransfer,
		Transfer:    misc.Transfer,
		Coinbase:    accounts.ZeroAddress,
		BlockNumber: 8000000,
		Time:        5,
		Difficulty:  *uint256.NewInt(0x30000),
		GasLimit:    uint64(6000000),
	}
	rules := context.Rules(config)
	m := execmoduletester.New(t)
	dbTx, err := m.DB.BeginTemporalRw(m.Ctx)
	require.NoError(t, err)
	defer dbTx.Rollback()
	statedb, err := testutil.MakePreState(rules, m.DB, dbTx, alloc, context.BlockNumber)
	require.NoError(t, err)
	tracer, err := tracers.New("prestateTracer", nil, json.RawMessage(`{"diffMode":true}`))
	require.NoError(t, err)
	statedb.SetHooks(tracer.Hooks)
	txContext := evmtypes.TxContext{Origin: origin, GasPrice: *uint256.NewInt(0)}
	evm := vm.NewEVM(context, txContext, statedb, config, vm.Config{Tracer: tracer.Hooks})
	msg, err := tx.AsMessage(*signer, nil, rules)
	require.NoError(t, err)
	tracer.OnTxStart(evm.GetVMContext(), tx, msg.From())
	st := protocol.NewTxnExecutor(evm, msg, new(protocol.GasPool).AddGas(tx.GetGasLimit()))
	vmRet, err := st.Execute(true, false)
	require.NoError(t, err)
	tracer.EmitTxEnd(&types.Receipt{GasUsed: vmRet.ReceiptGasUsed}, vmRet.TxnGasUsage, nil)
	res, err := tracer.GetResult()
	require.NoError(t, err)
	var out struct {
		Pre  map[common.Address]json.RawMessage `json:"pre"`
		Post map[common.Address]json.RawMessage `json:"post"`
	}
	require.NoError(t, json.Unmarshal(res, &out))
	return out.Pre, out.Post
}

func TestPrestateDiffModeSelfdestructSurvivesUnrelatedRevert(t *testing.T) {
	var (
		driver    = common.HexToAddress("0x00000000000000000000000000000000000000aa")
		reverter  = common.HexToAddress("0x00000000000000000000000000000000000000bb")
		destroyed = common.HexToAddress("0x00000000000000000000000000000000000000dd")
	)
	selfdestruct := []byte{byte(vm.PUSH1), 0xee, byte(vm.SELFDESTRUCT)}

	run := func(callReverter bool) json.RawMessage {
		code := evmCallTo(0xdd)
		if callReverter {
			code = append(code, evmCallTo(0xbb)...)
		}
		code = append(code, byte(vm.STOP))
		alloc := types.GenesisAlloc{
			driver:    {Nonce: 1, Code: code},
			reverter:  {Nonce: 1, Code: evmRevert},
			destroyed: {Nonce: 1, Code: selfdestruct, Balance: big.NewInt(7)},
		}
		_, post := tracePrestateDiff(t, chainspec.Mainnet.Config, alloc, driver)
		return post[destroyed]
	}

	require.Nil(t, run(false), "self-destructed account must not appear in post")
	require.Nil(t, run(true), "a later, unrelated reverted call must not resurrect the self-destructed account in post")
}

func TestPrestateDiffModeFailedCreateDoesNotMarkCollidedAccountCreated(t *testing.T) {
	cancun := *chain.AllProtocolChanges
	cancun.PragueTime, cancun.OsakaTime, cancun.AmsterdamTime = nil, nil, nil

	driver := common.HexToAddress("0x00000000000000000000000000000000000000aa")
	victim := types.CreateAddress(driver, 1)
	code := []byte{
		byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00, byte(vm.CREATE), byte(vm.POP),
		byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00, byte(vm.PUSH1), 0x00,
		byte(vm.PUSH20),
	}
	code = append(code, victim[:]...)
	code = append(code, byte(vm.GAS), byte(vm.CALL), byte(vm.POP), byte(vm.STOP))
	alloc := types.GenesisAlloc{
		driver: {Nonce: 1, Code: code},
		victim: {Nonce: 1, Code: []byte{byte(vm.PUSH1), 0xee, byte(vm.SELFDESTRUCT)}, Balance: big.NewInt(7)},
	}

	_, post := tracePrestateDiff(t, &cancun, alloc, driver)
	require.NotNil(t, post[victim], "a CREATE that collided with the account must not let its SELFDESTRUCT delete it under EIP-6780")
}

func TestPrestateDiffModeRevertedSelfdestructIsNotDeleted(t *testing.T) {
	var (
		driver    = common.HexToAddress("0x00000000000000000000000000000000000000aa")
		middle    = common.HexToAddress("0x00000000000000000000000000000000000000cc")
		destroyed = common.HexToAddress("0x00000000000000000000000000000000000000dd")
	)
	alloc := types.GenesisAlloc{
		driver:    {Nonce: 1, Code: append(evmCallTo(0xcc), byte(vm.STOP))},
		middle:    {Nonce: 1, Code: append(evmCallTo(0xdd), evmRevert...)},
		destroyed: {Nonce: 1, Code: []byte{byte(vm.PUSH1), 0xee, byte(vm.SELFDESTRUCT)}, Balance: big.NewInt(7)},
	}

	pre, post := tracePrestateDiff(t, chainspec.Mainnet.Config, alloc, driver)
	require.Nil(t, pre[destroyed], "reverted SELFDESTRUCT must leave the account out of pre")
	require.Nil(t, post[destroyed], "reverted SELFDESTRUCT must leave the account out of post")
}
