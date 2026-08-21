// Package benchmark provides EVM performance benchmarks targeting real-world
// bottlenecks identified from mainnet block analysis.
package benchmark

import (
	"errors"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/runtime"
)

// Well-known test addresses
var (
	addrSender   = accounts.InternAddress(common.HexToAddress("0xCafe01"))
	addrContract = accounts.InternAddress(common.HexToAddress("0xC0DE01"))
	addrRouter   = accounts.InternAddress(common.HexToAddress("0x5001"))
	addrPair     = accounts.InternAddress(common.HexToAddress("0x5002"))
	addrTokenA   = accounts.InternAddress(common.HexToAddress("0x5003"))
	addrTokenB   = accounts.InternAddress(common.HexToAddress("0x5004"))
)

// cancunConfig returns a chain config with all forks enabled through Cancun.
func cancunConfig() *chain.Config {
	return &chain.Config{
		ChainID:               uint256.NewInt(1),
		HomesteadBlock:        common.NewUint64(0),
		TangerineWhistleBlock: common.NewUint64(0),
		SpuriousDragonBlock:   common.NewUint64(0),
		ByzantiumBlock:        common.NewUint64(0),
		ConstantinopleBlock:   common.NewUint64(0),
		PetersburgBlock:       common.NewUint64(0),
		IstanbulBlock:         common.NewUint64(0),
		MuirGlacierBlock:      common.NewUint64(0),
		BerlinBlock:           common.NewUint64(0),
		LondonBlock:           common.NewUint64(0),
		ArrowGlacierBlock:     common.NewUint64(0),
		GrayGlacierBlock:      common.NewUint64(0),
		ShanghaiTime:          common.NewUint64(0),
		CancunTime:            common.NewUint64(0),
	}
}

// benchConfig builds the EVM a benchmark runs every iteration against, on
// Cancun rules and the parallel-execution path staged sync uses. One EVM per
// benchmark, as staged sync keeps one per worker: building it per iteration
// costs more than the cheap benchmarks measure and throws away the caches it
// interns into.
func benchConfig(b *testing.B, gasLimit uint64) *vm.EVM {
	b.Helper()
	return newBenchEnv(b, gasLimit, true)
}

// newBenchEnv returns the env, not the runtime.Config it built: NewEnv ignores
// Config.Value — only runtime.Call reads it — so a Value set here would never
// reach the call.
func newBenchEnv(t testing.TB, gasLimit uint64, noMaterialize bool) *vm.EVM {
	t.Helper()

	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, domains := temporaltest.NewTestTxSD(t, db)

	err := rawdbv3.TxNums.Append(tx, 1, 1)
	require.NoError(t, err)

	statedb := state.NewWithVersionMap(
		state.NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{})),
		state.NewVersionMap(nil),
	)
	statedb.SetNoMaterialize(noMaterialize)

	return runtime.NewEnv(&runtime.Config{
		ChainConfig: cancunConfig(),
		Origin:      addrSender,
		Coinbase:    accounts.ZeroAddress,
		BlockNumber: 1,
		Time:        1,
		GasLimit:    gasLimit,
		Difficulty:  uint256.NewInt(0),
		State:       statedb,
	})
}

// deployContract deploys code at the given address in the state.
func deployContract(tb testing.TB, statedb *state.IntraBlockState, addr accounts.Address, code []byte) {
	tb.Helper()
	require.NoError(tb, statedb.CreateAccount(addr, true))
	require.NoError(tb, statedb.SetCode(addr, code, tracing.CodeChangeUnspecified))
}

// deployContractWithBalance deploys code and sets an ETH balance.
func deployContractWithBalance(tb testing.TB, statedb *state.IntraBlockState, addr accounts.Address, code []byte, balance *uint256.Int) {
	tb.Helper()
	deployContract(tb, statedb, addr, code)
	require.NoError(tb, statedb.SetBalance(addr, *balance, 0))
}

// setStorage pre-populates storage slots for a contract address.
func setStorage(tb testing.TB, statedb *state.IntraBlockState, addr accounts.Address, slots map[uint256.Int]uint256.Int) {
	tb.Helper()
	for k, v := range slots {
		key := accounts.InternKey(k.Bytes32())
		require.NoError(tb, statedb.SetState(addr, key, v))
	}
}

// prepareAndCall sets up EVM access lists and calls the contract.
func prepareAndCall(vmenv *vm.EVM, addr accounts.Address, input []byte) ([]byte, mdgas.MdGas, error) {
	rules := vmenv.ChainRules()
	vmenv.IntraBlockState().Prepare(rules, vmenv.Origin, vmenv.Context.Coinbase, addr, vm.ActivePrecompiles(rules), nil)
	gas := mdgas.SplitTxnGasLimit(vmenv.Context.GasLimit, 0, rules)
	ret, left, _, err := vmenv.Call(vmenv.Origin, addr, input, gas, uint256.Int{}, false)
	return ret, left, err
}

// callOOG runs one iteration of a benchmark whose program loops until the gas
// budget is gone, and requires that it did. A call reaching an address with no
// code returns no error and burns nothing, so the gas assertion — not the error
// — is what catches a fixture that stopped working.
func callOOG(b *testing.B, vmenv *vm.EVM, addr accounts.Address) {
	statedb := vmenv.IntraBlockState()
	snap := statedb.PushSnapshot()
	_, left, err := prepareAndCall(vmenv, addr, nil)
	if !errors.Is(err, vm.ErrOutOfGas) || left.Total() != 0 {
		b.Fatalf("expected gas exhaustion, got gasLeft=%d err=%v", left.Total(), err)
	}
	statedb.RevertToSnapshot(snap, nil)
	statedb.PopSnapshot(snap)
}

// callComplete runs one iteration of a benchmark whose program returns on its
// own, and requires that it finished and did work. See callOOG on why the gas
// check carries the weight.
func callComplete(b *testing.B, vmenv *vm.EVM, addr accounts.Address, input []byte) {
	statedb := vmenv.IntraBlockState()
	snap := statedb.PushSnapshot()
	_, left, err := prepareAndCall(vmenv, addr, input)
	if err != nil {
		b.Fatal(err)
	}
	if limit := vmenv.Context.GasLimit; left.Total() >= limit {
		b.Fatalf("call consumed no gas (gasLeft=%d, limit=%d)", left.Total(), limit)
	}
	statedb.RevertToSnapshot(snap, nil)
	statedb.PopSnapshot(snap)
}
