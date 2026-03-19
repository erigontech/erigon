// Package benchmark provides EVM performance benchmarks targeting real-world
// bottlenecks identified from mainnet block analysis.
package benchmark

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/testutil"
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
		ChainID:               big.NewInt(1),
		HomesteadBlock:        big.NewInt(0),
		TangerineWhistleBlock: big.NewInt(0),
		SpuriousDragonBlock:   big.NewInt(0),
		ByzantiumBlock:        big.NewInt(0),
		ConstantinopleBlock:   big.NewInt(0),
		PetersburgBlock:       big.NewInt(0),
		IstanbulBlock:         big.NewInt(0),
		MuirGlacierBlock:      big.NewInt(0),
		BerlinBlock:           big.NewInt(0),
		LondonBlock:           big.NewInt(0),
		ArrowGlacierBlock:     big.NewInt(0),
		GrayGlacierBlock:      big.NewInt(0),
		ShanghaiTime:          big.NewInt(0),
		CancunTime:            big.NewInt(0),
	}
}

// benchConfig creates a runtime.Config for benchmarks with high gas limit
// and Cancun chain rules (EIP-2929 access lists, EIP-1153 transient storage).
func benchConfig(b *testing.B, gasLimit uint64) (*runtime.Config, *state.IntraBlockState) {
	b.Helper()

	db := testutil.TemporalDB(b)
	tx, domains := testutil.TemporalTxSD(b, db)

	err := rawdbv3.TxNums.Append(tx, 1, 1)
	require.NoError(b, err)

	statedb := state.NewWithVersionMap(
		state.NewReaderV3(domains.AsGetter(tx)),
		state.NewVersionMap(nil),
	)

	cfg := &runtime.Config{
		ChainConfig: cancunConfig(),
		Origin:      addrSender,
		Coinbase:    accounts.ZeroAddress,
		BlockNumber: 1,
		Time:        1,
		GasLimit:    gasLimit,
		Difficulty:  uint256.NewInt(0),
		State:       statedb,
	}

	return cfg, statedb
}

// deployContract deploys code at the given address in the state.
func deployContract(statedb *state.IntraBlockState, addr accounts.Address, code []byte) {
	statedb.CreateAccount(addr, true)
	statedb.SetCode(addr, code)
}

// deployContractWithBalance deploys code and sets an ETH balance.
func deployContractWithBalance(statedb *state.IntraBlockState, addr accounts.Address, code []byte, balance *uint256.Int) {
	statedb.CreateAccount(addr, true)
	statedb.SetCode(addr, code)
	statedb.SetBalance(addr, *balance, 0)
}

// setStorage pre-populates storage slots for a contract address.
func setStorage(statedb *state.IntraBlockState, addr accounts.Address, slots map[uint256.Int]uint256.Int) {
	for k, v := range slots {
		key := accounts.InternKey(k.Bytes32())
		statedb.SetState(addr, key, v)
	}
}

// prepareAndCall sets up EVM access lists and calls the contract.
func prepareAndCall(cfg *runtime.Config, addr accounts.Address, input []byte) ([]byte, uint64, error) {
	vmenv := runtime.NewEnv(cfg)
	rules := vmenv.ChainRules()
	cfg.State.Prepare(rules, cfg.Origin, cfg.Coinbase, addr, vm.ActivePrecompiles(rules), nil, nil)
	return vmenv.Call(cfg.Origin, addr, input, cfg.GasLimit, cfg.Value, false)
}
