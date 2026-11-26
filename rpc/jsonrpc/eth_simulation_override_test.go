package jsonrpc

import (
	"context"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
)

func contractGenesis(t *testing.T, cfg *chain.Config, contract common.Address, account types.GenesisAccount) (*execmoduletester.ExecModuleTester, common.Address) {
	t.Helper()
	bankAddr := testAddr
	alloc := types.GenesisAlloc{
		bankAddr: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil)},
		contract: account,
	}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{Config: cfg, Alloc: alloc, GasLimit: 60_000_000}), execmoduletester.WithKey(testKey))
	chainPack, err := m.GenerateChain(1, func(int, *blockgen.BlockGen) {})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chainPack))
	return m, bankAddr
}

func returningByte(b byte) []byte {
	return []byte{byte(vm.PUSH1), b, byte(vm.PUSH1), 0x00, byte(vm.MSTORE), byte(vm.PUSH1), 0x20, byte(vm.PUSH1), 0x00, byte(vm.RETURN)}
}

var simulateBases = []struct {
	name string
	base rpc.BlockNumberOrHash
}{
	{"historical", rpc.BlockNumberOrHashWithNumber(0)},
	{"latest", rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)},
}

var simulateOverrideConfigs = []struct {
	name string
	cfg  *chain.Config
}{
	{"osaka", chain.TestChainOsakaConfig},
	{"amsterdam", chain.AllProtocolChanges},
}

func TestSimulateV1FullStateOverrideKeepsCode(t *testing.T) {
	existing := common.HexToAddress("0x00000000000000000000000000000000c0ffee09")
	fresh := common.HexToAddress("0x00000000000000000000000000000000c0ffee0a")
	overrideCode := hexutil.Bytes(returningByte(0x2b))
	for _, tc := range []struct {
		name     string
		target   common.Address
		override ethapi.Account
		want     byte
	}{
		{"existing contract", existing, ethapi.Account{State: &map[common.Hash]common.Hash{{1}: {1}}}, 0x2a},
		{"existing contract with code override", existing, ethapi.Account{State: &map[common.Hash]common.Hash{{1}: {1}}, Code: &overrideCode}, 0x2b},
		{"new address", fresh, ethapi.Account{State: &map[common.Hash]common.Hash{{1}: {1}}, Code: &overrideCode}, 0x2b},
	} {
		for _, cfg := range simulateOverrideConfigs {
			for _, base := range simulateBases {
				t.Run(tc.name+"/"+cfg.name+"/"+base.name, func(t *testing.T) {
					m, bankAddr := contractGenesis(t, cfg.cfg, existing, types.GenesisAccount{Balance: new(big.Int), Code: returningByte(0x2a)})
					api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
					gas := hexutil.Uint64(100_000)
					call := []ethapi.CallArgs{{From: &bankAddr, To: &tc.target, Gas: &gas}}
					result, err := api.SimulateV1(context.Background(), SimulationRequest{
						BlockStateCalls: []SimulatedBlock{
							{StateOverrides: &ethapi.StateOverrides{accounts.InternAddress(tc.target): tc.override}, Calls: call},
							{Calls: call},
						},
					}, base.base)
					require.NoError(t, err)
					require.Len(t, result, 2)
					want := hexutil.Encode(common.LeftPadBytes([]byte{tc.want}, 32))
					for i, block := range result {
						require.Len(t, block.Calls, 1)
						require.Equal(t, want, block.Calls[0].ReturnData, "block %d", i)
					}
				})
			}
		}
	}
}

func TestSimulateV1EmptyFullStateOverrideClearsStorage(t *testing.T) {
	contract := common.HexToAddress("0x00000000000000000000000000000000c0ffee0b")
	// Store 2 in slot 1, then return slot 0.
	code := []byte{
		byte(vm.PUSH1), 0x02, byte(vm.PUSH1), 0x01, byte(vm.SSTORE),
		byte(vm.PUSH1), 0x00, byte(vm.SLOAD), byte(vm.PUSH1), 0x00, byte(vm.MSTORE),
		byte(vm.PUSH1), 0x20, byte(vm.PUSH1), 0x00, byte(vm.RETURN),
	}
	for _, touched := range []bool{true, false} {
		for _, cfg := range simulateOverrideConfigs {
			for _, base := range simulateBases {
				name := "untouched"
				if touched {
					name = "touched"
				}
				t.Run(name+"/"+cfg.name+"/"+base.name, func(t *testing.T) {
					simulate := func(storage map[common.Hash]common.Hash, override bool) ethapi.RPCBlocks {
						m, bankAddr := contractGenesis(t, cfg.cfg, contract, types.GenesisAccount{Balance: new(big.Int), Code: code, Storage: storage})
						api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
						gas := hexutil.Uint64(1_000_000)
						call := []ethapi.CallArgs{{From: &bankAddr, To: &contract, Gas: &gas}}
						var first SimulatedBlock
						if touched {
							first.Calls = call
						}
						if override {
							first.StateOverrides = &ethapi.StateOverrides{accounts.InternAddress(contract): {State: &map[common.Hash]common.Hash{}}}
						}
						result, err := api.SimulateV1(context.Background(), SimulationRequest{
							BlockStateCalls: []SimulatedBlock{first, {Calls: call}},
						}, base.base)
						require.NoError(t, err)
						require.Len(t, result, 2)
						for i, block := range result {
							for _, c := range block.Calls {
								require.Equal(t, uint64(types.ReceiptStatusSuccessful), uint64(c.Status), "block %d: %v", i, c.Error)
							}
						}
						return result
					}
					got := simulate(map[common.Hash]common.Hash{{}: {1}}, true)
					want := simulate(nil, false)
					zero := hexutil.Encode(make([]byte, 32))
					if touched {
						require.Equal(t, zero, got[0].Calls[0].ReturnData)
					}
					require.Equal(t, zero, got[1].Calls[0].ReturnData)
					// On a historical base without commitment history, the root is wrong for a full storage replacement.
					if base.name == "latest" {
						require.Equal(t, want[0].StateRoot, got[0].StateRoot)
					}
				})
			}
		}
	}
}

// Every way a simulation removes all of a contract's stored slots counts against one bound: an emptied contract
// that a call touches (EIP-161), a contract created over it, and a "state" override.
func TestSimulateV1BoundsTheStorageItRemoves(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	// The bank's first CREATE lands on the contract.
	contract := types.CreateAddress(testAddr, 0)
	zero := hexutil.Uint64(0)
	empty := hexutil.Bytes{}
	noBalance := hexutil.U256{}
	noBalancePtr := &noBalance
	// PUSH1 1 SLOAD PUSH1 0 MSTORE PUSH1 32 PUSH1 0 RETURN
	readSlot1 := hexutil.Bytes{byte(vm.PUSH1), 0x01, byte(vm.SLOAD), byte(vm.PUSH1), 0x00, byte(vm.MSTORE), byte(vm.PUSH1), 0x20, byte(vm.PUSH1), 0x00, byte(vm.RETURN)}
	gas := hexutil.Uint64(100_000)
	removals := []struct {
		name  string
		block func(bank common.Address) SimulatedBlock
	}{
		{"deleted", func(bank common.Address) SimulatedBlock {
			return SimulatedBlock{
				StateOverrides: &ethapi.StateOverrides{accounts.InternAddress(contract): {Nonce: &zero, Code: &empty, Balance: &noBalancePtr}},
				Calls:          []ethapi.CallArgs{{From: &bank, To: &contract, Gas: &gas}},
			}
		}},
		{"created over", func(bank common.Address) SimulatedBlock {
			return SimulatedBlock{
				StateOverrides: &ethapi.StateOverrides{accounts.InternAddress(contract): {Nonce: &zero, Code: &empty}, accounts.InternAddress(bank): {Nonce: &zero}},
				Calls:          []ethapi.CallArgs{{From: &bank, Gas: &gas, Input: &empty}},
			}
		}},
		{"state override", func(common.Address) SimulatedBlock {
			return SimulatedBlock{StateOverrides: &ethapi.StateOverrides{accounts.InternAddress(contract): {State: &map[common.Hash]common.Hash{}}}}
		}},
	}
	for _, size := range []struct {
		name  string
		slots int
		fails bool
	}{
		{"at the bound", maxSimRemovedSlots, false},
		{"above the bound", maxSimRemovedSlots + 1, true},
	} {
		storage := make(map[common.Hash]common.Hash, size.slots)
		for i := range size.slots {
			storage[common.BigToHash(big.NewInt(int64(i+1)))] = common.Hash{31: 1}
		}
		for _, cfg := range simulateOverrideConfigs {
			// SimulateV1 does not write to the database, so the subtests share one chain.
			m, bankAddr := contractGenesis(t, cfg.cfg, contract, types.GenesisAccount{Nonce: 1, Balance: big.NewInt(1), Code: []byte{0x00}, Storage: storage})
			api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
			for _, removal := range removals {
				for _, base := range simulateBases {
					t.Run(removal.name+"/"+size.name+"/"+cfg.name+"/"+base.name, func(t *testing.T) {
						result, err := api.SimulateV1(context.Background(), SimulationRequest{
							BlockStateCalls: []SimulatedBlock{
								removal.block(bankAddr),
								{
									StateOverrides: &ethapi.StateOverrides{accounts.InternAddress(contract): {Code: &readSlot1}},
									Calls:          []ethapi.CallArgs{{From: &bankAddr, To: &contract, Gas: &gas}},
								},
							},
						}, base.base)
						if !size.fails {
							require.NoError(t, err)
							require.Len(t, result, 2)
							for _, c := range result[0].Calls {
								require.Equal(t, uint64(types.ReceiptStatusSuccessful), uint64(c.Status), "%v", c.Error)
							}
							require.Equal(t, hexutil.Encode(make([]byte, 32)), result[1].Calls[0].ReturnData, "slot 1 after the removal")
							return
						}
						require.ErrorContains(t, err, "the simulation removes more than 100000 storage slots")
						var rpcErr rpc.Error
						require.ErrorAs(t, err, &rpcErr)
						require.Equal(t, rpc.ErrCodeClientLimitExceeded, rpcErr.ErrorCode())
					})
				}
			}
		}
	}
}

// On a historical base, deploying at the real address of a contract that is created after the base block, and holds
// more slots than the bound at the head, removes nothing: it is not refused, the later slots read as empty, and the
// state roots are the ones of the same simulation on a chain that ends at the base block.
func TestSimulateV1HistoricalDeployIgnoresLaterStorage(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	cfg := chain.TestChainOsakaConfig.Copy()
	cfg.OsakaTime = nil // the transaction that fills the contract needs more gas than the EIP-7825 cap
	newChain := func() *execmoduletester.ExecModuleTester {
		return execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{
			Config: cfg, GasLimit: 3_000_000_000,
			Alloc: types.GenesisAlloc{testAddr: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil)}},
		}), execmoduletester.WithKey(testKey))
	}
	contract := types.CreateAddress(testAddr, 0)
	slots := maxSimRemovedSlots + 1
	// PUSH3 slots; loop: PUSH1 1 DUP2 SSTORE PUSH1 1 SWAP1 SUB DUP1 PUSH1 4 JUMPI; STOP
	fill := []byte{
		byte(vm.PUSH3), byte(slots >> 16), byte(slots >> 8), byte(slots), byte(vm.JUMPDEST),
		byte(vm.PUSH1), 1, byte(vm.DUP2), byte(vm.SSTORE), byte(vm.PUSH1), 1, byte(vm.SWAP1), byte(vm.SUB),
		byte(vm.DUP1), byte(vm.PUSH1), 4, byte(vm.JUMPI), byte(vm.STOP),
	}
	later := newChain()
	pack, err := later.GenerateChain(2, func(i int, b *blockgen.BlockGen) {
		if i != 0 {
			return
		}
		txn, err := types.SignTx(types.NewContractCreation(0, new(uint256.Int), 2_400_000_000, uint256.NewInt(1_000_000_000), fill), *types.LatestSigner(later.ChainConfig), testKey)
		require.NoError(t, err)
		b.AddTx(txn)
	})
	require.NoError(t, err)
	require.NoError(t, later.InsertChain(pack))
	require.Equal(t, types.ReceiptStatusSuccessful, pack.Receipts[0][0].Status)
	require.Equal(t, contract, pack.Receipts[0][0].ContractAddress)

	// PUSH1 1 SLOAD PUSH1 0 MSTORE PUSH1 32 PUSH1 0 RETURN, deployed by its init code.
	readSlot1 := []byte{byte(vm.PUSH1), 1, byte(vm.SLOAD), byte(vm.PUSH1), 0, byte(vm.MSTORE), byte(vm.PUSH1), 32, byte(vm.PUSH1), 0, byte(vm.RETURN)}
	deploy := hexutil.Bytes(append(append([]byte{byte(vm.PUSH11)}, readSlot1...), byte(vm.PUSH1), 0, byte(vm.MSTORE), byte(vm.PUSH1), 11, byte(vm.PUSH1), 21, byte(vm.RETURN)))
	gas := hexutil.Uint64(1_000_000)
	req := SimulationRequest{BlockStateCalls: []SimulatedBlock{
		{Calls: []ethapi.CallArgs{{From: &testAddr, Gas: &gas, Input: &deploy}}},
		{Calls: []ethapi.CallArgs{{From: &testAddr, To: &contract, Gas: &gas}}},
	}}
	got, err := newEthApiForTest(newBaseApiForTest(later), later.DB, nil, nil).SimulateV1(context.Background(), req, rpc.BlockNumberOrHashWithNumber(0))
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, uint64(types.ReceiptStatusSuccessful), uint64(got[0].Calls[0].Status), "%v", got[0].Calls[0].Error)
	require.Equal(t, hexutil.Encode(make([]byte, 32)), got[1].Calls[0].ReturnData, "slot 1 of the later contract")

	base := newChain()
	want, err := newEthApiForTest(newBaseApiForTest(base), base.DB, nil, nil).SimulateV1(context.Background(), req, rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber))
	require.NoError(t, err)
	for i := range want {
		require.NotEqual(t, common.Hash{}, want[i].StateRoot, "block %d", i)
		require.Equal(t, want[i].StateRoot, got[i].StateRoot, "block %d", i)
		require.Equal(t, want[i].Calls[0].ReturnData, got[i].Calls[0].ReturnData, "block %d", i)
	}
}
