package jsonrpc

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
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
	bankKey, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	bankAddr := crypto.PubkeyToAddress(bankKey.PublicKey)
	alloc := types.GenesisAlloc{
		bankAddr: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil)},
		contract: account,
	}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{Config: cfg, Alloc: alloc, GasLimit: 60_000_000}), execmoduletester.WithKey(bankKey))
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
