// Copyright 2026 The Erigon Authors
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

package jsonrpc

import (
	"context"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
)

var (
	baseStateSlotContract    = common.HexToAddress("0x00000000000000000000000000000000c0ffee20")
	baseStateRecipient       = common.HexToAddress("0x00000000000000000000000000000000c0ffee21")
	baseStateBalanceContract = common.HexToAddress("0x00000000000000000000000000000000c0ffee22")
)

// With calldata: SSTORE(0, CALLDATALOAD(0)). Always: return SLOAD(0).
var writeThenReturnSlot0 = []byte{
	byte(vm.CALLDATASIZE), byte(vm.ISZERO), byte(vm.PUSH1), 0x0B, byte(vm.JUMPI),
	byte(vm.PUSH1), 0x00, byte(vm.CALLDATALOAD), byte(vm.PUSH1), 0x00, byte(vm.SSTORE),
	byte(vm.JUMPDEST), byte(vm.PUSH1), 0x00, byte(vm.SLOAD), byte(vm.PUSH1), 0x00, byte(vm.MSTORE),
	byte(vm.PUSH1), 0x20, byte(vm.PUSH1), 0x00, byte(vm.RETURN),
}

// Deploys returningByte(42).
var deployReturning42 = append([]byte{
	byte(vm.PUSH1), 10, byte(vm.DUP1), byte(vm.PUSH1), 11, byte(vm.PUSH1), 0x00, byte(vm.CODECOPY),
	byte(vm.PUSH1), 0x00, byte(vm.RETURN),
}, returningByte(42)...)

func returningBalanceOf(addr common.Address) []byte {
	code := append([]byte{byte(vm.PUSH20)}, addr[:]...)
	return append(code, byte(vm.BALANCE), byte(vm.PUSH1), 0x00, byte(vm.MSTORE), byte(vm.PUSH1), 0x20, byte(vm.PUSH1), 0x00, byte(vm.RETURN))
}

func word(n uint64) hexutil.Bytes {
	return common.LeftPadBytes(new(big.Int).SetUint64(n).Bytes(), 32)
}

type canonicalTx struct {
	to    *common.Address
	data  []byte
	value uint64
}

// baseStateChain starts a node from a fixed genesis. A non-empty head is mined
// into block 1, so the latest state differs from the genesis state.
func baseStateChain(t *testing.T, cfgIndex int, slot0 uint64, head []canonicalTx) (*execmoduletester.ExecModuleTester, common.Address) {
	t.Helper()
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	bank := crypto.PubkeyToAddress(key.PublicKey)
	slotContract := types.GenesisAccount{Balance: new(big.Int), Code: writeThenReturnSlot0}
	if slot0 != 0 {
		slotContract.Storage = map[common.Hash]common.Hash{{}: common.BytesToHash(word(slot0))}
	}
	alloc := types.GenesisAlloc{
		bank:                     {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil)},
		baseStateSlotContract:    slotContract,
		baseStateBalanceContract: {Balance: new(big.Int), Code: returningBalanceOf(baseStateRecipient)},
	}
	cfg := simulateOverrideConfigs[cfgIndex].cfg
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{Config: cfg, Alloc: alloc, GasLimit: 60_000_000}), execmoduletester.WithKey(key))
	if len(head) == 0 {
		return m, bank
	}
	signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)
	chainPack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) {
		for _, c := range head {
			txn, err := types.SignTx(&types.DynamicFeeTransaction{
				CommonTx: types.CommonTx{Nonce: b.TxNonce(bank), To: c.to, Data: c.data, Value: *uint256.NewInt(c.value), GasLimit: 1_000_000},
				ChainID:  *uint256.MustFromBig(m.ChainConfig.ChainID.ToBig()),
				FeeCap:   *uint256.NewInt(1_000_000_000_000),
			}, *signer, key)
			require.NoError(t, err)
			b.AddTx(txn)
		}
	})
	require.NoError(t, err)
	for _, receipt := range chainPack.Receipts[0] {
		require.Equal(t, types.ReceiptStatusSuccessful, receipt.Status)
	}
	require.NoError(t, m.InsertChain(chainPack))
	return m, bank
}

// A simulation on an old block must start from that block's state, not from the
// latest state, even when a simulated write matches the latest value. Each case
// is compared with the same request on a node whose latest block is the old one.
func TestSimulateV1HistoricalBaseMatchesSimulationAtBase(t *testing.T) {
	slotContract, recipient, balanceContract := baseStateSlotContract, baseStateRecipient, baseStateBalanceContract
	writeSlot := func(v uint64) ethapi.CallArgs {
		data := word(v)
		return ethapi.CallArgs{To: &slotContract, Data: &data}
	}
	readSlot := ethapi.CallArgs{To: &slotContract}
	readBalance := ethapi.CallArgs{To: &balanceContract}
	deploy := hexutil.Bytes(deployReturning42)
	for _, tc := range []struct {
		name   string
		slot0  uint64
		head   []canonicalTx
		blocks [][]ethapi.CallArgs
		want   []string
		deploy bool // the second block calls the contract the first block deploys
	}{
		{
			name:   "storage write equal to head",
			head:   []canonicalTx{{to: &slotContract, data: word(5)}},
			blocks: [][]ethapi.CallArgs{{writeSlot(5)}, {readSlot}},
			want:   []string{hexutil.Encode(word(5)), hexutil.Encode(word(5))},
		},
		{
			name:   "storage clear of a slot absent at head",
			slot0:  1,
			head:   []canonicalTx{{to: &slotContract, data: word(0)}},
			blocks: [][]ethapi.CallArgs{{writeSlot(0)}, {readSlot}},
			want:   []string{hexutil.Encode(word(0)), hexutil.Encode(word(0))},
		},
		{
			name:   "storage cleared and set back to its base value",
			slot0:  1,
			head:   []canonicalTx{{to: &slotContract, data: word(0)}},
			blocks: [][]ethapi.CallArgs{{writeSlot(0)}, {writeSlot(1)}, {readSlot}},
			want:   []string{hexutil.Encode(word(0)), hexutil.Encode(word(1)), hexutil.Encode(word(1))},
		},
		{
			name:   "storage set to its head value and cleared",
			head:   []canonicalTx{{to: &slotContract, data: word(5)}},
			blocks: [][]ethapi.CallArgs{{writeSlot(5)}, {writeSlot(0)}, {readSlot}},
			want:   []string{hexutil.Encode(word(5)), hexutil.Encode(word(0)), hexutil.Encode(word(0))},
		},
		{
			name:   "balance equal to head",
			head:   []canonicalTx{{to: &recipient, value: 1}},
			blocks: [][]ethapi.CallArgs{{{To: &recipient, Value: (*hexutil.U256)(uint256.NewInt(1))}}, {readBalance}},
			want:   []string{"0x", hexutil.Encode(word(1))},
		},
		{
			name:   "contract equal to head",
			head:   []canonicalTx{{data: deployReturning42}},
			blocks: [][]ethapi.CallArgs{{{Data: &deploy}}, {{}}},
			want:   []string{hexutil.Encode(returningByte(42)), hexutil.Encode(word(42))},
			deploy: true,
		},
		{
			name:   "nonce change of an account whose balance differs at head",
			head:   []canonicalTx{{to: &recipient, value: 1}},
			blocks: [][]ethapi.CallArgs{{{From: &recipient, To: &slotContract}}, {readBalance}},
			want:   []string{hexutil.Encode(word(0)), hexutil.Encode(word(0))},
		},
	} {
		for cfgIndex, cfg := range simulateOverrideConfigs {
			t.Run(cfg.name+"/"+tc.name, func(t *testing.T) {
				m, bank := baseStateChain(t, cfgIndex, tc.slot0, tc.head)
				oracle, _ := baseStateChain(t, cfgIndex, tc.slot0, nil)
				request := func() SimulationRequest {
					gas := hexutil.Uint64(1_000_000)
					req := SimulationRequest{}
					for _, calls := range tc.blocks {
						block := SimulatedBlock{}
						for _, call := range calls {
							if tc.deploy && call.Data == nil {
								deployed := types.CreateAddress(bank, 0)
								call.To = &deployed
							}
							if call.From == nil {
								call.From = &bank
							}
							call.Gas = &gas
							block.Calls = append(block.Calls, call)
						}
						req.BlockStateCalls = append(req.BlockStateCalls, block)
					}
					return req
				}
				got, err := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil).SimulateV1(context.Background(), request(), rpc.BlockNumberOrHashWithNumber(0))
				require.NoError(t, err)
				want, err := newEthApiForTest(newBaseApiForTest(oracle), oracle.DB, nil, nil).SimulateV1(context.Background(), request(), rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber))
				require.NoError(t, err)
				require.Len(t, got, len(tc.want))
				for i := range got {
					require.Len(t, got[i].Calls, 1)
					require.Equal(t, uint64(types.ReceiptStatusSuccessful), uint64(got[i].Calls[0].Status), "block %d: %v", i, got[i].Calls[0].Error)
					require.Equal(t, tc.want[i], got[i].Calls[0].ReturnData, "block %d return data", i)
					require.Equal(t, want[i].StateRoot, got[i].StateRoot, "block %d state root", i)
				}
			})
		}
	}
}
