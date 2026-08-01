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
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// pbinHeaderCodeCapacity is the code an account header holds: EIP-8297 gives it
// the sub-indexes from 128 to 255, one 31-byte chunk each, and everything past
// that spills into the code zone under a stem of its own.
const pbinHeaderCodeCapacity = 31 * (256 - 128)

// pbinStoreRuntime stores calldata[32:64] at slot calldata[0:32], so one deploy
// serves both a non-zero SSTORE and an SSTORE-to-zero.
var pbinStoreRuntime = []byte{
	0x60, 0x20, 0x35, // PUSH1 32; CALLDATALOAD -> value
	0x60, 0x00, 0x35, // PUSH1 0;  CALLDATALOAD -> slot
	0x55, // SSTORE
	0x00, // STOP
}

// pbinDeployCode wraps runtime code in an initcode that returns it verbatim.
func pbinDeployCode(runtime []byte) []byte {
	size := len(runtime)
	const prefixLen = 14
	initcode := []byte{
		0x61, byte(size >> 8), byte(size), // PUSH2 size
		0x60, prefixLen, // PUSH1 codeOffset
		0x60, 0x00, // PUSH1 destOffset
		0x39,                              // CODECOPY
		0x61, byte(size >> 8), byte(size), // PUSH2 size
		0x60, 0x00, // PUSH1 offset
		0xf3, // RETURN
	}
	return append(initcode, runtime...)
}

// pbinStoreCalldata is the (slot, value) pair pbinStoreRuntime writes.
func pbinStoreCalldata(slot common.Hash, value uint64) []byte {
	val := uint256.NewInt(value).Bytes32()
	return append(slot[:], val[:]...)
}

type pbinWitnessChain struct {
	m         *execmoduletester.ExecModuleTester
	pack      *blockgen.ChainPack
	receiver  common.Address
	small     common.Address // code fits the account header's chunks
	large     common.Address // code overflows into the code zone
	slot      common.Hash
	otherSlot common.Hash
}

// block returns the block at the given height; height 0 is genesis.
func (c *pbinWitnessChain) block(t *testing.T, num uint64) *types.Block {
	t.Helper()
	if num == 0 {
		return c.m.Genesis
	}
	require.LessOrEqual(t, num, uint64(len(c.pack.Blocks)))
	return c.pack.Blocks[num-1]
}

// buildPBinWitnessChain generates and imports a chain whose blocks each exercise
// one shape the binary witness has to carry: a plain transfer, a deploy the
// account header holds, a deploy that overflows into the code zone, a non-zero
// SSTORE, an SSTORE-to-zero, a call reading code back across the header/code-zone
// boundary, and a block with no transactions.
func buildPBinWitnessChain(t *testing.T) *pbinWitnessChain {
	t.Helper()

	m, bankKey, bankAddress := fundedBankGenesis(t, chain.TestChainBerlinConfig)
	signer := types.LatestSignerForChainID(nil)
	gasPrice := uint256.NewInt(1_000_000_000)

	c := &pbinWitnessChain{
		m:         m,
		receiver:  common.HexToAddress("0x00000000000000000000000000000000000f0f0f"),
		slot:      common.HexToHash("0x01"),
		otherSlot: common.HexToHash("0x02"),
	}

	overflowing := make([]byte, pbinHeaderCodeCapacity+31*8)
	copy(overflowing, pbinStoreRuntime)

	sign := func(txn *types.LegacyTx) types.Transaction {
		t.Helper()
		txn.GasPrice = *gasPrice
		signed, err := types.SignTx(txn, *signer, bankKey)
		require.NoError(t, err)
		return signed
	}

	pack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 7, func(i int, b *blockgen.BlockGen) {
		nonce := b.TxNonce(bankAddress)
		switch i {
		case 0: // plain transfer, creating the recipient
			b.AddTx(sign(&types.LegacyTx{CommonTx: types.CommonTx{
				Nonce: nonce, To: &c.receiver, GasLimit: 21_000, Value: *uint256.NewInt(1e9),
			}}))
		case 1: // deploy whose code the account header holds
			c.small = types.CreateAddress(bankAddress, nonce)
			b.AddTx(sign(&types.LegacyTx{CommonTx: types.CommonTx{
				Nonce: nonce, GasLimit: 200_000, Data: pbinDeployCode(pbinStoreRuntime),
			}}))
		case 2: // deploy whose code overflows into the code zone
			c.large = types.CreateAddress(bankAddress, nonce)
			b.AddTx(sign(&types.LegacyTx{CommonTx: types.CommonTx{
				Nonce: nonce, GasLimit: 4_000_000, Data: pbinDeployCode(overflowing),
			}}))
		case 3: // SSTORE a non-zero value
			b.AddTx(sign(&types.LegacyTx{CommonTx: types.CommonTx{
				Nonce: nonce, To: &c.small, GasLimit: 100_000, Data: pbinStoreCalldata(c.slot, 42),
			}}))
		case 4: // SSTORE the same slot back to zero
			b.AddTx(sign(&types.LegacyTx{CommonTx: types.CommonTx{
				Nonce: nonce, To: &c.small, GasLimit: 100_000, Data: pbinStoreCalldata(c.slot, 0),
			}}))
		case 5: // call the large contract, so its code is read back from the witness
			b.AddTx(sign(&types.LegacyTx{CommonTx: types.CommonTx{
				Nonce: nonce, To: &c.large, GasLimit: 200_000, Data: pbinStoreCalldata(c.otherSlot, 7),
			}}))
		case 6: // no transactions
		}
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(pack))
	c.pack = pack

	// An under-budgeted transaction fails silently as a reverted receipt and would
	// leave the shape it was meant to build out of the chain entirely.
	for i, receipts := range pack.Receipts {
		for _, receipt := range receipts {
			require.EqualValues(t, types.ReceiptStatusSuccessful, receipt.Status,
				"transaction in block %d failed", i+1)
		}
	}
	requirePBinChainShape(t, c)
	return c
}

// requirePBinChainShape reads back what each block was written to exercise. A
// block whose transaction ran but did something else — a deploy that no longer
// overflows the header, an SSTORE the runtime code silently skipped — would
// still produce a verifying witness, and the corpus would quietly stop covering
// the case it names.
func requirePBinChainShape(t *testing.T, c *pbinWitnessChain) {
	t.Helper()

	tx, err := c.m.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	// A history reader at block N reads the state block N starts from, so the
	// effect of block N-1 is read at N.
	stateAt := func(blockNum uint64) *state.IntraBlockState {
		reader, err := rpchelper.CreateHistoryStateReader(t.Context(), tx, blockNum, 0, rawdbv3.TxNums)
		require.NoError(t, err)
		st := state.New(reader)
		t.Cleanup(st.Close)
		return st
	}

	balance, err := stateAt(2).GetBalance(accounts.InternAddress(c.receiver))
	require.NoError(t, err)
	require.Equal(t, uint64(1e9), balance.Uint64(), "block 1 transfers to a new account")

	smallCode, err := stateAt(3).GetCode(accounts.InternAddress(c.small))
	require.NoError(t, err)
	require.Equal(t, pbinStoreRuntime, smallCode)
	require.LessOrEqual(t, len(smallCode), pbinHeaderCodeCapacity, "block 2's code fits the account header")

	largeCode, err := stateAt(4).GetCode(accounts.InternAddress(c.large))
	require.NoError(t, err)
	require.Greater(t, len(largeCode), pbinHeaderCodeCapacity, "block 3's code must reach the code zone")

	written, err := stateAt(5).GetState(accounts.InternAddress(c.small), accounts.InternKey(c.slot))
	require.NoError(t, err)
	require.Equal(t, uint64(42), written.Uint64(), "block 4 writes the slot")

	cleared, err := stateAt(6).GetState(accounts.InternAddress(c.small), accounts.InternKey(c.slot))
	require.NoError(t, err)
	require.True(t, cleared.IsZero(), "block 5 stores the slot back to zero")

	viaLargeCode, err := stateAt(7).GetState(accounts.InternAddress(c.large), accounts.InternKey(c.otherSlot))
	require.NoError(t, err)
	require.Equal(t, uint64(7), viaLargeCode.Uint64(), "block 6 runs the overflowing contract's code")
}

func pbinWitnessAPI(t *testing.T, m *execmoduletester.ExecModuleTester) *DebugAPIImpl {
	t.Helper()
	enableCommitmentHistoryFlag(t, m.DB)
	require.True(t, binCommitmentTrie(), "the chain is committed with the binary trie")
	return NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, nil, &rpccfg.DebugApiConfig{})
}

// requirePBinWitnessVerifies re-executes the block from the witness alone and
// asserts it reaches the header's post-state root. The build applies the same
// gate before returning, so this repeats it deliberately: the assertion belongs
// in the test rather than only in the code under test.
func requirePBinWitnessVerifies(t *testing.T, c *pbinWitnessChain, result *ExecutionWitnessResult, num uint64) {
	t.Helper()

	block := c.block(t, num)
	parentRoot := c.block(t, num-1).Root()
	require.NoError(t, verifyWitnessAgainstBlock(t.Context(), result, block, parentRoot,
		c.m.ChainConfig, c.m.Engine, true /* binTrie */),
		"block %d witness must re-execute to %x", num, block.Root())
}

func pbinWitnessOf(t *testing.T, api *DebugAPIImpl, num uint64) *ExecutionWitnessResult {
	t.Helper()

	bn := rpc.BlockNumber(num)
	result, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHash{BlockNumber: &bn}, nil)
	require.NoError(t, err, "block %d", num)
	require.NotNil(t, result)
	return result
}

// TestPBinExecutionWitnessEndToEnd is the end-to-end gate: over a bin-committed
// chain, every block's witness alone re-executes the block to its post-state root.
func TestPBinExecutionWitnessEndToEnd(t *testing.T) {
	// No t.Parallel: mutates process-global commitment flags.
	withCommitmentHistory(t)
	withBinCommitmentDatadir(t)

	c := buildPBinWitnessChain(t)
	api := pbinWitnessAPI(t, c.m)

	for _, tc := range []struct {
		num  uint64
		name string
	}{
		{1, "plain transfer"},
		{2, "deploy held by the account header"},
		{3, "deploy overflowing into the code zone"},
		{4, "storage write"},
		{5, "SSTORE to zero"},
		{6, "code read across the code-zone boundary"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := pbinWitnessOf(t, api, tc.num)
			require.NotEmpty(t, result.State, "a block that touches state proves it with nodes")
			require.NotEmpty(t, result.Keys)
			requirePBinWitnessVerifies(t, c, result, tc.num)
		})
	}
}

// A block carrying no transactions still pays the block reward, so it has a
// post-state root of its own to prove.
func TestPBinExecutionWitnessEmptyBlock(t *testing.T) {
	// No t.Parallel: mutates process-global commitment flags.
	withCommitmentHistory(t)
	withBinCommitmentDatadir(t)

	c := buildPBinWitnessChain(t)
	api := pbinWitnessAPI(t, c.m)

	require.Empty(t, c.block(t, 7).Transactions(), "block 7 is the no-transaction block")
	result := pbinWitnessOf(t, api, 7)
	require.NotEmpty(t, result.State)
	requirePBinWitnessVerifies(t, c, result, 7)
}

// Each witness has to stand on its own: a block's proof may not lean on nodes
// its neighbour's witness happens to carry.
func TestPBinExecutionWitnessConsecutiveBlocks(t *testing.T) {
	// No t.Parallel: mutates process-global commitment flags.
	withCommitmentHistory(t)
	withBinCommitmentDatadir(t)

	c := buildPBinWitnessChain(t)
	api := pbinWitnessAPI(t, c.m)

	first, second := pbinWitnessOf(t, api, 4), pbinWitnessOf(t, api, 5)
	requirePBinWitnessVerifies(t, c, first, 4)
	requirePBinWitnessVerifies(t, c, second, 5)
	require.NotEqual(t, first.State, second.State,
		"consecutive blocks over different pre-states cannot prove with the same node set")
}
