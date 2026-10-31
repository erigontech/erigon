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

package executiontests

import (
	"crypto/ecdsa"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
)

var burntRecipient = common.HexToAddress("0xb0b0")

// burntFeeConfig sends the base fee to burntRecipient, as Gnosis does.
func burntFeeConfig(base *chain.Config) *chain.Config {
	config := base.Copy()
	config.BurntContract = map[string]common.Address{"0": burntRecipient}
	return config
}

func londonConfig() *chain.Config {
	config := chain.TestChainBerlinConfig.Copy()
	config.LondonBlock = common.NewUint64(0)
	return config
}

func pragueConfig() *chain.Config {
	config := chain.TestChainOsakaConfig.Copy()
	config.OsakaTime = nil
	return config
}

func amsterdamConfig() *chain.Config {
	config := chain.TestChainOsakaConfig.Copy()
	config.AmsterdamTime = common.NewUint64(0)
	return config
}

// importRepeats is how often a racy parallel import is repeated: once in short mode, where
// TestValidatedFeeRecipientsStayEstimates covers the fix, and often enough to catch a regression
// otherwise.
func importRepeats() int {
	if testing.Short() {
		return 1
	}
	return 10
}

// baseFeePaid is the base fee paid by the tx at index i of the block.
func baseFeePaid(chainPack *blockgen.ChainPack, i int) uint256.Int {
	return *uint256.NewInt(chainPack.Receipts[0][i].GasUsed * chainPack.TopBlock.BaseFee().Uint64())
}

// importSerially imports the block with serial execution and checks the resulting state.
func importSerially(t *testing.T, gspec *types.Genesis, key *ecdsa.PrivateKey, chainPack *blockgen.ChainPack, check func(t *testing.T, st *state.IntraBlockState)) {
	t.Helper()
	prevParallel := dbg.Exec3Parallel
	t.Cleanup(func() { dbg.Exec3Parallel = prevParallel })
	t.Run("serial", func(t *testing.T) {
		dbg.Exec3Parallel = false
		m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(key))
		require.NoError(t, m.InsertChain(chainPack))
		require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
			st := state.New(m.NewStateReader(tx))
			defer st.Close()
			check(t, st)
			return nil
		}))
	})
}

// An empty burnt-fee recipient is touched by a zero-value CALL in a tx that pays
// it a base fee. The fee is credited before the end-of-tx EIP-161 sweep, so the
// account survives with its storage.
func TestBurntFeeKeepsTouchedEmptyRecipient(t *testing.T) {
	toucher := common.HexToAddress("0x7007")
	for _, tc := range []struct {
		name    string
		config  *chain.Config
		storage map[common.Hash]common.Hash
	}{
		{"london", londonConfig(), map[common.Hash]common.Hash{common.BytesToHash([]byte{1}): common.BytesToHash([]byte{1})}},
		{"prague", pragueConfig(), map[common.Hash]common.Hash{common.BytesToHash([]byte{1}): common.BytesToHash([]byte{1})}},
		{"prague without storage", pragueConfig(), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			alloc := types.GenesisAlloc{
				toucher:        {Code: callCode(burntRecipient, 0)},
				burntRecipient: {Balance: big.NewInt(0), Storage: tc.storage},
			}
			keys := revivalKeys(t, alloc, 1)
			gspec := &types.Genesis{Config: burntFeeConfig(tc.config), GasLimit: 30_000_000, Alloc: alloc}
			chainPack := buildBlockSerially(t, gspec, keys, common.Address{1}, 10_000_000_000, []revivalCall{{to: toucher}})
			check := func(t *testing.T, st *state.IntraBlockState) {
				balance, err := st.GetBalance(accounts.InternAddress(burntRecipient))
				require.NoError(t, err)
				require.Equal(t, baseFeePaid(chainPack, 0), balance)
				if tc.storage != nil {
					require.Equal(t, *uint256.NewInt(1), storageAt(t, st, burntRecipient, 1))
				}
			}
			importSerially(t, gspec, keys[0], chainPack, check)
			importInParallel(t, gspec, keys[0], chainPack, check)
		})
	}
}

// The burnt-fee recipient is destroyed by the tx that pays it a base fee: a
// pre-Cancun SELFDESTRUCT of an existing contract, or a same-tx CREATE2 and
// SELFDESTRUCT under EIP-6780. The fee is credited to the doomed account and
// removed with it, like the tip of a destroyed coinbase. The next tx's fee
// creates the recipient again, without code, nonce or storage.
func TestBurntFeeBurnedWithDestroyedRecipient(t *testing.T) {
	destructRuntime := program.New().Op(vm.CALLER, vm.SELFDESTRUCT).Bytes()
	destructInit := program.New().Sstore(1, 1).Op(vm.CALLER, vm.SELFDESTRUCT).Bytes()
	factory := common.HexToAddress("0xfac7")
	created := types.CreateAddress2(factory, [32]byte{}, accounts.InternCodeHash(crypto.Keccak256Hash(destructInit)))
	for _, tc := range []struct {
		name      string
		config    *chain.Config
		recipient common.Address
		alloc     types.GenesisAlloc
		destroy   common.Address
	}{
		{
			name:      "existing contract before Cancun",
			config:    londonConfig(),
			recipient: burntRecipient,
			alloc: types.GenesisAlloc{
				burntRecipient: {Code: destructRuntime, Nonce: 1, Balance: big.NewInt(3), Storage: map[common.Hash]common.Hash{common.BytesToHash([]byte{1}): common.BytesToHash([]byte{1})}},
			},
			destroy: burntRecipient,
		},
		{
			name:      "created in the same tx",
			config:    pragueConfig(),
			recipient: created,
			alloc:     types.GenesisAlloc{factory: {Code: program.New().Create2(destructInit, 0).Op(vm.POP, vm.STOP).Bytes()}},
			destroy:   factory,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := tc.config.Copy()
			config.BurntContract = map[string]common.Address{"0": tc.recipient}
			keys := revivalKeys(t, tc.alloc, 2)
			gspec := &types.Genesis{Config: config, GasLimit: 30_000_000, Alloc: tc.alloc}
			chainPack := buildBlockSerially(t, gspec, keys, common.Address{1}, 10_000_000_000, []revivalCall{
				{to: tc.destroy},
				{to: common.HexToAddress("0xbeef")},
			})
			check := func(t *testing.T, st *state.IntraBlockState) {
				recipient := accounts.InternAddress(tc.recipient)
				balance, err := st.GetBalance(recipient)
				require.NoError(t, err)
				require.Equal(t, baseFeePaid(chainPack, 1), balance, "only the second tx's fee survives")
				nonce, err := st.GetNonce(recipient)
				require.NoError(t, err)
				require.Zero(t, nonce)
				codeHash, err := st.GetCodeHash(recipient)
				require.NoError(t, err)
				require.Equal(t, accounts.EmptyCodeHash, codeHash)
				require.Equal(t, uint256.Int{}, storageAt(t, st, tc.recipient, 1))
			}
			importSerially(t, gspec, keys[0], chainPack, check)
			importInParallel(t, gspec, keys[0], chainPack, check)
		})
	}
}

// With a zero base fee the fee transfer is a zero-value touch of the burnt-fee
// recipient, so EIP-161 removes an empty one with its storage.
func TestZeroBaseFeeRemovesEmptyBurntRecipient(t *testing.T) {
	alloc := types.GenesisAlloc{
		burntRecipient: {Balance: big.NewInt(0), Storage: map[common.Hash]common.Hash{common.BytesToHash([]byte{1}): common.BytesToHash([]byte{1})}},
	}
	keys := revivalKeys(t, alloc, 1)
	gspec := &types.Genesis{Config: burntFeeConfig(pragueConfig()), GasLimit: 30_000_000, Alloc: alloc, BaseFee: uint256.NewInt(0)}
	chainPack := buildBlockSerially(t, gspec, keys, common.Address{1}, 1, []revivalCall{{to: common.HexToAddress("0xbeef")}})
	require.True(t, chainPack.TopBlock.BaseFee().IsZero())
	check := func(t *testing.T, st *state.IntraBlockState) {
		exists, err := st.Exist(accounts.InternAddress(burntRecipient))
		require.NoError(t, err)
		require.False(t, exists)
		require.Equal(t, uint256.Int{}, storageAt(t, st, burntRecipient, 1))
	}
	importSerially(t, gspec, keys[0], chainPack, check)
	importInParallel(t, gspec, keys[0], chainPack, check)
}

// Tx 0 touches an empty fee recipient, the burnt-fee recipient or the coinbase, with a zero-value
// call, so EIP-161 removes it and its fee revives it with its storage. Every later tx stores the
// recipient's balance, which must include the fees of all earlier txs.
func TestFeeRevivingTouchedRecipientIsVisibleToLaterReads(t *testing.T) {
	const txCount = 32
	const gasPrice = 10_000_000_000
	toucher := common.HexToAddress("0x7007")
	observer := common.HexToAddress("0x0b5e")
	for _, tc := range []struct {
		name     string
		config   *chain.Config
		coinbase bool
	}{
		{"burnt/london", londonConfig(), false},
		{"burnt/prague", pragueConfig(), false},
		{"burnt/amsterdam", amsterdamConfig(), false},
		{"coinbase/london", londonConfig(), true},
		{"coinbase/prague", pragueConfig(), true},
		{"coinbase/amsterdam", amsterdamConfig(), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recipient, coinbase := burntRecipient, common.Address{1}
			if tc.coinbase {
				recipient = common.HexToAddress("0xc0c0")
				coinbase = recipient
			}
			alloc := types.GenesisAlloc{
				toucher:   {Code: callCode(recipient, 0)},
				observer:  {Code: program.New().Push(recipient).Op(vm.BALANCE, vm.CALLER, vm.SSTORE).Bytes()},
				recipient: {Balance: big.NewInt(0), Storage: map[common.Hash]common.Hash{common.BytesToHash([]byte{1}): common.BytesToHash([]byte{1})}},
			}
			keys := revivalKeys(t, alloc, txCount)
			calls := make([]revivalCall, txCount)
			calls[0] = revivalCall{to: toucher}
			for i := 1; i < txCount; i++ {
				calls[i] = revivalCall{to: observer}
			}
			gspec := &types.Genesis{Config: burntFeeConfig(tc.config), GasLimit: txCount * 1_000_000, Alloc: alloc}
			chainPack := buildBlockSerially(t, gspec, keys, coinbase, gasPrice, calls)
			feePaid := func(i int) uint256.Int {
				if !tc.coinbase {
					return baseFeePaid(chainPack, i)
				}
				tip := uint256.NewInt(gasPrice)
				tip.Sub(tip, chainPack.TopBlock.BaseFee())
				return *tip.Mul(tip, uint256.NewInt(chainPack.Receipts[0][i].GasUsed))
			}
			check := func(t *testing.T, st *state.IntraBlockState) {
				var balance uint256.Int
				for i, call := range calls {
					if call.to == observer {
						sender := crypto.PubkeyToAddress(keys[i].PublicKey)
						stored, err := st.GetState(accounts.InternAddress(observer), accounts.InternKey(common.BytesToHash(sender[:])))
						require.NoError(t, err)
						require.Equal(t, balance, stored, "the balance read by tx %d", i)
					}
					fee := feePaid(i)
					balance.Add(&balance, &fee)
				}
				// Before the merge, the coinbase also gets the block reward.
				if !tc.coinbase || tc.config.TerminalTotalDifficulty != nil {
					final, err := st.GetBalance(accounts.InternAddress(recipient))
					require.NoError(t, err)
					require.Equal(t, balance, final)
				}
				require.Equal(t, *uint256.NewInt(1), storageAt(t, st, recipient, 1))
			}
			importSerially(t, gspec, keys[0], chainPack, check)
			for range importRepeats() {
				importInParallel(t, gspec, keys[0], chainPack, check)
			}
		})
	}
}

// Txs alternate between sending 1 wei to the burnt-fee recipient and storing its
// balance, so every stored balance includes the base fees of all earlier txs.
// A balance read can race with the crediting of an earlier tx's base fee, so the
// parallel import is repeated.
func TestBurntFeeIsVisibleToLaterBalanceReads(t *testing.T) {
	const txCount = 64
	const initialBalance = 1_000_000_000
	observer := common.HexToAddress("0x0b5e")
	for _, tc := range []struct {
		name   string
		config *chain.Config
	}{
		{"london", londonConfig()},
		{"prague", pragueConfig()},
		{"amsterdam", amsterdamConfig()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			alloc := types.GenesisAlloc{
				observer:       {Code: program.New().Push(burntRecipient).Op(vm.BALANCE, vm.CALLER, vm.SSTORE).Bytes()},
				burntRecipient: {Balance: big.NewInt(initialBalance)},
			}
			keys := revivalKeys(t, alloc, txCount)
			calls := make([]revivalCall, txCount)
			for i := range calls {
				if i%2 == 0 {
					calls[i] = revivalCall{to: burntRecipient, value: 1}
				} else {
					calls[i] = revivalCall{to: observer}
				}
			}
			gspec := &types.Genesis{Config: burntFeeConfig(tc.config), GasLimit: txCount * 1_000_000, Alloc: alloc}
			chainPack := buildBlockSerially(t, gspec, keys, common.Address{1}, 10_000_000_000, calls)
			check := func(t *testing.T, st *state.IntraBlockState) {
				balance := *uint256.NewInt(initialBalance)
				for i, call := range calls {
					if call.to == observer {
						sender := crypto.PubkeyToAddress(keys[i].PublicKey)
						stored, err := st.GetState(accounts.InternAddress(observer), accounts.InternKey(common.BytesToHash(sender[:])))
						require.NoError(t, err)
						require.Equal(t, balance, stored, "the balance read by tx %d", i)
					}
					fee := baseFeePaid(chainPack, i)
					balance.Add(&balance, &fee)
					balance.AddUint64(&balance, call.value)
				}
				final, err := st.GetBalance(accounts.InternAddress(burntRecipient))
				require.NoError(t, err)
				require.Equal(t, balance, final)
			}
			importSerially(t, gspec, keys[0], chainPack, check)
			for range importRepeats() {
				importInParallel(t, gspec, keys[0], chainPack, check)
			}
		})
	}
}
