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

package exec

import (
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// TestBlockAssemblerMinTxGasEarlyExit pins the early-exit threshold in
// AddTransactions either side of Amsterdam, and pins it to the execution
// dimension alone. Every case uses a zero-value self-transfer so its intrinsic
// gas is exactly the fork minimum.
func TestBlockAssemblerMinTxGasEarlyExit(t *testing.T) {
	t.Parallel()

	privateKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(privateKey.PublicKey)
	senderAcc := accounts.InternAddress(sender)
	engine := merge.NewFaker(ethash.NewFaker())

	makeConfig := func(amsterdam bool) *chain.Config {
		cfg := chain.AllProtocolChanges.Copy()
		if amsterdam {
			cfg.AmsterdamTime = common.NewUint64(0)
		} else {
			cfg.AmsterdamTime = nil
		}
		return cfg
	}

	const blockGasLimit = 100_000

	tests := []struct {
		name          string
		amsterdam     bool
		gasUsed       uint64 // execution gas already consumed; remaining = blockGasLimit - gasUsed
		stateGasUsed  uint64 // state gas already consumed
		wantEarlyExit bool
		wantPacked    int
	}{
		// Pre-Amsterdam: threshold is params.TxGas (21,000).
		{"pre-Amsterdam/exec=20999/exits", false, 79_001, 0, true, 0},
		{"pre-Amsterdam/exec=21000/continues", false, 79_000, 0, false, 1},
		// Paired with post-Amsterdam/exec=15000: same gas, different fork,
		// different outcome.
		{"pre-Amsterdam/exec=15000/exits", false, 85_000, 0, true, 0},
		// The state dimension is never consumed pre-Amsterdam.
		{"pre-Amsterdam/stateExhausted/continues", false, 79_000, 88_001, false, 1},

		// Post-Amsterdam: threshold is params.TxBaseEIP2780 (12,000).
		{"post-Amsterdam/exec=11999/exits", true, 88_001, 0, true, 0},
		{"post-Amsterdam/exec=12000/continues", true, 88_000, 0, false, 1},
		{"post-Amsterdam/exec=15000/continues", true, 85_000, 0, false, 1},
		// Exec plentiful, state exhausted: the scan continues and this
		// transaction is rejected on its own state-gas contribution.
		{"post-Amsterdam/stateExhausted/noEarlyExit", true, 50_000, 88_001, false, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			chainCfg := makeConfig(tc.amsterdam)

			var txGasLimit uint64
			if tc.amsterdam {
				txGasLimit = params.TxBaseEIP2780
			} else {
				txGasLimit = params.TxGas
			}

			header := &types.Header{
				Number:   *uint256.NewInt(1),
				Time:     1000,
				GasLimit: blockGasLimit,
			}
			block := &AssembledBlock{Header: header}
			ba := NewBlockAssembler(AssemblerCfg{ChainConfig: chainCfg, Engine: engine}, block)
			ba.gasUsed.BlockExecution = tc.gasUsed
			ba.gasUsed.BlockState = tc.stateGasUsed

			ibs := state.New(state.NewNoopReader())
			require.NoError(t, ibs.AddBalance(senderAcc, *uint256.NewInt(1_000_000_000_000_000_000), tracing.BalanceChangeUnspecified))

			signer := types.MakeSigner(chainCfg, 1, 1000)
			to := sender
			tx, err := types.SignNewTx(privateKey, *signer, &types.LegacyTx{
				CommonTx: types.CommonTx{
					Nonce:    0,
					To:       &to,
					GasLimit: txGasLimit,
				},
			})
			require.NoError(t, err)

			logs, done, err := ba.AddTransactions(
				context.Background(),
				nil,
				types.Transactions{tx},
				accounts.NilAddress,
				&vm.Config{NoBaseFee: true},
				ibs,
				nil,
				"test",
				log.Root(),
			)
			require.NoError(t, err)

			if tc.wantEarlyExit {
				require.True(t, done, "expected early exit (done=true)")
				require.Empty(t, logs, "no logs on early exit")
			} else {
				require.False(t, done, "expected loop to continue past gas check")
			}
			require.Len(t, ba.Txns, tc.wantPacked)
		})
	}
}
