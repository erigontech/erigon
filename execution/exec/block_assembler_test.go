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
	"github.com/jinzhu/copier"
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

// TestBlockAssemblerMinTxGasEarlyExit exercises the early-exit condition in
// AddTransactions:
//
//	gasPool.Gas() < minTxGas || (isAmsterdam && gasPool.StateGasAvailable() < minTxGas)
//
// Pre-Amsterdam the assembler stops when remaining execution gas falls below
// params.TxGas (21,000). Post-Amsterdam EIP-2780 lowers the minimum intrinsic
// cost to params.TxBaseEIP2780 (12,000) for self-transfers, and EIP-8037 adds
// a state-gas dimension: the inclusion check reserves the full tx.gas_limit
// in the state pool, so the assembler also exits when state gas is exhausted.
//
// Each subtest uses a zero-value self-transfer (to == sender, value == 0) so
// the intrinsic gas is exactly the fork-specific minimum:
//   - pre-Amsterdam:  params.TxGas         = 21,000
//   - post-Amsterdam: params.TxBaseEIP2780 = 12,000
func TestBlockAssemblerMinTxGasEarlyExit(t *testing.T) {
	t.Parallel()

	privateKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(privateKey.PublicKey)
	senderAcc := accounts.InternAddress(sender)
	engine := merge.NewFaker(ethash.NewFaker())

	makeConfig := func(amsterdam bool) *chain.Config {
		cfg := new(chain.Config)
		require.NoError(t, copier.CopyWithOption(cfg, chain.AllProtocolChanges, copier.Option{DeepCopy: true}))
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
		stateGasUsed  uint64 // state gas already consumed (post-Amsterdam only)
		wantEarlyExit bool
	}{
		// ── Pre-Amsterdam: threshold is params.TxGas (21,000) ──
		// Part B (state check) is always false due to the isAmsterdam guard.

		// Off-by-one below threshold: 20,999 < 21,000 → exit.
		{"pre-Amsterdam/exec=20999/exits", false, 79_001, 0, true},
		// Exact threshold: 21,000 is NOT < 21,000 → continue.
		{"pre-Amsterdam/exec=21000/continues", false, 79_000, 0, false},
		// Between the two thresholds: 15,000 < 21,000 → exit.
		// Paired with post-Amsterdam/exec=15000, this forms the core
		// regression pair: same gas, different fork, different outcome.
		{"pre-Amsterdam/exec=15000/exits", false, 85_000, 0, true},
		// State gas exhausted but isAmsterdam guard prevents early exit.
		// Verifies the guard: pre-Amsterdam ignores state gas entirely.
		{"pre-Amsterdam/stateExhausted/continues", false, 79_000, 88_001, false},

		// ── Post-Amsterdam: threshold is params.TxBaseEIP2780 (12,000) ──
		// Both Part A (execution check) and Part B (state check) can trigger.

		// Off-by-one below threshold: 11,999 < 12,000 → exit.
		{"post-Amsterdam/exec=11999/exits", true, 88_001, 0, true},
		// Exact threshold: 12,000 is NOT < 12,000 → continue.
		{"post-Amsterdam/exec=12000/continues", true, 88_000, 0, false},
		// Between the two thresholds: 15,000 >= 12,000 → continue.
		// Paired with pre-Amsterdam/exec=15000, this forms the core
		// regression pair: same gas, different fork, different outcome.
		{"post-Amsterdam/exec=15000/continues", true, 85_000, 0, false},
		// State gas below threshold (execution gas plentiful).
		// InclusionContributions reserves the full tx.gas_limit in the state
		// pool, so no transaction can pass the inclusion check.
		{"post-Amsterdam/state=11999/exits", true, 50_000, 88_001, true},
		// State gas at exact boundary: 12,000 is NOT < 12,000 → continue.
		{"post-Amsterdam/state=12000/continues", true, 50_000, 88_000, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			chainCfg := makeConfig(tc.amsterdam)

			// For "continues" cases the tx gas limit must fit both the
			// remaining pool and the fork-specific intrinsic cost. Use
			// the fork-appropriate intrinsic minimum (self-transfer).
			// For "exits" cases the tx is never reached.
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
				require.Nil(t, logs, "no logs on early exit")
				require.Empty(t, ba.Txns, "no txns packed on early exit")
			} else {
				require.False(t, done, "expected loop to continue past gas check")
				require.Len(t, ba.Txns, 1, "self-transfer should be packed")
			}
		})
	}
}
