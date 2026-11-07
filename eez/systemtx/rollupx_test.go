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
package systemtx_test

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/systemtx"
	"github.com/erigontech/erigon/execution/chain"
	execprotocol "github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var rollupXEEZL2 = accounts.InternAddress(protocol.RollupXEEZL2Address)

func rollupXChainConfig() *chain.Config {
	cfg := eezChainConfig()
	l2 := *cfg.L2.(*config.L2Config)
	l2.L1ChainID, l2.BlockInterval, l2.L1BlockTime = 10200, 1, 5
	l2.RollupX = &config.RollupXConfig{
		BlockIntervalSeconds: 1,
		SyncIntervalBlocks:   5,
		L1Reference:          config.L1Reference{ChainID: 10200, Hash: common.HexToHash("0x01")},
	}
	cfg.L2 = &l2
	denominator := config.RollupXBaseFeeChangeDenominator
	cfg.BaseFeeChangeDenominator = &denominator
	return cfg
}

func rollupXSystemTx(t *testing.T, chainID uint64) *systemtx.Transaction {
	return mustNew(t, protocol.SystemTx{ChainID: chainID, To: protocol.RollupXEEZL2Address})
}

func TestRollupXAsMessage(t *testing.T) {
	signer := *types.LatestSignerForChainID(uint256.NewInt(1))
	rules := &chain.Rules{ChainID: uint256.NewInt(1), IsLondon: true, L2Version: config.RollupX}

	msg, err := rollupXSystemTx(t, 1).AsMessage(signer, nil, rules)
	require.NoError(t, err)
	require.Equal(t, accounts.InternAddress(protocol.RollupXEEZL2Address), msg.To())
	require.Equal(t, protocol.RollupXSystemTxGasCap, msg.Gas())
	require.True(t, msg.GasFromBlock())

	msg, err = mustNew(t, protocol.SystemTx{ChainID: 1, To: protocol.EEZL2Address}).AsMessage(signer, nil, eezRules)
	require.NoError(t, err)
	require.False(t, msg.GasFromBlock())

	// Each rule set takes only its own EEZL2.
	_, err = mustNew(t, protocol.SystemTx{ChainID: 1, To: protocol.EEZL2Address}).AsMessage(signer, nil, rules)
	require.ErrorIs(t, err, protocol.ErrSystemTx)
	_, err = rollupXSystemTx(t, 1).AsMessage(signer, nil, eezRules)
	require.ErrorIs(t, err, protocol.ErrSystemTx)
}

// TestRollupXGas checks that a rollupX system transaction executes with what
// its block has left, at most the EIP-7825 cap.
func TestRollupXGas(t *testing.T) {
	for _, tc := range []struct {
		name      string
		blockLeft uint64
		used      uint64
		err       error
	}{
		{name: "capped", blockLeft: 30_000_000, used: protocol.RollupXSystemTxGasCap},
		{name: "remaining", blockLeft: 1_000_000, used: 1_000_000},
		{name: "below intrinsic gas", blockLeft: 20_000, err: execprotocol.ErrIntrinsicGas},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newExecEnvFor(t, rollupXChainConfig(), rollupXEEZL2, codeLoop, 0)
			res, err := env.apply(t, rollupXSystemTx(t, env.cfg.ChainID.Uint64()), tc.blockLeft)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			require.True(t, res.Failed())
			require.Equal(t, tc.used, res.ReceiptGasUsed)
		})
	}
}
