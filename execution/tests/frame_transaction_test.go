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
	"bytes"
	"fmt"
	"math/big"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/privateapi"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func TestFrameTransactionInsertChain(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		t.Run(fmt.Sprint(parallel), func(t *testing.T) {
			previous := dbg.Exec3Parallel
			dbg.Exec3Parallel = parallel
			t.Cleanup(func() { dbg.Exec3Parallel = previous })
			cfg := chain.TestChainOsakaConfig.Copy()
			cfg.AmsterdamTime, cfg.BogotaTime = common.NewUint64(0), common.NewUint64(0)
			sender, target := common.HexToAddress("0x1000"), common.HexToAddress("0x2000")
			genesis := &types.Genesis{Config: cfg, GasLimit: 30_000_000, Alloc: types.GenesisAlloc{
				sender: {Balance: big.NewInt(1_000_000_000_000_000_000), Code: []byte{0x60, 3, 0x5f, 0x5f, 0xaa}},
				target: {Code: []byte{0x60, 1, 0x5f, 0x55, 0x5f, 0x5f, 0xa0}},
			}}
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis))
			pack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) {
				b.SetCoinbase(common.Address{1})
				for nonce := range uint64(2) {
					b.AddTx(&types.FrameTransaction{
						NonceKeys: []uint256.Int{{}},
						ChainID:   *cfg.ChainID, SenderAddress: sender, Nonce: nonce,
						Frames: []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}}, {Mode: types.FrameSender, Target: &target, Limits: mdgas.MdGas{Execution: 200_000, State: genesis.GasLimit - params.StateGasPerStorageSet}}},
						Fees:   types.FrameFees{FeeCap: *uint256.NewInt(10_000_000_000), TipCap: *uint256.NewInt(1_000_000_000)},
					})
				}
			})
			require.NoError(t, err)
			require.Len(t, pack.Receipts[0], 2)
			for _, receipt := range pack.Receipts[0] {
				require.Equal(t, &sender, receipt.Payer)
				require.Len(t, receipt.FrameReceipts, 2)
				require.Equal(t, types.ReceiptStatusSuccessful, receipt.FrameReceipts[1].Status)
				require.Len(t, receipt.Logs, 1)
				require.Len(t, receipt.FrameReceipts[1].Logs, 1)
				require.Equal(t, receipt.Logs[0], receipt.FrameReceipts[1].Logs[0])
			}
			require.NoError(t, m.InsertChain(pack))
			require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
				ibs := state.New(m.NewStateReader(tx))
				defer ibs.Close()
				nonce, err := ibs.GetNonce(accounts.InternAddress(sender))
				require.NoError(t, err)
				require.Equal(t, uint64(2), nonce)
				value, err := ibs.GetState(accounts.InternAddress(target), accounts.ZeroKey)
				require.NoError(t, err)
				require.Equal(t, uint256.NewInt(1), &value)
				return nil
			}))
		})
	}
}

func TestFrameValidationUsesHeadWithdrawals(t *testing.T) {
	cfg := chain.TestChainOsakaConfig.Copy()
	cfg.AmsterdamTime, cfg.BogotaTime = common.NewUint64(0), common.NewUint64(0)
	sender := common.HexToAddress("0x1000")
	genesis := &types.Genesis{Config: cfg, GasLimit: 30_000_000, Alloc: types.GenesisAlloc{
		sender: {Code: []byte{0x60, 3, 0x5f, 0x5f, 0xaa}},
	}}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis))
	pack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) {
		b.AddWithdrawal(&types.Withdrawal{Address: sender, Amount: 1_000_000_000})
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(pack))
	tx := &types.FrameTransaction{
		NonceKeys: []uint256.Int{{}},
		ChainID:   *cfg.ChainID, SenderAddress: sender,
		Frames: []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}}},
		Fees:   types.FrameFees{FeeCap: *uint256.NewInt(10_000_000_000)},
	}
	var encoded bytes.Buffer
	require.NoError(t, tx.MarshalBinary(&encoded))
	server := privateapi.NewEthBackendServer(t.Context(), nil, m.DB, m.Notifications, m.BlockReader, log.New(), nil, cfg)
	result, err := server.FrameValidation(t.Context(), &remoteproto.FrameValidationRequest{Transaction: encoded.Bytes()})
	require.NoError(t, err)
	require.True(t, result.Valid)
	require.Equal(t, uint256.NewInt(1_000_000_000_000_000_000).Bytes(), result.PayerBalance)
}
