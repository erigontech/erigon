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

package stagedsync_test

import (
	"slices"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
)

func TestSendersRecoveryErrorDoesNotDeadlock(t *testing.T) {
	m := execmoduletester.New(t)
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	to := crypto.PubkeyToAddress(key.PublicKey)
	signer := types.MakeSigner(chain.TestChainBerlinConfig, 1, 0)
	const total = 2500
	txs := make([]types.Transaction, total)
	for i := range txs {
		tx, err := types.SignTx(&types.LegacyTx{CommonTx: types.CommonTx{Nonce: uint64(i), To: &to, Value: u256.Num1, GasLimit: 21000}, GasPrice: u256.Num1}, *signer, key)
		require.NoError(t, err)
		txs[i] = tx
	}
	bad := &types.LegacyTx{CommonTx: types.CommonTx{Nonce: 1 << 40, To: &to, Value: u256.Num1, GasLimit: 21000}, GasPrice: u256.Num1}
	bad.V.SetUint64(27)
	bad.R.Set(uint256.NewInt(0))
	bad.S.Set(uint256.NewInt(1))
	for it := range 60 {
		body := slices.Clone(txs)
		body[total-1000-30+it] = bad
		recoverSendersOfBlock(t, m, body, byte(it))
	}
}

func recoverSendersOfBlock(t *testing.T, m *execmoduletester.ExecModuleTester, body []types.Transaction, salt byte) {
	tx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	header := &types.Header{Number: *common.Num1, Extra: []byte{salt}}
	hash := header.Hash()
	require.NoError(t, rawdb.WriteHeader(tx, header))
	require.NoError(t, rawdb.WriteBody(tx, hash, 1, &types.Body{Transactions: body}))
	require.NoError(t, rawdb.WriteCanonicalHash(tx, hash, 1))
	require.NoError(t, stages.SaveStageProgress(tx, stages.Bodies, 1))
	cfg := stagedsync.StageSendersCfg(chain.TestChainBerlinConfig, true, t.TempDir(), m.BlockReader, exec.NewBlockReadAheader())
	done := make(chan error, 1)
	go func() {
		done <- stagedsync.SpawnRecoverSendersStage(cfg, &stagedsync.StageState{ID: stages.Senders}, nil, tx, 1, m.Ctx, log.New())
	}()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("senders stage hung after a sender recovery error")
	}
}
