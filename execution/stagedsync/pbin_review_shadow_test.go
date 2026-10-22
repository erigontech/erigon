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

package stagedsync

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestPBinReviewDualShadowWithBinSelected(t *testing.T) {
	db, tx, _ := dualCalculatorTest(t)
	doms, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithCommitmentDomain(kv.CommitmentBinDomain), execctx.WithParaTrieDB(db))
	require.NoError(t, err)
	defer doms.Close()
	doms.SetInMemHistoryReads(true)
	doms.SetDisableInlineTouchKey(true)
	setDeferCommitmentUpdates(doms, true, true, true)
	roTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()
	activation := uint64(10)
	cc := &commitmentCalculator{doms: doms, db: db, roTx: roTx, logger: log.New(), chainConfig: &chain.Config{BinaryTrieTime: &activation}}
	key := bytes.Repeat([]byte{0x11}, 20)
	account := accounts.Account{Nonce: 1}
	value := accounts.SerialiseV3(&account)
	require.NoError(t, doms.DomainPut(kv.AccountsDomain, tx, key, value, 1, nil))
	updates := commitment.NewUpdates(commitment.ModeUpdate, t.TempDir(), commitment.KeyToHexNibbleHash)
	defer updates.Close()
	updates.TouchPlainKey(string(key), value, updates.TouchAccount)
	result, err := cc.computeDualFromUpdatesWithRole(t.Context(), commitTarget{blockNum: 1, blockHash: common.Hash{1}, lastTxNum: 1, blockTime: 1}, updates,
		&asOfStateReader{sd: doms, roTx: roTx, commitmentDomain: kv.CommitmentDomain}, doms.GetCommitmentCtxForDomain(kv.CommitmentDomain), doms.GetCommitmentCtxForDomain(kv.CommitmentBinDomain),
		dualHexFeed(key, commitment.Update{Flags: commitment.BalanceUpdate | commitment.NonceUpdate | commitment.CodeUpdate, Nonce: 1, CodeHash: empty.CodeHash}), nil)
	require.NoError(t, err)
	require.NotEmpty(t, result.shadowRoot)
	root, _, err := doms.GetLatest(kv.CommitmentBinDomain, tx, pbt.GlobalRootKey())
	require.NoError(t, err)
	require.NotEmpty(t, root, "accepted shadow root has no applied root record")
}
