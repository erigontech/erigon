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

package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// Pins that HistoryReaderV3WithBlockCache (unlike WithSharedDomains) sees a
// pre-flush BlockStateCache write at block-finalize time.
func TestFinalizeReaderSeesBlockCacheWrite(t *testing.T) {
	t.Parallel()

	_, tx, domains := NewTestRwTx(t)
	domains.SetInMemHistoryReads(true)

	addr := accounts.InternAddress(common.HexToAddress("0x6be457e04092b28865e0cba84e3b2cfa0f871e67"))
	addrValue := addr.Value()

	preBlockBalance := uint256.NewInt(7290)
	preAcc := &accounts.Account{
		Nonce:       1,
		Balance:     *preBlockBalance,
		CodeHash:    accounts.EmptyCodeHash,
		Incarnation: 0,
	}
	preEnc := accounts.SerialiseV3(preAcc)

	const preBlockTxNum uint64 = 10
	const blockStartTxNum uint64 = 20
	const tx28TxNum uint64 = 28
	const finalTxNum uint64 = 30

	domains.SetTxNum(preBlockTxNum)
	require.NoError(t,
		domains.DomainPut(kv.AccountsDomain, tx, addrValue[:], preEnc, preBlockTxNum, nil),
	)

	postTx28Balance := uint256.NewInt(6707)
	postAcc := &accounts.Account{
		Nonce:       1,
		Balance:     *postTx28Balance,
		CodeHash:    accounts.EmptyCodeHash,
		Incarnation: 0,
	}
	postEnc := accounts.SerialiseV3(postAcc)

	blockCache := NewBlockStateCache()
	blockCache.PutCommittedAccount(addr, preAcc)
	blockCache.WriteAccount(addr, postEnc, 100)

	curReader := NewCurrentCachedReaderV3(domains.AsGetter(tx), blockCache)
	curAcc, err := curReader.ReadAccountData(addr)
	require.NoError(t, err)
	require.NotNil(t, curAcc, "current-cached reader should see the blockCache write")
	require.Equal(t, *postTx28Balance, curAcc.Balance,
		"CurrentCachedReaderV3 must return the post-tx28 balance from blockCache")

	_ = tx28TxNum
	_ = blockStartTxNum
	histReader := NewHistoryReaderV3WithBlockCache(tx, domains, blockCache, finalTxNum)
	histAcc, err := histReader.ReadAccountData(addr)
	require.NoError(t, err)
	require.NotNil(t, histAcc, "finalize-mode historic reader must not return nil for a funded address")
	require.Equal(t, *postTx28Balance, histAcc.Balance,
		"HistoryReaderV3WithBlockCache at block-finalize txNum must see tx 28's BlockStateCache write; "+
			"otherwise withdrawal processing reads the stale pre-block balance and stomps tx 28's write, "+
			"which is the root cause of the trie-root race at block 24839300")

	sdOnly := NewHistoryReaderV3WithSharedDomains(tx, domains, finalTxNum)
	sdAcc, err := sdOnly.ReadAccountData(addr)
	require.NoError(t, err)
	require.NotNil(t, sdAcc)
	require.Equal(t, *preBlockBalance, sdAcc.Balance,
		"NewHistoryReaderV3WithSharedDomains is expected to NOT see the blockCache write; "+
			"it reads only from sd.GetAsOf → ttx.GetAsOf and therefore returns the stale pre-block balance")
}
