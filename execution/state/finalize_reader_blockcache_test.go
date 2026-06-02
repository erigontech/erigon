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

// TestFinalizeReaderSeesBlockCacheWrite reproduces the root-cause pattern
// of the trie-root race at block 24839300.
//
// In the parallel executor, applyVersionedWrites (rw_v3.go) writes per-tx
// account updates into the per-block BlockStateCache only — nothing lands
// in sd.mem until the whole block's BlockStateCache is Flushed at the end.
//
// At block-finalize time (engine.Finalize, withdrawals, EIP-7002/7251
// system calls) an IBS is constructed. When the block is classified
// historic (tip-adjacent, past the last frozen tx num boundary), the
// reader used is HistoryReaderV3WithSharedDomains, which chains
// sd.GetAsOf → ttx.GetAsOf. Neither tier consults the BlockStateCache.
//
// As a result, withdrawal processing reads the *pre-block* balance for
// any address that received an intra-block SubBalance or AddBalance, and
// then its own balance update overwrites the in-block write at a higher
// TxIndex during the next flush. The effect observed in the wild:
// block 24839223 tx 28 subtracts 0.583 ETH from 0x6be457e0...; the
// block-finalize IBS reads the stale pre-block value, the withdrawal
// write stomps tx 28's write, and the corruption propagates 77 blocks
// forward to tx 29 of block 24839300 (+1506 gas diff).
//
// This test seeds domains with a pre-block account, mutates the account
// via BlockStateCache (standing in for tx 28's applyVersionedWrites),
// and checks both reader variants. CurrentCachedReaderV3 returns the
// post-tx value (passes). HistoryReaderV3WithSharedDomains misses the
// blockCache entirely and returns the stale pre-block value — this is
// the bug the test pins down.
func TestFinalizeReaderSeesBlockCacheWrite(t *testing.T) {
	t.Parallel()

	_, tx, domains := NewTestRwTx(t)
	domains.SetInMemHistoryReads(true)

	addr := accounts.InternAddress(common.HexToAddress("0x6be457e04092b28865e0cba84e3b2cfa0f871e67"))
	addrValue := addr.Value()

	// Pre-block committed balance: the value sd.mem / ttx would return.
	preBlockBalance := uint256.NewInt(7290)
	preAcc := &accounts.Account{
		Nonce:    1,
		Balance:  *preBlockBalance,
		CodeHash: accounts.EmptyCodeHash,
	}
	preEnc := accounts.SerialiseV3(preAcc)

	const preBlockTxNum uint64 = 10
	const blockStartTxNum uint64 = 20
	const tx28TxNum uint64 = 28
	const finalTxNum uint64 = 30 // block-finalize / withdrawal txNum

	domains.SetTxNum(preBlockTxNum)
	require.NoError(t,
		domains.DomainPut(kv.AccountsDomain, tx, addrValue[:], preEnc, preBlockTxNum, nil),
	)

	// Simulate tx 28's SubBalance landing in the BlockStateCache (and only
	// the BlockStateCache — applyVersionedWrites never touches sd.mem in the
	// parallel path).
	postTx28Balance := uint256.NewInt(6707)
	postAcc := &accounts.Account{
		Nonce:    1,
		Balance:  *postTx28Balance,
		CodeHash: accounts.EmptyCodeHash,
	}
	postEnc := accounts.SerialiseV3(postAcc)

	blockCache := NewBlockStateCache()
	blockCache.PutCommittedAccount(addr, preAcc)
	blockCache.WriteAccount(addr, postEnc, 100)

	// Sanity: CurrentCachedReaderV3 (the reader used for non-historic
	// blocks) sees the post-tx28 value.
	curReader := NewCurrentCachedReaderV3(domains.AsGetter(tx), blockCache)
	curAcc, err := curReader.ReadAccountData(addr)
	require.NoError(t, err)
	require.NotNil(t, curAcc, "current-cached reader should see the blockCache write")
	require.Equal(t, *postTx28Balance, curAcc.Balance,
		"CurrentCachedReaderV3 must return the post-tx28 balance from blockCache")

	// The actual bug: HistoryReaderV3WithSharedDomains (the reader used
	// for tip-adjacent historic blocks at finalize time) ignores the
	// BlockStateCache entirely and reads from sd.mem → ttx.
	// The blockCache-aware variant must see the post-tx28 balance,
	// otherwise withdrawal processing reads a stale value and overwrites
	// tx 28's write.
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

	// Sanity: the sd-only variant still misses the blockCache write. This
	// pins down why callers that construct a finalize IBS must use the
	// blockCache-aware constructor above — swapping it back would
	// reintroduce the 24839300 bug.
	sdOnly := NewHistoryReaderV3WithSharedDomains(tx, domains, finalTxNum)
	sdAcc, err := sdOnly.ReadAccountData(addr)
	require.NoError(t, err)
	require.NotNil(t, sdAcc)
	require.Equal(t, *preBlockBalance, sdAcc.Balance,
		"NewHistoryReaderV3WithSharedDomains is expected to NOT see the blockCache write; "+
			"it reads only from sd.GetAsOf → ttx.GetAsOf and therefore returns the stale pre-block balance")
}
