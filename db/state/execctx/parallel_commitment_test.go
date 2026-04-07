// Copyright 2024 The Erigon Authors
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

package execctx_test

import (
	"context"
	"math/rand/v2"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestParallelHashSort_RealDB verifies that ParallelHashSort produces the same
// root hash as the serial path when using real SharedDomains + MemBatch.
//
// This reproduces the hive rpc-compat failure where ParallelHashSort produced
// wrong trie roots due to interaction with DomainPut/GetLatest.
func TestParallelHashSort_RealDB(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	stepSize := uint64(1000)
	ctx := context.Background()

	// Generate random addresses — random bytes produce well-distributed
	// keccak256 hashes that diverge at the first nibble, enabling
	// CanDoConcurrentNext to return true.
	numAccounts := 500
	addrs := make([][]byte, numAccounts)
	rng := rand.New(rand.NewPCG(42, 0))
	for i := 0; i < numAccounts; i++ {
		addr := make([]byte, length.Addr)
		for j := range addr {
			addr[j] = byte(rng.Uint32())
		}
		addrs[i] = addr
	}

	// --- Serial path ---
	dbSerial := newTestDb(t, stepSize)
	rwTxSerial, err := dbSerial.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTxSerial.Rollback()

	domsSerial, err := execctx.NewSharedDomains(ctx, rwTxSerial, log.New())
	require.NoError(t, err)
	defer domsSerial.Close()

	// --- Parallel path ---
	dbPar := newTestDb(t, stepSize)
	rwTxPar, err := dbPar.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTxPar.Rollback()

	domsPar, err := execctx.NewSharedDomains(ctx, rwTxPar, log.New())
	require.NoError(t, err)
	defer domsPar.Close()

	// Enable parallel trie DB so ConcurrentPatriciaHashed can use ParallelHashSort
	domsPar.EnableParaTrieDB(dbPar)

	var txNum uint64

	// Block 0 (genesis): write many accounts to both paths
	for i := 0; i < numAccounts; i++ {
		acc := accounts.Account{
			Nonce:    0,
			Balance:  *uint256.NewInt(uint64(i+1) * 1e18),
			CodeHash: accounts.EmptyCodeHash,
		}
		v := accounts.SerialiseV3(&acc)

		err = domsSerial.DomainPut(kv.AccountsDomain, rwTxSerial, addrs[i], v, txNum, nil)
		require.NoError(t, err)
		err = domsPar.DomainPut(kv.AccountsDomain, rwTxPar, addrs[i], v, txNum, nil)
		require.NoError(t, err)
	}

	rhSerial, err := domsSerial.ComputeCommitment(ctx, rwTxSerial, true, 0, txNum, "serial", nil)
	require.NoError(t, err)
	rhPar, err := domsPar.ComputeCommitment(ctx, rwTxPar, true, 0, txNum, "parallel", nil)
	require.NoError(t, err)

	require.Equal(t, rhSerial, rhPar, "genesis root mismatch")
	t.Logf("genesis root: %x", rhSerial)

	// Check if trie is ConcurrentPatriciaHashed
	_, isConcurrent := domsPar.GetCommitmentCtx().Trie().(*commitment.ConcurrentPatriciaHashed)
	if !isConcurrent {
		t.Skip("trie is not ConcurrentPatriciaHashed")
	}
	// Don't force parallel immediately. Let the first block's Process
	// set it via CanDoConcurrentNext (which we need to re-enable).
	// For now, force it after genesis to match the hive exec3 path where
	// enableConcurrentCommitmentIfPossible runs during SeekCommitment.
	domsPar.GetCommitmentCtx().SetConcurrentCommitment(true)
	t.Logf("forced concurrent=true after genesis")

	// Blocks 1-10: update subsets of accounts + storage
	for block := uint64(1); block <= 10; block++ {
		txNum = block * 3 // simple txNum progression

		// Update accounts + storage per block (mix like real chain)
		for i := 0; i < 10; i++ {
			idx := int((block*10 + uint64(i)) % uint64(numAccounts))
			acc := accounts.Account{
				Nonce:    block,
				Balance:  *uint256.NewInt(block*1000 + uint64(i)),
				CodeHash: accounts.EmptyCodeHash,
			}
			v := accounts.SerialiseV3(&acc)

			prevSerial, _, err := domsSerial.GetLatest(kv.AccountsDomain, rwTxSerial, addrs[idx])
			require.NoError(t, err)
			err = domsSerial.DomainPut(kv.AccountsDomain, rwTxSerial, addrs[idx], v, txNum, prevSerial)
			require.NoError(t, err)

			prevPar, _, err := domsPar.GetLatest(kv.AccountsDomain, rwTxPar, addrs[idx])
			require.NoError(t, err)
			err = domsPar.DomainPut(kv.AccountsDomain, rwTxPar, addrs[idx], v, txNum, prevPar)
			require.NoError(t, err)

			// Also write storage for some accounts
			if i%3 == 0 {
				storageKey := make([]byte, length.Addr+length.Hash)
				copy(storageKey, addrs[idx])
				storageKey[length.Addr] = byte(block)
				storageKey[length.Addr+1] = byte(i)
				storageVal := []byte{byte(block), byte(i), 0x01}

				prevS, _, err := domsSerial.GetLatest(kv.StorageDomain, rwTxSerial, storageKey)
				require.NoError(t, err)
				err = domsSerial.DomainPut(kv.StorageDomain, rwTxSerial, storageKey, storageVal, txNum, prevS)
				require.NoError(t, err)

				prevSP, _, err := domsPar.GetLatest(kv.StorageDomain, rwTxPar, storageKey)
				require.NoError(t, err)
				err = domsPar.DomainPut(kv.StorageDomain, rwTxPar, storageKey, storageVal, txNum, prevSP)
				require.NoError(t, err)
			}
		}

		rhSerial, err = domsSerial.ComputeCommitment(ctx, rwTxSerial, true, block, txNum, "serial", nil)
		require.NoError(t, err)
		rhPar, err = domsPar.ComputeCommitment(ctx, rwTxPar, true, block, txNum, "parallel", nil)
		require.NoError(t, err)

		t.Logf("block %d: serial=%x par=%x", block, common.Copy(rhSerial)[:8], common.Copy(rhPar)[:8])
		require.Equalf(t, rhSerial, rhPar, "block %d root mismatch", block)
	}
}
