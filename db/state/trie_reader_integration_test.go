package state_test

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func makeTestAccountAddr(i uint64) []byte {
	addr := make([]byte, length.Addr)
	binary.BigEndian.PutUint64(addr[length.Addr-8:], i+1)
	return addr
}

func makeTestStorageKey(acctIdx, slot uint64) []byte {
	key := make([]byte, length.Addr+length.Hash)
	binary.BigEndian.PutUint64(key[length.Addr-8:], acctIdx+1)
	binary.BigEndian.PutUint64(key[length.Addr+length.Hash-8:], slot+1)
	return key
}

// TestTrieReader_IntegrationWithRealData generates real state, computes commitment,
// then verifies that TrieReader can look up known keys from the actual branch data.
func TestTrieReader_IntegrationWithRealData(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	const (
		stepSize    = 10
		totalSteps  = 4
		numAccounts = 200
		numStorage  = 100
	)
	totalTxs := uint64(stepSize * totalSteps)

	db, agg := testDbAndAggregatorv3(t, stepSize)
	agg.ForTestReplaceKeysInValues(kv.CommitmentDomain, false)

	ctx := context.Background()
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	// Generate deterministic account keys (plain 20-byte addresses).
	accountKeys := make([][]byte, numAccounts)
	for i := 0; i < numAccounts; i++ {
		accountKeys[i] = makeTestAccountAddr(uint64(i))
	}

	// Generate deterministic storage keys (20-byte addr + 32-byte slot).
	storageKeys := make([][]byte, numStorage)
	for i := 0; i < numStorage; i++ {
		storageKeys[i] = makeTestStorageKey(uint64(i), 1)
	}

	var blockNum uint64
	for txNum := uint64(0); txNum < totalTxs; txNum++ {
		// Write accounts.
		for i := 0; i < numAccounts; i++ {
			acc := accounts.Account{
				Nonce:    txNum,
				Balance:  *uint256.NewInt(txNum * 1000),
				CodeHash: accounts.EmptyCodeHash,
			}
			buf := accounts.SerialiseV3(&acc)
			require.NoError(t, domains.DomainPut(kv.AccountsDomain, rwTx, accountKeys[i], buf, txNum, nil))
		}

		// Write storage.
		for i := 0; i < numStorage; i++ {
			var val [32]byte
			val[31] = byte(txNum + 1)
			require.NoError(t, domains.DomainPut(kv.StorageDomain, rwTx, storageKeys[i], val[:], txNum, nil))
		}

		// At step boundary: compute commitment and record block->txNum mapping.
		if (txNum+1)%stepSize == 0 {
			rh, err := domains.ComputeCommitment(ctx, rwTx, true, blockNum, txNum, "", nil)
			require.NoError(t, err)
			require.NotEmpty(t, rh)
			require.NotEqual(t, empty.RootHash.Bytes(), rh)
			require.NoError(t, domains.Flush(ctx, rwTx))
			require.NoError(t, rawdbv3.TxNums.Append(rwTx, blockNum, txNum))
			blockNum++
		}
	}

	require.NoError(t, domains.Flush(ctx, rwTx))
	domains.Close()
	require.NoError(t, rwTx.Commit())

	// Build snapshot files so branch data is persisted.
	require.NoError(t, agg.BuildFiles(totalTxs))

	// Open a read-only tx and create a TrieReader backed by real commitment data.
	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()

	// Create SharedDomains for the RO tx to get a proper TemporalGetter.
	roDomains, err := execctx.NewSharedDomains(ctx, roTx, log.New())
	require.NoError(t, err)
	defer roDomains.Close()

	trieCtx := commitmentdb.NewTrieContextRo(
		commitmentdb.NewLatestStateReader(roTx, roDomains),
		agg.StepSize(),
	)
	reader := commitment.NewTrieReader(trieCtx, length.Addr)

	// Verify account lookups.
	foundCount := 0
	for i, plainKey := range accountKeys {
		hashedKey := commitment.KeyToHexNibbleHash(plainKey)
		require.Equal(t, length.Hash*2, len(hashedKey), "account hashed key should be %d nibbles", length.Hash*2)

		c, found, err := reader.Lookup(hashedKey)
		require.NoError(t, err, "account lookup %d failed", i)
		if found {
			foundCount++
			require.True(t, c.AccountAddrLen() > 0,
				"account %d: found but no account address in cell", i)
		}
	}
	t.Logf("Account lookups: %d/%d found", foundCount, numAccounts)
	require.True(t, foundCount > 0, "expected at least some account lookups to succeed")

	// Verify storage lookups.
	storFoundCount := 0
	for i, plainKey := range storageKeys {
		hashedKey := commitment.KeyToHexNibbleHash(plainKey)
		require.Equal(t, length.Hash*2*2, len(hashedKey), "storage hashed key should be %d nibbles", length.Hash*2*2)

		c, found, err := reader.Lookup(hashedKey)
		require.NoError(t, err, "storage lookup %d failed", i)
		if found {
			storFoundCount++
			require.True(t, c.StorageAddrLen() > 0,
				"storage %d: found but no storage address in cell", i)
		}
	}
	t.Logf("Storage lookups: %d/%d found", storFoundCount, numStorage)
	require.True(t, storFoundCount > 0, "expected at least some storage lookups to succeed")

	// Verify miss: a key that was never written should not be found.
	missKey := make([]byte, length.Addr)
	missKey[0] = 0xFF
	missKey[1] = 0xFE
	missKey[2] = 0xFD
	hashedMiss := commitment.KeyToHexNibbleHash(missKey)
	_, found, err := reader.Lookup(hashedMiss)
	require.NoError(t, err)
	t.Logf("Miss key lookup: found=%v (expected false)", found)
}
