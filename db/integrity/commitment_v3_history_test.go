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

package integrity_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/integrity"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/execfinality"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type commitmentHistoryBlockReader struct {
	dbservices.FullBlockReader
	roots map[uint64]common.Hash
}

func (r *commitmentHistoryBlockReader) HeaderByNumber(_ context.Context, _ kv.Getter, blockNum uint64) (*types.Header, error) {
	root, ok := r.roots[blockNum]
	if !ok {
		return nil, fmt.Errorf("missing test header for block %d", blockNum)
	}
	return &types.Header{Root: root, Number: *uint256.NewInt(blockNum)}, nil
}

func (*commitmentHistoryBlockReader) TxnumReader() rawdbv3.TxNumsReader {
	return rawdbv3.TxNums
}

func TestStateRootVerifyByHistoryRebuildsFromAccountAndStorageHistory(t *testing.T) {
	const (
		stepSize = uint64(1)
		blocks   = uint64(3)
	)

	ctx := t.Context()
	logger := log.New()
	statecfg.EnableHistoricalCommitment()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()), temporaltest.WithStepSize(stepSize))
	agg := db.(state.HasAgg).Agg().(*state.Aggregator)
	agg.ForTestEdgeRecordsInCommitment(kv.CommitmentDomain, true)

	address := make([]byte, length.Addr)
	address[0] = 0x42
	address[len(address)-1] = 0x24
	secondAddress := make([]byte, length.Addr)
	secondAddress[0] = 0x24
	secondAddress[len(secondAddress)-1] = 0x42
	slot := make([]byte, length.Hash)
	slot[0] = 0x17
	slot[len(slot)-1] = 0x71
	storageKey := append(append([]byte(nil), address...), slot...)
	secondSlot := make([]byte, length.Hash)
	secondSlot[0] = 0x71
	secondSlot[len(secondSlot)-1] = 0x17
	secondStorageKey := append(append([]byte(nil), address...), secondSlot...)
	roots := make(map[uint64]common.Hash, blocks)

	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, rawdbv3.TxNums.Append(tx, 0, 0))
	domains, err := execctx.NewSharedDomains(ctx, tx, logger)
	require.NoError(t, err)
	genesisAccount := accounts.Account{
		Balance:  *uint256.NewInt(1),
		CodeHash: accounts.EmptyCodeHash,
	}
	genesisSecondAccount := accounts.Account{
		Balance:  *uint256.NewInt(2),
		CodeHash: accounts.EmptyCodeHash,
	}
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, address, accounts.SerialiseV3(&genesisAccount), 0, nil))
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, secondAddress, accounts.SerialiseV3(&genesisSecondAccount), 0, nil))
	require.NoError(t, domains.DomainPut(kv.StorageDomain, tx, storageKey, []byte{0x90, 0x00}, 0, nil))
	require.NoError(t, domains.DomainPut(kv.StorageDomain, tx, secondStorageKey, []byte{0x91, 0x00}, 0, nil))
	genesisRoot, err := domains.ComputeCommitment(ctx, tx, true, 0, 0, "history", nil)
	require.NoError(t, err)
	require.NoError(t, domains.Commit(ctx, tx))
	domains.Close()
	require.NoError(t, tx.Commit())
	roots[0] = common.BytesToHash(genesisRoot)

	for blockNum := uint64(1); blockNum <= blocks; blockNum++ {
		roots[blockNum] = writeHistoryBlock(t, db, logger, blockNum, address, storageKey)
	}

	require.NoError(t, agg.BuildFiles2(ctx, db, 0, kv.Step(blocks*2+1), execfinality.NewContext(^uint64(0), ^uint64(0), 0, false), false))
	agg.WaitForFiles()
	samplerCfg, err := integrity.NewSamplerCfg(1, 1)
	require.NoError(t, err)
	reader := &commitmentHistoryBlockReader{roots: roots}
	require.NoError(t, integrity.CheckCommitmentHistAtBlkRange(ctx, samplerCfg, db, reader, 1, blocks+1, logger))
}

func writeHistoryBlock(t *testing.T, db kv.TemporalRwDB, logger log.Logger, blockNum uint64, address, storageKey []byte) common.Hash {
	t.Helper()
	ctx := t.Context()
	writeTxNum := blockNum*2 - 1
	maxTxNum := blockNum * 2
	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	require.NoError(t, rawdbv3.TxNums.Append(tx, blockNum, maxTxNum))
	domains, err := execctx.NewSharedDomains(ctx, tx, logger)
	require.NoError(t, err)

	account := accounts.Account{
		Nonce:    blockNum,
		Balance:  *uint256.NewInt(blockNum * 100),
		CodeHash: accounts.EmptyCodeHash,
	}
	accountValue := accounts.SerialiseV3(&account)
	previous, _, err := domains.GetLatest(kv.AccountsDomain, tx, address)
	require.NoError(t, err)
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, address, accountValue, writeTxNum, previous))

	storageValue := []byte{0xa0 + byte(blockNum), byte(blockNum)}
	previous, _, err = domains.GetLatest(kv.StorageDomain, tx, storageKey)
	require.NoError(t, err)
	require.NoError(t, domains.DomainPut(kv.StorageDomain, tx, storageKey, storageValue, writeTxNum, previous))

	root, err := domains.ComputeCommitment(ctx, tx, true, blockNum, maxTxNum, "history", nil)
	require.NoError(t, err)
	require.NoError(t, domains.Commit(ctx, tx))
	domains.Close()
	require.NoError(t, tx.Commit())
	return common.BytesToHash(root)
}
