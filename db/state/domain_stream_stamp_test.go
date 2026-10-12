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
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/statecfg"
)

func TestDomainLatestIterFileStamp(t *testing.T) {
	db, d := testDbAndDomainOfStep(t, statecfg.Schema.AccountsDomain, 16, log.New())
	ctx := t.Context()

	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	domainTx := d.beginForTests()
	writer := domainTx.NewWriter(db)

	fileKey := []byte("file")
	dbKey := []byte("db__")
	sourceKey := []byte("source")
	deletedKey := []byte("delete")
	fileValue := []byte("file-new")
	dbValue := []byte("db-value")
	sourceFileValue := []byte("source-new")

	require.NoError(t, writer.PutWithPrev(fileKey, []byte("file-old"), 1, nil))
	require.NoError(t, writer.PutWithPrev(fileKey, fileValue, 17, []byte("file-old")))
	require.NoError(t, writer.PutWithPrev(dbKey, dbValue, 33, nil))
	require.NoError(t, writer.PutWithPrev(sourceKey, []byte("source-old"), 1, nil))
	require.NoError(t, writer.PutWithPrev(sourceKey, sourceFileValue, 17, []byte("source-old")))
	require.NoError(t, writer.PutWithPrev(sourceKey, []byte("source-db"), 33, sourceFileValue))
	require.NoError(t, writer.PutWithPrev(deletedKey, []byte("delete-old"), 1, nil))
	require.NoError(t, writer.DeleteWithPrev(deletedKey, 17, []byte("delete-old")))
	require.NoError(t, writer.Flush(ctx, tx))
	writer.Close()
	domainTx.Close()
	require.NoError(t, d.collateBuildIntegrate(ctx, kv.Step(0), tx, background.NewProgressSet()))
	require.NoError(t, d.collateBuildIntegrate(ctx, kv.Step(1), tx, background.NewProgressSet()))
	require.NoError(t, tx.Commit())

	roTx, err := db.BeginRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	domainRoTx := d.beginForTests()
	defer domainRoTx.Close()

	iter, err := domainRoTx.DebugRangeLatest(roTx, nil, nil, kv.Unlim)
	require.NoError(t, err)
	defer iter.Close()

	values := make(map[string][]byte)
	stamps := make(map[string]uint64)
	for iter.HasNext() {
		key, value, iterErr := iter.Next()
		require.NoError(t, iterErr)
		values[string(key)] = bytes.Clone(value)
		stamps[string(key)] = iter.Stamp()
	}

	require.Equal(t, fileValue, values[string(fileKey)])
	require.Equal(t, uint64(31), stamps[string(fileKey)])
	require.Equal(t, dbValue, values[string(dbKey)])
	require.Equal(t, uint64(32), stamps[string(dbKey)])
	require.Equal(t, []byte("source-db"), values[string(sourceKey)])
	require.Equal(t, uint64(32), stamps[string(sourceKey)])
	_, deleted := values[string(deletedKey)]
	require.False(t, deleted)

	filesIter, err := domainRoTx.DebugRangeLatestFromFiles(nil, nil, kv.Unlim)
	require.NoError(t, err)
	defer filesIter.Close()

	fileValues := make(map[string][]byte)
	fileStamps := make(map[string]uint64)
	for filesIter.HasNext() {
		key, value, iterErr := filesIter.Next()
		require.NoError(t, iterErr)
		fileValues[string(key)] = bytes.Clone(value)
		fileStamps[string(key)] = filesIter.Stamp()
	}

	require.Equal(t, fileValue, fileValues[string(fileKey)])
	require.Equal(t, uint64(31), fileStamps[string(fileKey)])
	require.Equal(t, sourceFileValue, fileValues[string(sourceKey)])
	require.Equal(t, uint64(31), fileStamps[string(sourceKey)])
	_, found := fileValues[string(dbKey)]
	require.False(t, found)
	_, found = fileValues[string(deletedKey)]
	require.False(t, found)
}
