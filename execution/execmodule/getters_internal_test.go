// Copyright 2026 The Erigon Authors
// This file is part of Erigon.

package execmodule

import (
	"context"
	"errors"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/types"
)

type canonicalHashErrorReader struct {
	dbservices.FullBlockReader
	err error
}

func (r canonicalHashErrorReader) CanonicalHash(context.Context, kv.Getter, uint64) (common.Hash, bool, error) {
	return common.Hash{}, false, r.err
}

func TestHeaderLookupsReturnCanonicalHashError(t *testing.T) {
	expectedErr := errors.New("canonical hash read failed")
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	module := &ExecModule{db: db, blockReader: canonicalHashErrorReader{err: expectedErr}}
	number := uint64(11)
	head := &types.Header{Number: *uint256.NewInt(number)}
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		if err := rawdb.WriteHeader(tx, head); err != nil {
			return err
		}
		return rawdb.WriteHeadHeaderHash(tx, head.Hash())
	}))

	t.Run("number", func(t *testing.T) {
		header, err := module.GetHeader(t.Context(), nil, &number)
		require.Nil(t, header)
		require.ErrorIs(t, err, expectedErr)
	})
	t.Run("range", func(t *testing.T) {
		headers, err := module.GetHeadersByRange(t.Context(), number, 2)
		require.Nil(t, headers)
		require.ErrorIs(t, err, expectedErr)
	})
}

func TestResolveSegmentByNumberNotFound(t *testing.T) {
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	module := &ExecModule{db: db}
	number := uint64(11)
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	_, _, err = module.resolveSegment(t.Context(), tx, nil, &number)
	require.ErrorIs(t, err, errNotFound)

	header, err := module.GetHeader(t.Context(), nil, &number)
	require.NoError(t, err)
	require.Nil(t, header)
	headers, err := module.GetHeadersByRange(t.Context(), number, 2)
	require.NoError(t, err)
	require.Empty(t, headers)
}

type reorgBlockReader struct {
	dbservices.FullBlockReader
	reorg func() error
}

func (r *reorgBlockReader) CanonicalHash(_ context.Context, tx kv.Getter, number uint64) (common.Hash, bool, error) {
	hash, err := rawdb.ReadCanonicalHash(tx, number)
	return hash, hash != (common.Hash{}), err
}

func (r *reorgBlockReader) Header(_ context.Context, tx kv.Getter, hash common.Hash, number uint64) (*types.Header, error) {
	header := rawdb.ReadHeader(tx, hash, number)
	if r.reorg != nil {
		reorg := r.reorg
		r.reorg = nil
		if err := reorg(); err != nil {
			return nil, err
		}
	}
	return header, nil
}

func TestGetHeadersByRangePreservesGaps(t *testing.T) {
	for _, tc := range []struct {
		name          string
		canonicalHash bool
	}{
		{name: "missing canonical hash"},
		{name: "missing header", canonicalHash: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
			headers := []*types.Header{
				{Number: *uint256.NewInt(10), Time: 250},
				{Number: *uint256.NewInt(11), Time: 251},
				{Number: *uint256.NewInt(12), Time: 252},
			}
			headers[1].ParentHash = headers[0].Hash()
			headers[2].ParentHash = headers[1].Hash()
			require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
				for i, header := range headers {
					if i != 1 {
						if err := rawdb.WriteHeader(tx, header); err != nil {
							return err
						}
					}
					if i != 1 || tc.canonicalHash {
						if err := rawdb.WriteCanonicalHash(tx, header.Hash(), header.Number.Uint64()); err != nil {
							return err
						}
					}
				}
				return rawdb.WriteHeadHeaderHash(tx, headers[2].Hash())
			}))
			module := &ExecModule{db: db, blockReader: &reorgBlockReader{}}

			got, err := module.GetHeadersByRange(t.Context(), 10, 4)
			require.NoError(t, err)
			require.Len(t, got, 3)
			require.Equal(t, headers[0].Hash(), got[0].Hash())
			require.Nil(t, got[1])
			require.Equal(t, headers[2].Hash(), got[2].Hash())
		})
	}
}

func TestGetHeadersByRangeReorg(t *testing.T) {
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	oldChain := []*types.Header{
		{Number: *uint256.NewInt(10), Time: 250},
		{Number: *uint256.NewInt(11), Time: 251},
	}
	oldChain[1].ParentHash = oldChain[0].Hash()
	newChain := []*types.Header{
		{Number: *uint256.NewInt(10), Time: 252},
		{Number: *uint256.NewInt(11), Time: 253},
		{Number: *uint256.NewInt(12), Time: 254},
	}
	newChain[1].ParentHash = newChain[0].Hash()
	newChain[2].ParentHash = newChain[1].Hash()
	writeChain := func(headers []*types.Header) error {
		return db.Update(t.Context(), func(tx kv.RwTx) error {
			for _, header := range headers {
				if err := rawdb.WriteHeader(tx, header); err != nil {
					return err
				}
				if err := rawdb.WriteCanonicalHash(tx, header.Hash(), header.Number.Uint64()); err != nil {
					return err
				}
			}
			return rawdb.WriteHeadHeaderHash(tx, headers[len(headers)-1].Hash())
		})
	}
	require.NoError(t, writeChain(oldChain))
	reader := &reorgBlockReader{reorg: func() error {
		result := make(chan error, 1)
		go func() { result <- writeChain(newChain) }()
		return <-result
	}}
	module := &ExecModule{db: db, blockReader: reader}

	headers, err := module.GetHeadersByRange(t.Context(), 10, 4)
	require.NoError(t, err)
	require.Len(t, headers, 2)
	for i, header := range headers {
		require.Equal(t, oldChain[i].Hash(), header.Hash(), "header %d", i)
	}

	number := uint64(11)
	header, err := module.GetHeader(t.Context(), nil, &number)
	require.NoError(t, err)
	require.Equal(t, newChain[1].Hash(), header.Hash())
}
