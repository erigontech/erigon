// Copyright 2026 The Erigon Authors
// This file is part of Erigon.

package execmodule

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
)

type canonicalHashErrorReader struct {
	dbservices.FullBlockReader
	err error
}

func (r canonicalHashErrorReader) CanonicalHash(context.Context, kv.Getter, uint64) (common.Hash, bool, error) {
	return common.Hash{}, false, r.err
}

func TestGetHeaderByNumberReturnsCanonicalHashError(t *testing.T) {
	expectedErr := errors.New("canonical hash read failed")
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	module := &ExecModule{db: db, blockReader: canonicalHashErrorReader{err: expectedErr}}
	number := uint64(11)

	header, err := module.GetHeader(t.Context(), nil, &number)

	require.Nil(t, header)
	require.ErrorIs(t, err, expectedErr)
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
}
