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

package jsonrpc

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/rpc"
)

type canonicalHashErrorBlockReader struct {
	dbservices.FullBlockReader
	err error
}

func (r canonicalHashErrorBlockReader) CanonicalHash(context.Context, kv.Getter, uint64) (common.Hash, bool, error) {
	return common.Hash{}, false, r.err
}

func newBorAPIWithCanonicalHashError(t *testing.T) (*BorImpl, error) {
	t.Helper()
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	wantErr := errors.New("canonical hash failure")
	base := newBaseApiForTest(m)
	base._blockReader = canonicalHashErrorBlockReader{FullBlockReader: base._blockReader, err: wantErr}
	return NewBorAPI(base, m.DB, nil), wantErr
}

func TestBorGetSnapshotPropagatesHeaderLookupError(t *testing.T) {
	api, wantErr := newBorAPIWithCanonicalHashError(t)
	number := rpc.BlockNumber(0)

	snapshot, err := api.GetSnapshot(&number)

	require.ErrorIs(t, err, wantErr)
	require.Nil(t, snapshot)
}

func TestBorGetSignersPropagatesHeaderLookupError(t *testing.T) {
	api, wantErr := newBorAPIWithCanonicalHashError(t)
	number := rpc.BlockNumber(0)

	signers, err := api.GetSigners(&number)

	require.ErrorIs(t, err, wantErr)
	require.Nil(t, signers)
}
