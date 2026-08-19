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
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	borengine "github.com/erigontech/erigon/polygon/bor"
	borchain "github.com/erigontech/erigon/polygon/chain"
	"github.com/erigontech/erigon/rpc"
)

type headerLookupErrorBlockReader struct {
	dbservices.FullBlockReader
	err error
}

func (r headerLookupErrorBlockReader) CanonicalHash(context.Context, kv.Getter, uint64) (common.Hash, bool, error) {
	return common.Hash{}, false, r.err
}

func (r headerLookupErrorBlockReader) HeaderNumber(context.Context, kv.Getter, common.Hash) (*uint64, error) {
	return nil, r.err
}

func newBorAPIWithHeaderLookupError(t *testing.T) (*BorImpl, error) {
	t.Helper()
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	wantErr := errors.New("header lookup failure")
	base := newBaseApiForTest(m)
	base._blockReader = headerLookupErrorBlockReader{FullBlockReader: base._blockReader, err: wantErr}
	engine := borengine.New(borchain.BorDevnet.Config, base._blockReader, nil, nil, log.New(), nil, nil)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	base._engine = engine
	return NewBorAPI(base, m.DB, nil), wantErr
}

func newBorAPI(t *testing.T) *BorImpl {
	t.Helper()
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	base := newBaseApiForTest(m)
	engine := borengine.New(borchain.BorDevnet.Config, base._blockReader, nil, nil, log.New(), nil, nil)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	base._engine = engine
	return NewBorAPI(base, m.DB, nil)
}

func TestBorNumberEndpointsPreserveUnknownBlockError(t *testing.T) {
	api := newBorAPI(t)
	number := rpc.BlockNumber(1_000_000)
	selector := rpc.BlockNumberOrHashWithNumber(number)

	tests := []struct {
		name string
		call func() error
	}{
		{"getSnapshot", func() error { _, err := api.GetSnapshot(&number); return err }},
		{"getSigners", func() error { _, err := api.GetSigners(&number); return err }},
		{"getAuthor", func() error { _, err := api.GetAuthor(&selector); return err }},
		{"getSnapshotProposer", func() error { _, err := api.GetSnapshotProposer(&selector); return err }},
		{"getSnapshotProposerSequence", func() error { _, err := api.GetSnapshotProposerSequence(&selector); return err }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.ErrorIs(t, test.call(), errUnknownBlock)
		})
	}
}

func TestBorGetSnapshotPropagatesHeaderLookupError(t *testing.T) {
	api, wantErr := newBorAPIWithHeaderLookupError(t)
	number := rpc.BlockNumber(0)

	snapshot, err := api.GetSnapshot(&number)

	require.ErrorIs(t, err, wantErr)
	require.Nil(t, snapshot)
}

func TestBorGetSignersPropagatesHeaderLookupError(t *testing.T) {
	api, wantErr := newBorAPIWithHeaderLookupError(t)
	number := rpc.BlockNumber(0)

	signers, err := api.GetSigners(&number)

	require.ErrorIs(t, err, wantErr)
	require.Nil(t, signers)
}

func TestBorGetAuthorPropagatesHeaderLookupError(t *testing.T) {
	api, wantErr := newBorAPIWithHeaderLookupError(t)
	number := rpc.BlockNumberOrHashWithNumber(0)

	_, err := api.GetAuthor(&number)

	require.ErrorIs(t, err, wantErr)
}

func TestBorGetSignersAtHashPropagatesHeaderLookupError(t *testing.T) {
	api, wantErr := newBorAPIWithHeaderLookupError(t)

	signers, err := api.GetSignersAtHash(common.Hash{1})

	require.ErrorIs(t, err, wantErr)
	require.Nil(t, signers)
}

func TestBorGetSnapshotProposerPropagatesHeaderLookupError(t *testing.T) {
	api, wantErr := newBorAPIWithHeaderLookupError(t)
	number := rpc.BlockNumberOrHashWithNumber(0)

	_, err := api.GetSnapshotProposer(&number)

	require.ErrorIs(t, err, wantErr)
}

func TestBorGetSnapshotProposerSequencePropagatesHeaderLookupError(t *testing.T) {
	api, wantErr := newBorAPIWithHeaderLookupError(t)
	number := rpc.BlockNumberOrHashWithNumber(0)

	_, err := api.GetSnapshotProposerSequence(&number)

	require.ErrorIs(t, err, wantErr)
}
