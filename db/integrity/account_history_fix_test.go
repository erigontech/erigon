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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/integrity"
)

func TestFixCodeHash(t *testing.T) {
	ctx, sc := t.Context(), integrity.SamplerCfg{SampleRatio: 1}
	db := buildAccountHistory(t, accountHistoryFaults{codeKeptAfterClear: true})
	require.ErrorIs(t, integrity.CheckCodeHash(ctx, sc, db, true, 0, log.New()), integrity.ErrIntegrity)

	res, err := integrity.FixCodeHash(ctx, db, false, log.New())
	require.NoError(t, err)
	require.Equal(t, 2, res.Keys)
	require.Zero(t, res.Unfixable)

	require.NoError(t, integrity.CheckCodeHash(ctx, sc, db, true, 0, log.New()))
	require.NoError(t, integrity.CheckStorageOfAbsentAccount(ctx, sc, db, true, 0, log.New()))

	res, err = integrity.FixCodeHash(ctx, db, false, log.New())
	require.NoError(t, err)
	require.Zero(t, res.Keys)
}

func TestFixStorageOfAbsentAccount(t *testing.T) {
	ctx, sc := t.Context(), integrity.SamplerCfg{SampleRatio: 1}
	db := buildAccountHistory(t, accountHistoryFaults{storageKeptAfterDestroy: true})
	require.ErrorIs(t, integrity.CheckStorageOfAbsentAccount(ctx, sc, db, true, 0, log.New()), integrity.ErrIntegrity)

	res, err := integrity.FixStorageOfAbsentAccount(ctx, db, false, log.New())
	require.NoError(t, err)
	require.Equal(t, 2, res.Keys)
	require.Zero(t, res.Unfixable)

	require.NoError(t, integrity.CheckStorageOfAbsentAccount(ctx, sc, db, true, 0, log.New()))
	require.NoError(t, integrity.CheckCodeHash(ctx, sc, db, true, 0, log.New()))

	res, err = integrity.FixStorageOfAbsentAccount(ctx, db, false, log.New())
	require.NoError(t, err)
	require.Zero(t, res.Keys)
}

func TestFixDryRunKeepsFiles(t *testing.T) {
	ctx, sc := t.Context(), integrity.SamplerCfg{SampleRatio: 1}
	db := buildAccountHistory(t, accountHistoryFaults{codeKeptAfterClear: true, storageKeptAfterDestroy: true})

	res, err := integrity.FixCodeHash(ctx, db, true, log.New())
	require.NoError(t, err)
	require.Equal(t, 2, res.Keys)
	res, err = integrity.FixStorageOfAbsentAccount(ctx, db, true, log.New())
	require.NoError(t, err)
	require.Equal(t, 2, res.Keys)

	require.ErrorIs(t, integrity.CheckCodeHash(ctx, sc, db, true, 0, log.New()), integrity.ErrIntegrity)
	require.ErrorIs(t, integrity.CheckStorageOfAbsentAccount(ctx, sc, db, true, 0, log.New()), integrity.ErrIntegrity)
}
