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

package execmodule

import (
	"context"
	"errors"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/types"
)

type headerNumberErrorReader struct {
	dbservices.FullBlockReader
	err error
}

func (r headerNumberErrorReader) HeaderNumber(context.Context, kv.Getter, common.Hash) (*uint64, error) {
	return nil, r.err
}

type emptyStageProgressTx struct {
	kv.TemporalRwTx
}

func (emptyStageProgressTx) GetOne(string, []byte) ([]byte, error) {
	return nil, nil
}

// The module is the one owner of the domain state cache: callers pass a byte
// budget, never a constructed cache, so a disabled cache cannot be built
// upstream and leak its memory-envelope reservation.
func TestNewDomainStateCacheRespectsUseStateCache(t *testing.T) {
	prev := dbg.UseStateCache
	t.Cleanup(func() { dbg.SetUseStateCache(prev) })

	dbg.SetUseStateCache(false)
	require.Nil(t, newDomainStateCache(0), "disabled mode must construct no cache")
	require.Nil(t, newDomainStateCache(16*datasize.MB), "a budget must not override the kill switch")

	dbg.SetUseStateCache(true)
	sc := newDomainStateCache(16 * datasize.MB)
	require.NotNil(t, sc)
	sc.Close()
	scDefault := newDomainStateCache(0)
	require.NotNil(t, scDefault, "zero budget means the production default, not no cache")
	scDefault.Close()
}

func TestUnwindToCommonCanonicalReturnsCanonicalityError(t *testing.T) {
	expectedErr := errors.New("canonicality read failed")
	e := &ExecModule{
		bacgroundCtx: t.Context(),
		blockReader:  headerNumberErrorReader{err: expectedErr},
	}
	header := &types.Header{Number: *uint256.NewInt(0)}

	err := e.unwindToCommonCanonical(nil, emptyStageProgressTx{}, header)

	require.ErrorIs(t, err, expectedErr)
}
