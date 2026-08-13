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
	"github.com/erigontech/erigon/common/log/v3"
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

type sideForkReader struct {
	dbservices.FullBlockReader
	canonicalHash common.Hash
	forkHeader    *types.Header
	forkBody      *types.Body
}

func (r sideForkReader) IsCanonical(_ context.Context, _ kv.Getter, hash common.Hash, _ uint64) (bool, error) {
	return hash == r.canonicalHash, nil
}

func (r sideForkReader) Header(_ context.Context, _ kv.Getter, hash common.Hash, _ uint64) (*types.Header, error) {
	if hash == r.forkHeader.Hash() {
		return r.forkHeader, nil
	}
	return nil, nil
}

func (r sideForkReader) BodyWithTransactions(_ context.Context, _ kv.Getter, hash common.Hash, _ uint64) (*types.Body, error) {
	if hash == r.forkHeader.Hash() {
		return r.forkBody, nil
	}
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

	err := e.unwindToCommonCanonical(nil, emptyStageProgressTx{}, header, func() {})

	require.ErrorIs(t, err, expectedErr)
}

func TestForkValidatorSuspendsReadAheadBeforeItsOwnUnwind(t *testing.T) {
	canonicalHash := common.HexToHash("0x01")
	forkHeader := &types.Header{ParentHash: canonicalHash, Number: *uint256.NewInt(2)}
	payloadHeader := &types.Header{ParentHash: forkHeader.Hash(), Number: *uint256.NewInt(3)}
	reader := sideForkReader{
		canonicalHash: canonicalHash,
		forkHeader:    forkHeader,
		forkBody:      &types.Body{},
	}
	fv := newForkValidator(t.Context(), 10, &PipelineExecutor{}, reader, 16)

	// Stop at the suspension boundary; this test needs no execution pipeline to
	// prove that read-ahead is suspended before the validator stages its unwind.
	const stopAfterSuspension = "read-ahead suspended"
	require.PanicsWithValue(t, stopAfterSuspension, func() {
		fv.ValidatePayload(t.Context(), nil, nil, payloadHeader, &types.RawBody{}, func() {
			panic(stopAfterSuspension)
		}, log.New())
	})
}
