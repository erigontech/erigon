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
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

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
		backgroundCtx: t.Context(),
		blockReader:   headerNumberErrorReader{err: expectedErr},
	}
	header := &types.Header{Number: *uint256.NewInt(0)}

	err := e.unwindToCommonCanonical(nil, emptyStageProgressTx{}, header, func() error { return nil })

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
	// prove that suspension failure aborts before the validator stages its unwind.
	suspendErr := errors.New("read-ahead suspension cancelled")
	_, _, _, criticalErr := fv.ValidatePayload(t.Context(), nil, nil, payloadHeader, &types.RawBody{}, func() error {
		return suspendErr
	}, log.New())
	require.ErrorIs(t, criticalErr, suspendErr)
}

func TestUpdateForkChoiceDropsExplicitlyCanceledRequestBeforeAdmission(t *testing.T) {
	admission := semaphore.NewWeighted(1)
	require.NoError(t, admission.Acquire(t.Context(), 1))
	defer admission.Release(1)

	expectedErr := errors.New("selected head changed")
	requestCtx, cancel := context.WithCancelCause(t.Context())
	cancel(expectedErr)
	outcome := make(chan forkchoiceOutcome, 1)
	module := &ExecModule{semaphore: admission, logger: log.New()}

	err := module.updateForkChoice(t.Context(), requestCtx, common.Hash{}, common.Hash{}, common.Hash{}, outcome)

	require.ErrorIs(t, err, expectedErr)
	require.ErrorIs(t, (<-outcome).err, expectedErr)
}

func TestForkchoiceWorkContextCancelsExplicitRequest(t *testing.T) {
	expectedErr := errors.New("selected head changed")
	requestCtx, cancelRequest := context.WithCancelCause(t.Context())
	workCtx, cleanup := forkchoiceWorkContext(t.Context(), requestCtx)
	defer cleanup()

	cancelRequest(expectedErr)

	select {
	case <-workCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("forkchoice work remained active after explicit cancellation")
	}
	require.ErrorIs(t, context.Cause(workCtx), expectedErr)
}

func TestForkchoiceWorkContextKeepsDeadlineAsynchronous(t *testing.T) {
	requestCtx, cancelRequest := context.WithCancelCause(t.Context())
	workCtx, cleanup := forkchoiceWorkContext(t.Context(), requestCtx)
	defer cleanup()
	cancelRequest(context.DeadlineExceeded)

	select {
	case <-workCtx.Done():
		t.Fatal("forkchoice deadline canceled asynchronous work")
	case <-time.After(20 * time.Millisecond):
	}
}
