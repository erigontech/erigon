// Copyright 2024 The Erigon Authors
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

package execctx_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/execctx"
)

type errUnwindTx struct {
	kv.TemporalRwTx
	err error
}

func (e *errUnwindTx) Unwind(ctx context.Context, txNumUnwindTo uint64, cs *[kv.DomainLen][]kv.DomainEntryDiff) error {
	return e.err
}

// TestTemporalMemBatch_FlushPropagatesUnwindError demonstrates that
// TemporalMemBatch.Flush silently discards the error returned by the
// tx.(kv.TemporalRwTx).Unwind call it makes while draining a pending
// unwind changeset. When that call fails (e.g., ctx cancellation, cursor
// failure, bad diff data), Flush proceeds to flushDiffSet / flushWriters /
// IncrementStateVersion and ultimately returns nil, hiding a partial unwind
// from the caller. Callers of Flush therefore see a successful flush even
// though the DB state is inconsistent with sd.unwindToTxNum.
func TestTemporalMemBatch_FlushPropagatesUnwindError(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	const stepSize = uint64(16)
	db := newTestDb(t, stepSize)
	ctx := context.Background()

	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	stateChangeset := &changeset.StateChangeSet{}
	domains.SetChangesetAccumulator(stateChangeset)

	// Produce at least one diff entry so unwindChangeset is non-nil and
	// Flush has a reason to call tx.Unwind.
	addr := make([]byte, 20)
	addr[0] = 1
	domains.SetTxNum(1)
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, rwTx, addr, []byte{0x01}, 1, nil))

	var diffSet [kv.DomainLen][]kv.DomainEntryDiff
	for idx, d := range stateChangeset.Diffs {
		diffSet[idx] = d.GetDiffSet()
	}
	domains.Unwind(0, &diffSet)

	sentinel := errors.New("injected unwind failure")
	wrapped := &errUnwindTx{TemporalRwTx: rwTx, err: sentinel}

	err = domains.Flush(ctx, wrapped)
	require.ErrorIs(t, err, sentinel,
		"SharedDomains.Flush must propagate the tx.Unwind error — currently "+
			"TemporalMemBatch.Flush discards it at db/state/temporal_mem_batch.go (line ~636).")
}
