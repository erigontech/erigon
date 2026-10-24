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

package state_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
)

func TestPBinRangeWriterExistingSplitStampMatchesReference(t *testing.T) {
	selectPBinRangeWriterHash(t)
	run := openPBinRangeWriterRun(t, 7, 0, 23)
	first := make([]byte, 34)
	first[0] = eip8297.CodeZone
	second := append([]byte(nil), first...)
	second[33] = 0x10
	ops := []pbt.Op{{Key: first}, {Key: second}}
	for i := range ops {
		copy(ops[i].Value[:], pbinRangeWriterCodeValue(1))
	}
	run.domains.GetCommitmentCtx().SetPBinOps(ops)
	_, err := run.domains.GetCommitmentCtx().ComputeCommitmentWithDiff(t.Context(), run.tx, true, 0, 23, "review-seed", nil, nil, nil, nil)
	require.NoError(t, err)
	require.NoError(t, run.domains.Flush(t.Context(), run.tx))
	_, err = run.write(t.Context(), []state.PBinLeaf{{Key: first, Value: pbinRangeWriterCodeValue(2), Stamp: 23}})
	require.ErrorContains(t, err, "already holds rows", "the range writer must refuse a target that holds rows")
}
