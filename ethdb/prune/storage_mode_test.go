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

package prune

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon/erigon-lib/common/math"
	"github.com/erigontech/erigon/erigon-lib/kv/memdb"
)

func TestSetStorageModeIfNotExist(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	prune, err := Get(tx)
	assert.NoError(t, err)
	assert.Equal(t, Mode{true, Distance(math.MaxUint64), Distance(math.MaxUint64), Experiments{}}, prune)

	err = setIfNotExist(tx, Mode{true, Distance(1), Distance(2), Experiments{}})
	assert.NoError(t, err)

	prune, err = Get(tx)
	assert.NoError(t, err)
	assert.Equal(t, Mode{true, Distance(1), Distance(2), Experiments{}}, prune)
}

var distanceTests = []struct {
	stageHead uint64
	pruneTo   uint64
	expected  uint64
}{
	{3_000_000, 1, 2_999_999},
	{3_000_000, 4_000_000, 0},
	{3_000_000, math.MaxUint64, 0},
	{3_000_000, 1_000_000, 2_000_000},
}

func TestDistancePruneTo(t *testing.T) {
	for _, tt := range distanceTests {
		t.Run(strconv.FormatUint(tt.pruneTo, 10), func(t *testing.T) {
			stageHead := tt.stageHead
			d := Distance(tt.pruneTo)
			pruneTo := d.PruneTo(stageHead)

			if pruneTo != tt.expected {
				t.Errorf("got %d, want %d", pruneTo, tt.expected)
			}
		})
	}
}
