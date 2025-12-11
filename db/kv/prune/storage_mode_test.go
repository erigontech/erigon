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

	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon/db/kv/memdb"
)

func TestSetStorageModeIfNotExist(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		_, tx := memdb.NewTestTx(t)
		prune, err := Get(tx)
		assert.NoError(t, err)
		assert.Equal(t, DefaultMode, prune)
	})

	t.Run("setIfNotExist", func(t *testing.T) {
		_, tx := memdb.NewTestTx(t)
		prune, err := Get(tx)
		assert.NoError(t, err)
		assert.Equal(t, DefaultMode, prune)

		err = setIfNotExist(tx, FullMode)
		assert.NoError(t, err)

		prune, err = Get(tx)
		assert.NoError(t, err)
		assert.Equal(t, FullMode, prune)
	})
}

func TestParseCLIMode(t *testing.T) {
	t.Run("full", func(t *testing.T) {
		mode, err := FromCli(fullModeStr, 0, 0)
		assert.NoError(t, err)
		assert.Equal(t, FullMode, mode)

		assert.Equal(t, "full", mode.String())
	})
	t.Run("archive", func(t *testing.T) {
		mode, err := FromCli(archiveModeStr, 0, 0)
		assert.NoError(t, err)
		assert.Equal(t, ArchiveMode, mode)
		assert.Equal(t, archiveModeStr, mode.String())
	})
	t.Run("archive_override", func(t *testing.T) {
		exp := ArchiveMode
		exp.Blocks = Distance(100500)
		exp.History = Distance(400500)

		mode, err := FromCli(archiveModeStr, exp.History.toValue(), exp.Blocks.toValue())
		assert.NoError(t, err)
		assert.Equal(t, exp, mode)
		assert.Equal(t, "archive --prune.distance=400500 --prune.distance.blocks=100500", mode.String())
	})
	t.Run("minimal", func(t *testing.T) {
		mode, err := FromCli(minimalModeStr, 0, 0)
		assert.NoError(t, err)
		assert.Equal(t, MinimalMode, mode)
		assert.Equal(t, minimalModeStr, mode.String())
	})
	t.Run("garbage", func(t *testing.T) {
		_, err := FromCli("garb", 1, 2)
		assert.ErrorIs(t, err, ErrUnknownPruneMode)
	})
	t.Run("empty", func(t *testing.T) {
		mode, err := FromCli("", 0, 0)
		assert.NoError(t, err)

		assert.Equal(t, DefaultMode, mode)
		assert.Equal(t, "archive", mode.String())
	})
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
