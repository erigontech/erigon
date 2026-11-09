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

package paymentlane

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

func TestAppendPageRejectsPageLargerThanPageSize(t *testing.T) {
	page := make([]common.Address, pageSize+1)
	err := appendPage(make(map[common.Address]struct{}), 0, page, uint64(len(page)))
	require.ErrorIs(t, err, ErrCorruptConfig)
	require.Contains(t, err.Error(), "returned 129 entries")
}

func TestAppendPageRejectsOverflowingPageLength(t *testing.T) {
	page := make([]common.Address, 2)
	err := appendPage(make(map[common.Address]struct{}), math.MaxUint64-1, page, math.MaxUint64)
	require.ErrorIs(t, err, ErrCorruptConfig)
	require.Contains(t, err.Error(), "length 2 exceeds totalLength")
}
