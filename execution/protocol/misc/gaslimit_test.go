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

package misc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// A test for https://github.com/erigontech/erigon/issues/18424
func TestCalcGasLimit(t *testing.T) {
	// https://gnosisscan.io//block/43788389
	parentGasLimit := uint64(16_999_984)
	desiredLimit := uint64(17_000_000)
	gasLimit := CalcGasLimit(parentGasLimit, desiredLimit)
	assert.Equal(t, desiredLimit, gasLimit)
}
