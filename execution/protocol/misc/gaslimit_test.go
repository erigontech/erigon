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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

func TestMaxGasLimit(t *testing.T) {
	config := &chain.Config{LondonBlock: common.NewUint64(10)}
	for _, test := range []struct {
		name           string
		parentNumber   uint64
		parentGasLimit uint64
		want           uint64
	}{
		{name: "after London", parentNumber: 10, parentGasLimit: 30_000_000, want: 30_029_295},
		{name: "London transition", parentNumber: 9, parentGasLimit: 15_000_000, want: 30_029_295},
	} {
		parent := &types.Header{GasLimit: test.parentGasLimit}
		parent.Number.SetUint64(test.parentNumber)
		header := &types.Header{}
		header.Number.SetUint64(test.parentNumber + 1)
		assert.Equal(t, test.want, MaxGasLimit(config, parent, header), test.name)
		header.GasLimit = test.want
		assert.NoError(t, VerifyParentGasLimit(config, parent, header), test.name)
		header.GasLimit++
		assert.Error(t, VerifyParentGasLimit(config, parent, header), test.name)
	}
	for _, parentGasLimit := range []uint64{0, 1, 1023} {
		parent := &types.Header{GasLimit: parentGasLimit}
		parent.Number.SetUint64(10)
		header := &types.Header{}
		header.Number.SetUint64(11)
		assert.Equal(t, parentGasLimit, MaxGasLimit(config, parent, header), "no gas limit is valid after a parent with gas limit %d", parentGasLimit)
	}
}

// A test for https://github.com/erigontech/erigon/issues/18424
func TestCalcGasLimit(t *testing.T) {
	// https://gnosisscan.io//block/43788389
	parentGasLimit := uint64(16_999_984)
	desiredLimit := uint64(17_000_000)
	gasLimit := CalcGasLimit(parentGasLimit, desiredLimit)
	assert.Equal(t, desiredLimit, gasLimit)
}
