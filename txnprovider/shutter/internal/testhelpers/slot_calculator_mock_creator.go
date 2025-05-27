// Copyright 2025 The Erigon Authors
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

package testhelpers

import (
	"testing"

	"go.uber.org/mock/gomock"
)

type MockSlotCalculatorCreatorOpt func(mock *MockSlotCalculator)

func MockSlotCalculatorCreator(opts ...MockSlotCalculatorCreatorOpt) func(t *testing.T) *MockSlotCalculator {
	return func(t *testing.T) *MockSlotCalculator {
		ctrl := gomock.NewController(t)
		sc := NewMockSlotCalculator(ctrl)
		for _, opt := range opts {
			opt(sc)
		}
		return sc
	}
}

func WithCalcCurrentSlotMockResult(results ...uint64) MockSlotCalculatorCreatorOpt {
	return func(sc *MockSlotCalculator) {
		i := -1
		sc.EXPECT().
			CalcCurrentSlot().
			DoAndReturn(func() uint64 {
				i++
				return results[i]
			}).
			Times(len(results))
	}
}
