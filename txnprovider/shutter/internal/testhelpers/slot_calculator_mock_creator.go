package testhelpers

import (
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/txnprovider/shutter"
)

type MockSlotCalculatorCreatorOpt func(mock *MockSlotCalculator)

func MockSlotCalculatorCreator(opts ...MockSlotCalculatorCreatorOpt) func(t *testing.T) shutter.SlotCalculator {
	return func(t *testing.T) shutter.SlotCalculator {
		ctrl := gomock.NewController(t)
		sc := NewMockSlotCalculator(ctrl)
		for _, opt := range opts {
			opt(sc)
		}
		return sc
	}
}

func WithCalcCurrentSlotMockResult(results ...uint64) MockSlotCalculatorCreatorOpt {
	i := -1
	return func(sc *MockSlotCalculator) {
		sc.EXPECT().
			CalcCurrentSlot().
			DoAndReturn(func() uint64 {
				i++
				return results[i]
			}).
			Times(len(results))
	}
}
