package jsonrpc

import (
	"math/big"
	"sync"
	"testing"
)

func Test_RecurringL1GasPriceTracker_newLowestPrice(t *testing.T) {
	cases := map[string]struct {
		history    []*big.Int
		newPrice   *big.Int
		want       *big.Int
		totalCount uint64
	}{
		"no history": {
			history:    []*big.Int{},
			newPrice:   big.NewInt(1),
			want:       big.NewInt(1),
			totalCount: 10,
		},
		"some history": {
			history:    []*big.Int{big.NewInt(1), big.NewInt(2), big.NewInt(3)},
			newPrice:   big.NewInt(4),
			want:       big.NewInt(1),
			totalCount: 10,
		},
		"new price is lower": {
			history:    []*big.Int{big.NewInt(2), big.NewInt(3), big.NewInt(4)},
			newPrice:   big.NewInt(1),
			want:       big.NewInt(1),
			totalCount: 10,
		},
		"old history is removed, latest price is new lowest": {
			history:    []*big.Int{big.NewInt(2), big.NewInt(3), big.NewInt(4), big.NewInt(5), big.NewInt(6)},
			newPrice:   big.NewInt(4),
			want:       big.NewInt(4),
			totalCount: 3,
		},
		"old history is removed, latest price is not new lowest": {
			history:    []*big.Int{big.NewInt(2), big.NewInt(3), big.NewInt(4), big.NewInt(5), big.NewInt(6)},
			newPrice:   big.NewInt(7),
			want:       big.NewInt(5),
			totalCount: 3,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			tracker := &RecurringL1GasPriceTracker{
				lowestMtx:    &sync.Mutex{},
				latestMtx:    &sync.Mutex{},
				priceHistory: tc.history,
				totalCount:   tc.totalCount,
			}

			tracker.calculateAndStoreNewLowestPrice(tc.newPrice)

			lowest := tracker.GetLowestPrice()

			if lowest.Cmp(tc.want) != 0 {
				t.Errorf("got %v, want %v", lowest, tc.want)
			}
		})
	}

}
