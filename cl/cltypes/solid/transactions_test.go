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

package solid

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransactionsSSZProgressiveRootMemoized(t *testing.T) {
	transactions := NewProgressiveTransactionsSSZFromTransactions([][]byte{
		make([]byte, 256),
		make([]byte, 256),
	})
	_, err := transactions.HashSSZProgressive()
	require.NoError(t, err)

	var hashErr error
	allocs := testing.AllocsPerRun(100, func() {
		_, hashErr = transactions.HashSSZProgressive()
	})
	require.NoError(t, hashErr)
	require.Zero(t, allocs)
}

func TestTransactionsSSZMutationInvalidatesProgressiveRoot(t *testing.T) {
	wantTransactions := [][]byte{{4, 5}, {6, 7}}
	encoded, err := NewProgressiveTransactionsSSZFromTransactions(wantTransactions).EncodeSSZ(nil)
	require.NoError(t, err)
	tests := []struct {
		name   string
		mutate func(*TransactionsSSZ) error
		want   [][]byte
	}{
		{
			name: "DecodeSSZ",
			mutate: func(transactions *TransactionsSSZ) error {
				return transactions.DecodeSSZ(encoded, 0)
			},
			want: wantTransactions,
		},
		{
			name: "DecodeSSZ empty",
			mutate: func(transactions *TransactionsSSZ) error {
				return transactions.DecodeSSZ(nil, 0)
			},
		},
		{
			name: "UnmarshalJSON",
			mutate: func(transactions *TransactionsSSZ) error {
				return transactions.UnmarshalJSON([]byte(`["0x0405","0x0607"]`))
			},
			want: wantTransactions,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transactions := NewProgressiveTransactionsSSZFromTransactions([][]byte{{1, 2, 3}})
			_, err := transactions.HashSSZProgressive()
			require.NoError(t, err)
			require.NoError(t, test.mutate(transactions))

			got, err := transactions.HashSSZProgressive()
			require.NoError(t, err)
			want, err := NewProgressiveTransactionsSSZFromTransactions(test.want).HashSSZProgressive()
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}

func TestTransactionsSSZDecodeFailureInvalidatesProgressiveRoot(t *testing.T) {
	transactions := NewTransactionsSSZWithLimits(2, 1)
	require.NoError(t, transactions.DecodeSSZ([]byte{4, 0, 0, 0, 0xaa}, 0))
	_, err := transactions.HashSSZProgressive()
	require.NoError(t, err)

	err = transactions.DecodeSSZ([]byte{8, 0, 0, 0, 9, 0, 0, 0, 0x11, 0x22, 0x33}, 0)
	require.Error(t, err)
	require.Equal(t, [][]byte{{0x11}, nil}, transactions.underlying)

	got, err := transactions.HashSSZProgressive()
	require.NoError(t, err)
	want, err := NewProgressiveTransactionsSSZFromTransactions([][]byte{{0x11}, nil}).HashSSZProgressive()
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestTransactionsSSZLegacyAndProgressiveRootsAreSeparate(t *testing.T) {
	transactions := [][]byte{{1, 2, 3}, {4, 5, 6}}

	legacyFirst := NewProgressiveTransactionsSSZFromTransactions(transactions)
	legacyRoot, err := legacyFirst.HashSSZ()
	require.NoError(t, err)
	progressiveRoot, err := legacyFirst.HashSSZProgressive()
	require.NoError(t, err)

	wantLegacy, err := NewProgressiveTransactionsSSZFromTransactions(transactions).HashSSZ()
	require.NoError(t, err)
	wantProgressive, err := NewProgressiveTransactionsSSZFromTransactions(transactions).HashSSZProgressive()
	require.NoError(t, err)
	require.Equal(t, wantLegacy, legacyRoot)
	require.Equal(t, wantProgressive, progressiveRoot)
	require.NotEqual(t, legacyRoot, progressiveRoot)

	progressiveFirst := NewProgressiveTransactionsSSZFromTransactions(transactions)
	progressiveRoot, err = progressiveFirst.HashSSZProgressive()
	require.NoError(t, err)
	legacyRoot, err = progressiveFirst.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, wantProgressive, progressiveRoot)
	require.Equal(t, wantLegacy, legacyRoot)
}

func TestTransactionsSSZProgressiveRootConcurrent(t *testing.T) {
	txData := [][]byte{
		make([]byte, 256),
		make([]byte, 256),
	}
	transactions := NewProgressiveTransactionsSSZFromTransactions(txData)
	want, err := NewProgressiveTransactionsSSZFromTransactions(txData).HashSSZProgressive()
	require.NoError(t, err)

	const goroutines = 16
	results := make(chan [32]byte, goroutines)
	errs := make(chan error, goroutines)
	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			root, err := transactions.HashSSZProgressive()
			results <- root
			errs <- err
		})
	}
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	for root := range results {
		require.Equal(t, want, root)
	}
}
