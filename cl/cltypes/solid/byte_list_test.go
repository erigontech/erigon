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

func TestByteListSSZProgressiveRootMemoized(t *testing.T) {
	byteList := NewByteListSSZ(2048)
	require.NoError(t, byteList.SetBytes(make([]byte, 1024)))
	_, err := byteList.HashSSZProgressive()
	require.NoError(t, err)

	var hashErr error
	allocs := testing.AllocsPerRun(100, func() {
		_, hashErr = byteList.HashSSZProgressive()
	})
	require.NoError(t, hashErr)
	require.Zero(t, allocs)
}

func TestByteListSSZMutationInvalidatesProgressiveRoot(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ByteListSSZ) error
	}{
		{
			name: "DecodeSSZ",
			mutate: func(byteList *ByteListSSZ) error {
				return byteList.DecodeSSZ([]byte{4, 5, 6, 7}, 0)
			},
		},
		{
			name: "SetBytes",
			mutate: func(byteList *ByteListSSZ) error {
				return byteList.SetBytes([]byte{4, 5, 6, 7})
			},
		},
		{
			name: "UnmarshalJSON",
			mutate: func(byteList *ByteListSSZ) error {
				return byteList.UnmarshalJSON([]byte(`"0x04050607"`))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			byteList := NewByteListSSZ(16)
			require.NoError(t, byteList.SetBytes([]byte{1, 2, 3}))
			_, err := byteList.HashSSZProgressive()
			require.NoError(t, err)
			require.NoError(t, test.mutate(byteList))

			got, err := byteList.HashSSZProgressive()
			require.NoError(t, err)
			wantList := NewByteListSSZ(16)
			require.NoError(t, wantList.SetBytes([]byte{4, 5, 6, 7}))
			want, err := wantList.HashSSZProgressive()
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}

func TestByteListSSZProgressiveRootConcurrent(t *testing.T) {
	byteList := NewByteListSSZ(2048)
	require.NoError(t, byteList.SetBytes(make([]byte, 1024)))
	wantList := NewByteListSSZ(2048)
	require.NoError(t, wantList.SetBytes(make([]byte, 1024)))
	want, err := wantList.HashSSZProgressive()
	require.NoError(t, err)

	const goroutines = 16
	results := make(chan [32]byte, goroutines)
	errs := make(chan error, goroutines)
	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			root, err := byteList.HashSSZProgressive()
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
