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

package parlia

import (
	"compress/gzip"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

func loadChapelHeaders(t *testing.T) []*types.Header {
	t.Helper()
	f, err := os.Open("testdata/chapel_headers.rlp.gz")
	require.NoError(t, err)
	defer f.Close()
	zr, err := gzip.NewReader(f)
	require.NoError(t, err)
	s := rlp.NewStream(zr, 0)
	var headers []*types.Header
	for {
		h := new(types.Header)
		if err := s.Decode(h); errors.Is(err, io.EOF) {
			break
		} else {
			require.NoError(t, err)
		}
		if len(headers) > 0 {
			require.Equal(t, headers[len(headers)-1].Hash(), h.ParentHash)
		}
		headers = append(headers, h)
	}
	return headers
}

func headerAt(t *testing.T, headers []*types.Header, num uint64) *types.Header {
	t.Helper()
	first := headers[0].Number.Uint64()
	require.GreaterOrEqual(t, num, first)
	require.Less(t, num-first, uint64(len(headers)))
	return headers[num-first]
}
