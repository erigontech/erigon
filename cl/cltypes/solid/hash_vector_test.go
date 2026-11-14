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

package solid_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
)

func TestHashVectorUnmarshalJSONLength(t *testing.T) {
	for _, count := range []int{32, 33, 34, 4097} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			vector := solid.NewHashVector(33)
			vector.Set(0, common.Hash{0xaa})
			before := append([]byte(nil), vector.Bytes()...)
			rootBefore, err := vector.HashSSZ()
			require.NoError(t, err)

			hashes := make([]common.Hash, count)
			for i := range hashes {
				hashes[i] = common.Hash{byte(i + 1)}
			}
			encoded, err := json.Marshal(hashes)
			require.NoError(t, err)
			err = json.Unmarshal(encoded, vector)
			if count != 33 {
				require.Error(t, err)
				require.Equal(t, 33, vector.Length())
				require.Equal(t, before, vector.Bytes())
				root, err := vector.HashSSZ()
				require.NoError(t, err)
				require.Equal(t, rootBefore, root)
				return
			}
			require.NoError(t, err)
			require.Equal(t, 33, vector.Length())
			expected := solid.NewHashVector(33)
			for i, hash := range hashes {
				require.Equal(t, hash, vector.Get(i))
				expected.Set(i, hash)
			}
			root, err := vector.HashSSZ()
			require.NoError(t, err)
			expectedRoot, err := expected.HashSSZ()
			require.NoError(t, err)
			require.Equal(t, expectedRoot, root)
		})
	}
}
