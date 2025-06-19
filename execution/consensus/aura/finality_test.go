// Copyright 2024 The Erigon Authors
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

package aura

import (
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRollingFinality(t *testing.T) {
	t.Run("RejectsUnknownSigners", func(t *testing.T) {
		f := NewRollingFinality([]common.Address{{1}, {2}, {3}})
		_, err := f.push(common.Hash{}, 0, []common.Address{{0}, {4}})
		assert.Error(t, err)
		_, err = f.push(common.Hash{}, 0, []common.Address{{0}, {1}, {4}})
		assert.Error(t, err)
	})
	t.Run("FinalizeMultiple", func(t *testing.T) {
		signers := []common.Address{{0}, {1}, {2}, {3}, {4}, {5}}
		f := NewRollingFinality(signers)
		// 3 / 6 signers is < 51% so no finality.
		for i := 0; i < 6; i++ {
			l, err := f.push(common.Hash{byte(i)}, uint64(i%3), []common.Address{signers[i%3]})
			require.NoError(t, err)
			assert.Empty(t, l)
		}
		// after pushing a block signed by a fourth validator, the first four
		// blocks of the unverified chain become verified.
		l, err := f.push(common.Hash{byte(6)}, 6, []common.Address{signers[4]})
		require.NoError(t, err)
		for i := uint64(0); i < 4; i++ {
			assert.Equal(t, common.Hash{byte(i)}, l[i].hash)
		}
		assert.Len(t, l, 4)
	})
	t.Run("FromAncestry", func(t *testing.T) {
		signers := []common.Address{{0}, {1}, {2}, {3}, {4}, {5}}
		f := NewRollingFinality(signers)
		i := 12
		get := func(hash common.Hash) ([]common.Address, common.Hash, common.Hash, uint64, bool) {
			i--
			if i == -1 {
				return nil, common.Hash{}, common.Hash{}, 0, false
			}
			return []common.Address{signers[i%6]}, common.Hash{byte(i)}, common.Hash{byte(i - 1)}, uint64(i), true
		}
		err := f.buildAncestrySubChain(get, common.Hash{11}, common.Hash{99})
		require.NoError(t, err)
		assert.Equal(t, 3, f.headers.l.Len())
		assert.Equal(t, common.Hash{11}, *f.lastPushed)
	})
	t.Run("FromAncestryMultipleSigners", func(t *testing.T) {
		signers := []common.Address{{0}, {1}, {2}, {3}, {4}, {5}}
		f := NewRollingFinality(signers)
		i := 12
		get := func(hash common.Hash) ([]common.Address, common.Hash, common.Hash, uint64, bool) {
			i--
			if i == -1 {
				return nil, common.Hash{}, common.Hash{}, 0, false
			}
			return []common.Address{signers[i%6], signers[(i+1)%6], signers[(i+2)%6]}, common.Hash{byte(i)}, common.Hash{byte(i - 1)}, uint64(i), true
		}
		err := f.buildAncestrySubChain(get, common.Hash{11}, common.Hash{99})
		require.NoError(t, err)

		// only the last hash has < 51% of authorities' signatures
		assert.Equal(t, 1, f.headers.l.Len())
		assert.Equal(t, common.Hash{11}, f.headers.Front().hash)
		assert.Equal(t, common.Hash{11}, *f.lastPushed)
	})

}
