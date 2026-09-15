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

package cache

import (
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/cachebudget"
)

// NewByteLRU stays out of cachebudget.Global: filling it reserves nothing and Close returns nothing.
func TestNewByteLRUOutsideBudget(t *testing.T) {
	used := cachebudget.Global.Used()
	b := NewByteLRU(4*datasize.KB, func(_ uint64, v []byte) int64 { return int64(len(v)) })
	for i := range uint64(64) {
		b.Add(i, make([]byte, 512))
	}
	require.Equal(t, used, cachebudget.Global.Used())
	require.LessOrEqual(t, b.Len(), 8)
	b.Close()
	require.Equal(t, used, cachebudget.Global.Used())
}

func TestHashByteLRUMissesForeignHash(t *testing.T) {
	l := NewHashByteLRU(datasize.MB, func(v []byte) int64 { return int64(len(v)) })
	hash := common.Hash{1, 2, 3}
	l.Add(hash, []byte{1})
	_, ok := l.Get(hash)
	require.True(t, ok)

	foreign := hash
	foreign[31]++
	_, ok = l.Get(foreign)
	require.False(t, ok, "a hash sharing the 8-byte slot must miss")
}
