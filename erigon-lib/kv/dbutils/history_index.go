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

package dbutils

import (
	"github.com/erigontech/erigon-lib/common/length"
)

func CompositeKeyWithoutIncarnation(key []byte) []byte {
	if len(key) == length.Hash*2+length.Incarnation {
		kk := make([]byte, length.Hash*2)
		copy(kk, key[:length.Hash])
		copy(kk[length.Hash:], key[length.Hash+length.Incarnation:])
		return kk
	}
	if len(key) == length.Addr+length.Hash+length.Incarnation {
		kk := make([]byte, length.Addr+length.Hash)
		copy(kk, key[:length.Addr])
		copy(kk[length.Addr:], key[length.Addr+length.Incarnation:])
		return kk
	}
	return key
}
