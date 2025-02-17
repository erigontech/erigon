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

package bridge

import "encoding/binary"

type ProcessedBlockInfo struct {
	BlockNum  uint64
	BlockTime uint64
}

func (info *ProcessedBlockInfo) MarshallBytes() (key []byte, value []byte) {
	key = make([]byte, 8)
	binary.BigEndian.PutUint64(key, info.BlockNum)
	value = make([]byte, 8)
	binary.BigEndian.PutUint64(value, info.BlockTime)
	return
}

func (info *ProcessedBlockInfo) UnmarshallBytes(key []byte, value []byte) {
	info.BlockNum = binary.BigEndian.Uint64(key)
	info.BlockTime = binary.BigEndian.Uint64(value)
}
