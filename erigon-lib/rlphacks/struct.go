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

package rlphacks

func GenerateStructLen(buffer []byte, l uint64) int {
	if l < 56 {
		buffer[0] = byte(192 + l)
		return 1
	}
	if l < 256 {
		// l can be encoded as 1 byte
		buffer[1] = byte(l)
		buffer[0] = byte(247 + 1)
		return 2
	}
	if l < 65536 {
		buffer[2] = byte(l & 255)
		buffer[1] = byte(l >> 8)
		buffer[0] = byte(247 + 2)
		return 3
	}
	if l < (1 << 24) {
		buffer[3] = byte(l & 255)
		buffer[2] = byte((l >> 8) & 255)
		buffer[1] = byte(l >> 16)
		buffer[0] = byte(247 + 3)
		return 4
	}
	if l < (1 << 32) {
		buffer[4] = byte(l & 255)
		buffer[3] = byte((l >> 8) & 255)
		buffer[2] = byte((l >> 16) & 255)
		buffer[1] = byte(l >> 24)
		buffer[0] = byte(247 + 4)
		return 5
	}
	if l < (1 << 40) {
		buffer[5] = byte(l & 255)
		buffer[4] = byte((l >> 8) & 255)
		buffer[3] = byte((l >> 16) & 255)
		buffer[2] = byte((l >> 24) & 255)
		buffer[1] = byte(l >> 32)
		buffer[0] = byte(247 + 5)
		return 6
	}
	if l < (1 << 48) {
		buffer[6] = byte(l & 255)
		buffer[5] = byte((l >> 8) & 255)
		buffer[4] = byte((l >> 16) & 255)
		buffer[3] = byte((l >> 24) & 255)
		buffer[2] = byte((l >> 32) & 255)
		buffer[1] = byte(l >> 40)
		buffer[0] = byte(247 + 6)
		return 7
	}
	if l < (1 << 56) {
		buffer[7] = byte(l & 255)
		buffer[6] = byte((l >> 8) & 255)
		buffer[5] = byte((l >> 16) & 255)
		buffer[4] = byte((l >> 24) & 255)
		buffer[3] = byte((l >> 32) & 255)
		buffer[2] = byte((l >> 40) & 255)
		buffer[1] = byte(l >> 48)
		buffer[0] = byte(247 + 7)
		return 8
	}
	buffer[8] = byte(l & 255)
	buffer[7] = byte((l >> 8) & 255)
	buffer[6] = byte((l >> 16) & 255)
	buffer[5] = byte((l >> 24) & 255)
	buffer[4] = byte((l >> 32) & 255)
	buffer[3] = byte((l >> 40) & 255)
	buffer[2] = byte((l >> 48) & 255)
	buffer[1] = byte(l >> 56)
	buffer[0] = byte(247 + 8)
	return 9
}
