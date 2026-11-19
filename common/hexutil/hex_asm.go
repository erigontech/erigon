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

//go:build amd64 || arm64

package hexutil

import (
	"encoding/hex"
	"runtime"

	"golang.org/x/sys/cpu"
)

// maxAsmBytes bounds the input of one assembly call. Assembly is never async-preempted, so a
// collection waits for the call to return.
const maxAsmBytes = 64 << 10

// NEON is in the arm64 baseline.
var useAsm = runtime.GOARCH == "arm64" || cpu.X86.HasAVX2

// encodeBlocks hex-encodes blocks of 16 bytes.
//
//go:noescape
func encodeBlocks(dst, src *byte, blocks int)

// decodeBlocks decodes blocks of 32 characters, stopping before the first block that holds a
// non-hex character, and returns the number of characters decoded.
//
//go:noescape
func decodeBlocks(dst, src *byte, blocks int) (chars int)

// encodeHex is hex.Encode with whole 16-byte blocks done by AVX2 or NEON.
func encodeHex(dst, src []byte) {
	if useAsm {
		for len(src) >= 16 && len(dst) >= 32 {
			n := min(len(src), len(dst)/2, maxAsmBytes) &^ 15
			encodeBlocks(&dst[0], &src[0], n/16)
			src, dst = src[n:], dst[2*n:]
		}
	}
	hex.Encode(dst, src)
}

// decodeHex is hex.Decode with whole 32-character blocks done by AVX2 or NEON, by algorithm 3 of
// http://0x80.pl/notesen/2022-01-17-validating-hex-parse.html: a digit maps to 0-9 and a letter of
// either case to 10-15, anything else to more than 15 on both paths, so the smaller of the two is
// the nibble. A block holding a non-hex character is left to hex.Decode, which reports it.
func decodeHex(dst, src []byte) (int, error) {
	n := 0
	if useAsm {
		for len(src) >= 32 && len(dst) >= 16 {
			c := min(len(src), 2*len(dst), maxAsmBytes) &^ 31
			done := decodeBlocks(&dst[0], &src[0], c/32)
			src, dst, n = src[done:], dst[done/2:], n+done/2
			if done < c {
				break
			}
		}
	}
	m, err := hex.Decode(dst, src)
	return n + m, err
}
