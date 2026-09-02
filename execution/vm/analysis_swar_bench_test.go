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

package vm

import (
	"testing"
)

// BenchmarkJumpdestAnalysisJumpdest24k mirrors the EIP-2780
// unique_code_jumpdest receiver: 24KiB of JUMPDEST with no PUSH data.
func BenchmarkJumpdestAnalysisJumpdest24k(b *testing.B) {
	code := make([]byte, 24576)
	for i := range code {
		code[i] = 0x5b
	}
	for b.Loop() {
		codeBitmap(code)
	}
}
