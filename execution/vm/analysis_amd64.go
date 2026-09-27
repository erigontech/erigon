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

import "golang.org/x/sys/cpu"

var hasSSE4 = cpu.X86.HasSSSE3 && cpu.X86.HasSSE41

// jumpdestBitmapSSE4 writes the JUMPDEST bits of the given number of whole 32-byte blocks of code to
// out (4 bytes per block) and returns the offset past the last block at which the next instruction
// starts (0..32). tab is a scratch table initialized from sse4TabInit.
//
//go:noescape
func jumpdestBitmapSSE4(code *byte, blocks int, tab *byte, out *byte) (entry int)
