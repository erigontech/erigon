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

//go:build go1.27 && goexperiment.simd && amd64

package vm

import "simd/archsimd"

var hasVBMI = archsimd.X86.AVX512VBMI()

var liftLane, liftNext = func() (lane, next [64]uint8) {
	for i := range lane {
		lane[i], next[i] = uint8(i), uint8(i+1)
	}
	return lane, next
}()

// codeBitmapLift resolves each 64-byte chunk on its own, then fixes up the entry. In a chunk every
// byte x gets J(x) = x+1+pushlen, capped at 64 for "past the chunk". Doubling builds J^1..J^32 with
// 64-lane byte permutes, and binary lifting over them finds for every byte the first instruction
// start at or past it on the chain from offset 0; bytes that are their own such start are starts.
// Chunks do not depend on each other. The real entry, where the previous chunk's last push data
// ends, joins the chain from offset 0 within a few instructions; a scalar walk covers the bytes
// before it does.
func codeBitmapLift(code []byte) bitvec {
	if !hasVBMI {
		return codeBitmap(code)
	}
	out := make(bitvec, (len(code)+63)/64)
	lane := archsimd.LoadUint8x64Array(&liftLane)
	next := archsimd.LoadUint8x64Array(&liftNext)
	sink := archsimd.BroadcastUint8x64(64)
	c5f := archsimd.BroadcastUint8x64(0x5f)
	c5b := archsimd.BroadcastUint8x64(0x5b)
	e, w := 0, 0
	for ; len(code) >= 64; w, code = w+1, code[64:] {
		chunk := (*[64]byte)(code)
		c := archsimd.LoadUint8x64Array(chunk)
		pushLen := c.AsInt8x64().Max(c5f.AsInt8x64()).AsUint8x64().Sub(c5f)
		p0 := next.Add(pushLen).Min(sink)
		p1 := p0.ConcatPermute(sink, p0)
		p2 := p1.ConcatPermute(sink, p1)
		p3 := p2.ConcatPermute(sink, p2)
		p4 := p3.ConcatPermute(sink, p3)
		p5 := p4.ConcatPermute(sink, p4)
		entry := archsimd.BroadcastUint8x64(uint8(e))
		cur := lift(p0, p1, p2, p3, p4, p5, sink, lane, entry)
		isStart := p0.ConcatPermute(sink, cur).Equal(lane).Or(lane.Equal(entry))
		starts := isStart.ToBits()
		last := lane.IfElse(isStart, cur).GetHi().GetHi().GetElem(15) // the last start in the chunk
		exit := int(last) + 1 + int(pushLenOf(chunk[last&63]))
		out[w] = starts & c.Equal(c5b).ToBits()
		e = exit - 64
	}
	for pc := e; pc < len(code); pc++ {
		if op := OpCode(code[pc]); int8(op) >= int8(PUSH1) {
			pc += int(op - PUSH1 + 1)
		} else if op == JUMPDEST {
			out[w] |= 1 << uint(pc)
		}
	}
	return out
}

func pushLenOf(op byte) uint8 {
	if int8(op) >= int8(PUSH1) {
		return op - byte(PUSH1) + 1
	}
	return 0
}

// lift moves each lane from its position in cur along the chain of instruction starts, by
// J^32 down to J^1, to the last start below the lane.
func lift(p0, p1, p2, p3, p4, p5, sink, lane, cur archsimd.Uint8x64) archsimd.Uint8x64 {
	cur = liftStep(p5, sink, lane, cur)
	cur = liftStep(p4, sink, lane, cur)
	cur = liftStep(p3, sink, lane, cur)
	cur = liftStep(p2, sink, lane, cur)
	cur = liftStep(p1, sink, lane, cur)
	return liftStep(p0, sink, lane, cur)
}

func liftStep(p, sink, lane, cur archsimd.Uint8x64) archsimd.Uint8x64 {
	nxt := p.ConcatPermute(sink, cur)
	return nxt.IfElse(nxt.Less(lane), cur)
}
