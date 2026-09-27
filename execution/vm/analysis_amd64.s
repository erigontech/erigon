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

#include "textflag.h"

// Branch-free JUMPDEST analysis of 32-byte blocks (SSSE3 + SSE4.1).
//
// Each 8-byte half of a 16-byte group is solved for every entry offset at once: next[i] = i + 1 + pushlen,
// followed by 3 pointer-jumping rounds with PSHUFB (x = x[x], v |= v[x]). A pointer that leaves its
// half points to itself. v collects the instruction starts visited from each entry. Small tables,
// indexed by the entry offset e (stored as 0x80 + e), are then written per block, and a scalar chain
// follows the real entry through them: one dependent load per block. The instruction starts are
// masked with the JUMPDEST bytes. The cost does not depend on the code bytes.
//
// Table layout (tab, 384 bytes, tails pre-filled by the caller):
//   TA   0: 0x80 + entry into bytes 16..31, by block entry
//   PT  48: 0x80 + entry into the next block, by block entry
//   T0  96: 0x78 + entry into bytes 8..15, by entry into bytes 0..7
//   T2 144: 0x78 + entry into bytes 24..31, by entry into bytes 16..23
//   V0 192, V1 296, V2 240, V3 344: instruction-start bits of each 8-byte half, by entry

DATA jdaIota<>+0(SB)/8, $0x21201f1e1d1c1b1a
DATA jdaIota<>+8(SB)/8, $0x21201f1e1d1c1b1a
GLOBL jdaIota<>(SB), RODATA|NOPTR, $16
DATA jdaK8<>+0(SB)/8, $0x0808080808080808
DATA jdaK8<>+8(SB)/8, $0x0000000000000000
GLOBL jdaK8<>(SB), RODATA|NOPTR, $16
DATA jdaLane<>+0(SB)/8, $0x0706050403020100
DATA jdaLane<>+8(SB)/8, $0x0f0e0d0c0b0a0908
GLOBL jdaLane<>(SB), RODATA|NOPTR, $16
DATA jdaBit<>+0(SB)/8, $0x8040201008040201
DATA jdaBit<>+8(SB)/8, $0x8040201008040201
GLOBL jdaBit<>(SB), RODATA|NOPTR, $16
DATA jda5f<>+0(SB)/8, $0x5f5f5f5f5f5f5f5f
DATA jda5f<>+8(SB)/8, $0x5f5f5f5f5f5f5f5f
GLOBL jda5f<>(SB), RODATA|NOPTR, $16
DATA jda10<>+0(SB)/8, $0x1010101010101010
DATA jda10<>+8(SB)/8, $0x1010101010101010
GLOBL jda10<>(SB), RODATA|NOPTR, $16
DATA jda5b<>+0(SB)/8, $0x5b5b5b5b5b5b5b5b
DATA jda5b<>+8(SB)/8, $0x5b5b5b5b5b5b5b5b
GLOBL jda5b<>(SB), RODATA|NOPTR, $16

// HALF computes, for the 16 code bytes in c, per entry lane: xm = 0x80 + entry into the next
// 16-byte group, n = the exit of the lane's half, v = the instruction starts visited.
#define HALF(c, xm, n, v, t) \
	PMAXSB X11, c \  // max(c, 0x5f): 0x5f + pushlen
	PADDB  X15, c \  // y = 0x78 + next start relative to the half
	MOVOU  c, n \
	PSUBB  X14, n \  // 0x70 + next start in the group
	PXOR   X14, c \
	PMAXSB X13, c \  // x: in-half pointer, or the lane itself (a sink)
	MOVOU  X12, v \
	MOVOU  v, t \
	PSHUFB c, t \
	POR    t, v \
	PSHUFB c, c \
	MOVOU  v, t \
	PSHUFB c, t \
	POR    t, v \
	PSHUFB c, c \
	MOVOU  v, t \
	PSHUFB c, t \
	POR    t, v \
	PSHUFB c, c \
	PSHUFB c, n \    // exit of the half
	MOVOU  n, xm \
	PSHUFB n, xm \
	PMAXUB n, xm     // 1st-half exits inside the 2nd half take its exit

// func jumpdestBitmapSSE4(code *byte, blocks int, tab *byte, out *byte) (entry int)
TEXT ·jumpdestBitmapSSE4(SB), NOSPLIT, $0-40
	MOVQ code+0(FP), SI
	MOVQ blocks+8(FP), CX
	MOVQ tab+16(FP), BX
	MOVQ out+24(FP), DI
	MOVOU jda10<>(SB), X10
	MOVOU jda5f<>(SB), X11
	MOVOU jdaBit<>(SB), X12
	MOVOU jdaLane<>(SB), X13
	MOVOU jdaK8<>(SB), X14
	MOVOU jdaIota<>(SB), X15
	MOVQ $0x80, R8 // e = 0x80 + entry offset

loop:
	MOVOU 0(SI), X4
	MOVOU X4, X0
	PCMPEQB jda5b<>(SB), X0
	PMOVMSKB X0, R12
	HALF(X4, X1, X2, X3, X5)
	MOVOU 16(SI), X9
	MOVOU X9, X0
	PCMPEQB jda5b<>(SB), X0
	PMOVMSKB X0, R13
	SHLL $16, R13
	ORL  R13, R12               // JUMPDEST bytes
	HALF(X9, X6, X7, X8, X5)

	MOVOU X1, 0(BX)   // TA
	MOVOU X6, 64(BX)  // PT[16..32): entry into the next block from bytes 16..31
	MOVOU X1, X9
	PSUBB X10, X9     // entry into bytes 16..31, as an index
	MOVOU X6, X0
	PSHUFB X9, X0
	PMAXUB X9, X0
	MOVOU X0, 48(BX)  // PT[0..16)
	MOVQ  X2, 96(BX)  // T0
	MOVQ  X7, 144(BX) // T2
	MOVQ  X3, 192(BX) // V0
	MOVQ  X8, 240(BX) // V2
	MOVOU X3, 288(BX) // V1 (at 296)
	MOVOU X8, 336(BX) // V3 (at 344)

	MOVBQZX -32(BX)(R8*1), R9   // e1 = T0[e]
	MOVBQZX -128(BX)(R8*1), R10 // e2 = TA[e]
	MOVBQZX 16(BX)(R10*1), R11  // e3 = T2[e2]
	MOVBLZX 64(BX)(R8*1), AX    // V0[e]
	MOVBLZX 176(BX)(R9*1), DX   // V1[e1]
	SHLL $8, DX
	ORL  DX, AX
	MOVBLZX 112(BX)(R10*1), DX  // V2[e2]
	SHLL $16, DX
	ORL  DX, AX
	MOVBLZX 224(BX)(R11*1), DX  // V3[e3]
	SHLL $24, DX
	ORL  DX, AX
	ANDL R12, AX
	MOVL AX, 0(DI)
	MOVBQZX -80(BX)(R8*1), R8   // e = PT[e]

	ADDQ $32, SI
	ADDQ $4, DI
	DECQ CX
	JNZ  loop

	SUBQ $0x80, R8
	MOVQ R8, entry+32(FP)
	RET
