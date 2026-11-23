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

DATA hexDigits<>+0(SB)/8, $"01234567"
DATA hexDigits<>+8(SB)/8, $"89abcdef"
DATA hexDigits<>+16(SB)/8, $"01234567"
DATA hexDigits<>+24(SB)/8, $"89abcdef"
GLOBL hexDigits<>(SB), RODATA|NOPTR, $32

// 0xc6, 6, 0xf0, 0xdf, 'A', 10, 112, 0x0f
DATA decodeBytes<>+0(SB)/8, $0x0f700a41dff006c6
GLOBL decodeBytes<>(SB), RODATA|NOPTR, $8

DATA pairWeights<>+0(SB)/8, $0x0110011001100110
DATA pairWeights<>+8(SB)/8, $0x0110011001100110
DATA pairWeights<>+16(SB)/8, $0x0110011001100110
DATA pairWeights<>+24(SB)/8, $0x0110011001100110
GLOBL pairWeights<>(SB), RODATA|NOPTR, $32

// The 128-byte frames are unused. With a smaller frame the assembler drops the stack check of a
// leaf function, and that check is where a pending preemption stops the goroutine between the
// chunks of hex_asm.go.

// func encodeBlocks(dst, src *byte, blocks int)
TEXT ·encodeBlocks(SB), 0, $128-24
	MOVQ dst+0(FP), DI
	MOVQ src+8(FP), SI
	MOVQ blocks+16(FP), CX
	VMOVDQU hexDigits<>(SB), Y4
	VPBROADCASTB decodeBytes<>+7(SB), Y5
	VPMOVZXBW X5, Y5

loop:
	VPMOVZXBW (SI), Y0
	VPSRLW $4, Y0, Y1
	VPAND Y5, Y0, Y0
	VPSLLW $8, Y0, Y0
	VPOR Y1, Y0, Y0
	VPSHUFB Y0, Y4, Y0
	VMOVDQU Y0, (DI)
	ADDQ $16, SI
	ADDQ $32, DI
	DECQ CX
	JNZ loop
	VZEROUPPER
	RET

// func decodeBlocks(dst, src *byte, blocks int) (chars int)
TEXT ·decodeBlocks(SB), 0, $128-32
	MOVQ dst+0(FP), DI
	MOVQ src+8(FP), SI
	MOVQ blocks+16(FP), CX
	XORQ AX, AX
	VPBROADCASTB decodeBytes<>+0(SB), Y8
	VPBROADCASTB decodeBytes<>+1(SB), Y9
	VPBROADCASTB decodeBytes<>+2(SB), Y10
	VPBROADCASTB decodeBytes<>+3(SB), Y11
	VPBROADCASTB decodeBytes<>+4(SB), Y12
	VPBROADCASTB decodeBytes<>+5(SB), Y13
	VPBROADCASTB decodeBytes<>+6(SB), Y14
	VMOVDQU pairWeights<>(SB), Y15

loop:
	VMOVDQU (SI)(AX*1), Y0
	VPADDB Y8, Y0, Y1
	VPSUBUSB Y9, Y1, Y1
	VPSUBB Y10, Y1, Y1
	VPAND Y11, Y0, Y2
	VPSUBB Y12, Y2, Y2
	VPADDUSB Y13, Y2, Y2
	VPMINUB Y2, Y1, Y1
	VPADDUSB Y14, Y1, Y2
	VPMOVMSKB Y2, BX
	TESTL BX, BX
	JNZ done
	VPMADDUBSW Y15, Y1, Y1
	VPACKUSWB Y1, Y1, Y1
	VPERMQ $0x08, Y1, Y1
	VMOVDQU X1, (DI)
	ADDQ $16, DI
	ADDQ $32, AX
	DECQ CX
	JNZ loop

done:
	VZEROUPPER
	MOVQ AX, chars+24(FP)
	RET
