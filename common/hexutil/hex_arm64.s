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
GLOBL hexDigits<>(SB), RODATA|NOPTR, $16

// The 128-byte frames keep the stack check; see hex_amd64.s.

// func encodeBlocks(dst, src *byte, blocks int)
TEXT ·encodeBlocks(SB), 0, $128-24
	MOVD dst+0(FP), R0
	MOVD src+8(FP), R1
	MOVD blocks+16(FP), R2
	MOVD $hexDigits<>(SB), R3
	VLD1 (R3), [V4.B16]
	VMOVI $15, V5.B16

loop:
	VLD1.P 16(R1), [V0.B16]
	VUSHR $4, V0.B16, V1.B16
	VAND V5.B16, V0.B16, V0.B16
	VTBL V1.B16, [V4.B16], V1.B16
	VTBL V0.B16, [V4.B16], V0.B16
	VZIP1 V0.B16, V1.B16, V2.B16
	VZIP2 V0.B16, V1.B16, V3.B16
	VST1.P [V2.B16, V3.B16], 32(R0)
	SUBS $1, R2, R2
	BNE loop
	RET

// Go 1.26 assembles neither UQSUB, UQADD nor UMAXV: saturation is UMAX/UMIN around SUB/ADD, and
// a block is valid when no nibble has a bit above the low four.
//
// func decodeBlocks(dst, src *byte, blocks int) (chars int)
TEXT ·decodeBlocks(SB), 0, $128-32
	MOVD dst+0(FP), R0
	MOVD src+8(FP), R1
	MOVD blocks+16(FP), R2
	MOVD $0, R5
	VMOVI $0xc6, V8.B16
	VMOVI $6, V9.B16
	VMOVI $0xf0, V10.B16
	VMOVI $0xdf, V11.B16
	VMOVI $0x41, V12.B16
	VMOVI $10, V13.B16
	VMOVI $245, V14.B16

loop:
	VLD1.P 32(R1), [V0.B16, V1.B16]
	VADD V8.B16, V0.B16, V2.B16
	VUMAX V9.B16, V2.B16, V2.B16
	VSUB V9.B16, V2.B16, V2.B16
	VSUB V10.B16, V2.B16, V2.B16
	VAND V11.B16, V0.B16, V3.B16
	VSUB V12.B16, V3.B16, V3.B16
	VUMIN V14.B16, V3.B16, V3.B16
	VADD V13.B16, V3.B16, V3.B16
	VUMIN V3.B16, V2.B16, V2.B16
	VADD V8.B16, V1.B16, V6.B16
	VUMAX V9.B16, V6.B16, V6.B16
	VSUB V9.B16, V6.B16, V6.B16
	VSUB V10.B16, V6.B16, V6.B16
	VAND V11.B16, V1.B16, V7.B16
	VSUB V12.B16, V7.B16, V7.B16
	VUMIN V14.B16, V7.B16, V7.B16
	VADD V13.B16, V7.B16, V7.B16
	VUMIN V7.B16, V6.B16, V6.B16
	VUMAX V6.B16, V2.B16, V3.B16
	VUSHR $4, V3.B16, V3.B16
	VUADDLV V3.B16, V3
	VMOV V3.H[0], R4
	CBNZ R4, done
	VUZP1 V6.B16, V2.B16, V3.B16
	VUZP2 V6.B16, V2.B16, V7.B16
	VSHL $4, V3.B16, V3.B16
	VORR V7.B16, V3.B16, V3.B16
	VST1.P [V3.B16], 16(R0)
	ADD $32, R5
	SUBS $1, R2, R2
	BNE loop

done:
	MOVD R5, chars+24(FP)
	RET
