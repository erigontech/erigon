#include "textflag.h"

// func fnvHash16AVX2(data, mix *uint32, prime uint32)
TEXT ·fnvHash16AVX2(SB), NOSPLIT, $0
	MOVQ data+0(FP), AX
	MOVQ mix+8(FP), BX
	MOVL prime+16(FP), X0

	VPBROADCASTD X0, Y0

	VPMULLD (AX), Y0, Y1
	VPXOR (BX), Y1, Y1
	VMOVDQU Y1, (AX)

	VPMULLD 32(AX), Y0, Y0
	VPXOR 32(BX), Y0, Y0
	VMOVDQU Y0, 32(AX)

	VZEROUPPER

	RET
