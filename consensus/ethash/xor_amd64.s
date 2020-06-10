#include "textflag.h"

// func xor64BytesSSE2(a, b *byte)
TEXT ·xor64BytesSSE2(SB), NOSPLIT, $0
	MOVQ  a+0(FP), AX
	MOVQ  b+8(FP), BX

// a[0:16] ^= b[0:16]
	MOVOU (AX), X0
	MOVOU (BX), X1
	PXOR  X1, X0
	MOVOU X0, (AX)

// a[16:32] ^= b[16:32]
	MOVOU 16(AX), X0
	MOVOU 16(BX), X1
	PXOR  X1, X0
	MOVOU X0, 16(AX)

// a[32:48] ^= b[32:48]
	MOVOU 32(AX), X0
	MOVOU 32(BX), X1
	PXOR  X1, X0
	MOVOU X0, 32(AX)

// a[48:64] ^= b[48:64]
	MOVOU 48(AX), X0
	MOVOU 48(BX), X1
	PXOR  X1, X0
	MOVOU X0, 48(AX)

	RET
