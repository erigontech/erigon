//go:build amd64

#include "textflag.h"

// func addMulVVW(z, x []uint, y uint) (carry uint)
//
// Computes z[i] += x[i] * y for each i in [0, len(z)), propagating carry.
// Returns the final carry word.
//
// Two code paths:
//   - MULQ: baseline amd64, 4x unrolled
//   - ADX/BMI2: MULXQ + ADCXQ + ADOXQ with dual carry chains, 4x unrolled
//
// Stack layout (ABI0):
//   z_base+0(FP)  z_len+8(FP)  z_cap+16(FP)
//   x_base+24(FP) x_len+32(FP) x_cap+40(FP)
//   y+48(FP)
//   carry+56(FP) [return]
TEXT ·addMulVVW(SB), NOSPLIT, $0-64
	CMPB ·supportADX(SB), $1
	JEQ  adx

	// ==========================================
	// MULQ path (baseline amd64)
	// ==========================================
	MOVQ z_len+8(FP), CX     // CX = n = len(z)
	TESTQ CX, CX
	JZ   mulq_done
	MOVQ z_base+0(FP), DI    // DI = z pointer
	MOVQ x_base+24(FP), SI   // SI = x pointer
	MOVQ y+48(FP), BX        // BX = y (multiplier)
	XORQ R8, R8              // R8 = carry = 0

	// Compute unrolled loop lengths
	MOVQ CX, R9
	ANDQ $3, R9              // R9 = n % 4 (remainder)
	SHRQ $2, CX              // CX = n / 4

	// 1x loop for remainder
	TESTQ R9, R9
	JZ   mulq_loop4_start
mulq_loop1:
	MOVQ (SI), AX
	MULQ BX                  // DX:AX = x[i] * y
	ADDQ R8, AX              // AX += carry
	ADCQ $0, DX
	ADDQ (DI), AX            // AX += z[i]
	ADCQ $0, DX
	MOVQ AX, (DI)            // z[i] = lo
	MOVQ DX, R8              // carry = hi
	LEAQ 8(SI), SI
	LEAQ 8(DI), DI
	SUBQ $1, R9
	JNZ  mulq_loop1

mulq_loop4_start:
	TESTQ CX, CX
	JZ   mulq_done
mulq_loop4:
	// Iteration 0
	MOVQ 0(SI), AX
	MULQ BX
	ADDQ R8, AX
	ADCQ $0, DX
	ADDQ 0(DI), AX
	ADCQ $0, DX
	MOVQ AX, 0(DI)
	MOVQ DX, R8
	// Iteration 1
	MOVQ 8(SI), AX
	MULQ BX
	ADDQ R8, AX
	ADCQ $0, DX
	ADDQ 8(DI), AX
	ADCQ $0, DX
	MOVQ AX, 8(DI)
	MOVQ DX, R8
	// Iteration 2
	MOVQ 16(SI), AX
	MULQ BX
	ADDQ R8, AX
	ADCQ $0, DX
	ADDQ 16(DI), AX
	ADCQ $0, DX
	MOVQ AX, 16(DI)
	MOVQ DX, R8
	// Iteration 3
	MOVQ 24(SI), AX
	MULQ BX
	ADDQ R8, AX
	ADCQ $0, DX
	ADDQ 24(DI), AX
	ADCQ $0, DX
	MOVQ AX, 24(DI)
	MOVQ DX, R8

	LEAQ 32(SI), SI
	LEAQ 32(DI), DI
	SUBQ $1, CX
	JNZ  mulq_loop4

mulq_done:
	MOVQ R8, carry+56(FP)
	RET

	// ==========================================
	// ADX/BMI2 path (MULXQ + ADCXQ + ADOXQ)
	// ==========================================
adx:
	MOVQ z_len+8(FP), CX     // CX = n = len(z)
	TESTQ CX, CX
	JZ   adx_done
	MOVQ z_base+0(FP), R8    // R8 = z pointer
	MOVQ x_base+24(FP), R9   // R9 = x pointer
	MOVQ y+48(FP), DX        // DX = y (implicit MULXQ source)
	XORQ BX, BX              // BX = carry = 0
	XORQ DI, DI              // DI = zero constant

	// Compute unrolled loop lengths
	MOVQ CX, R10
	ANDQ $7, R10             // R10 = n % 8 (remainder)
	SHRQ $3, CX              // CX = n / 8

	// 1x loop for remainder
	TESTQ R10, R10
	JZ   adx_loop8_start
adx_loop1:
	// Clear CF and OF (SUBQ at end of loop clobbers flags)
	XORQ AX, AX
	MULXQ 0(R9), R13, R12    // R13:R12 = x[i] * y
	ADCXQ BX, R13            // R13 += carry (via CF)
	ADOXQ 0(R8), R13         // R13 += z[i] (via OF)
	MOVQ  R13, 0(R8)         // z[i] = R13
	MOVQ  R12, BX            // carry = hi
	ADCXQ DI, BX             // capture final CF into carry
	ADOXQ DI, BX             // capture final OF into carry
	LEAQ 8(R9), R9
	LEAQ 8(R8), R8
	SUBQ $1, R10
	JNZ  adx_loop1

adx_loop8_start:
	TESTQ CX, CX
	JZ   adx_done
adx_loop8:
	// Clear CF and OF
	XORQ AX, AX
	// Iterations 0-1: paired MULXQ with interleaved carry chains
	MULXQ 0(R9), R13, R11    // R13 = lo0, R11 = hi0
	ADCXQ BX, R13            // R13 += carry_in (CF)
	ADOXQ 0(R8), R13         // R13 += z[0] (OF)
	MULXQ 8(R9), R14, BX     // R14 = lo1, BX = hi1
	ADCXQ R11, R14           // R14 += hi0 (CF)
	ADOXQ 8(R8), R14         // R14 += z[1] (OF)
	MOVQ  R13, 0(R8)
	MOVQ  R14, 8(R8)
	// Iterations 2-3
	MULXQ 16(R9), R13, R11
	ADCXQ BX, R13
	ADOXQ 16(R8), R13
	MULXQ 24(R9), R14, BX
	ADCXQ R11, R14
	ADOXQ 24(R8), R14
	MOVQ  R13, 16(R8)
	MOVQ  R14, 24(R8)
	// Iterations 4-5
	MULXQ 32(R9), R13, R11
	ADCXQ BX, R13
	ADOXQ 32(R8), R13
	MULXQ 40(R9), R14, BX
	ADCXQ R11, R14
	ADOXQ 40(R8), R14
	MOVQ  R13, 32(R8)
	MOVQ  R14, 40(R8)
	// Iterations 6-7
	MULXQ 48(R9), R13, R11
	ADCXQ BX, R13
	ADOXQ 48(R8), R13
	MULXQ 56(R9), R14, BX
	ADCXQ R11, R14
	ADOXQ 56(R8), R14
	MOVQ  R13, 48(R8)
	MOVQ  R14, 56(R8)
	// Capture remaining carries from both chains
	ADCXQ DI, BX             // BX += 0 + CF
	ADOXQ DI, BX             // BX += 0 + OF

	LEAQ 64(R9), R9
	LEAQ 64(R8), R8
	SUBQ $1, CX
	JNZ  adx_loop8

adx_done:
	MOVQ BX, carry+56(FP)
	RET
