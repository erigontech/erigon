//go:build ignore

// gen_keccakx4.go generates keccakx4_amd64.s: Keccak-f[1600] on four independent states at
// once, one state per 64-bit lane of a YMM register. Lane i of message m lives at byte
// offset 32*i + 8*m of the state.
//
// Two variants share the absorb step:
//   - keccakX4AVX2 keeps the state in memory and ping-pongs with a scratch state, so it fits
//     in 16 YMM registers. Rotations are a shift pair and an OR.
//   - keccakX4AVX512 (AVX-512F+VL on YMM) keeps the state in Y0-Y24. rho+pi become a
//     renaming of registers done here, theta and chi use VPTERNLOGQ and rotations VPROLQ.
//     All 24 rounds are unrolled because the renaming differs per round.
//
// Usage: go run gen_keccakx4.go
package main

import (
	"bufio"
	"fmt"
	"os"
)

// rot[x][y] is the rho rotation of lane (x, y).
var rot = [5][5]int{
	{0, 36, 3, 41, 18},
	{1, 44, 10, 45, 2},
	{62, 6, 43, 15, 61},
	{28, 55, 25, 21, 56},
	{27, 20, 39, 8, 14},
}

var rc = [24]uint64{
	0x0000000000000001, 0x0000000000008082, 0x800000000000808A, 0x8000000080008000,
	0x000000000000808B, 0x0000000080000001, 0x8000000080008081, 0x8000000000008009,
	0x000000000000008A, 0x0000000000000088, 0x0000000080008009, 0x000000008000000A,
	0x000000008000808B, 0x800000000000008B, 0x8000000000008089, 0x8000000000008003,
	0x8000000000008002, 0x8000000000000080, 0x000000000000800A, 0x800000008000000A,
	0x8000000080008081, 0x8000000000008080, 0x0000000080000001, 0x8000000080008008,
}

var w *bufio.Writer

func emit(format string, args ...any) { fmt.Fprintf(w, "\t"+format+"\n", args...) }

// ternImm returns the VPTERNLOGQ immediate of f(a, b, c), a being the destination operand.
func ternImm(f func(a, b, c int) int) int {
	v := 0
	for i := range 8 {
		if f(i>>2&1, i>>1&1, i&1) == 1 {
			v |= 1 << i
		}
	}
	return v
}

// absorb XORs one 136-byte block of each of the four messages into the state at AX. The block
// pointers are in R8-R11; Y0-Y7 and X0-X3 are clobbered.
func absorb() {
	ptr := [4]string{"R8", "R9", "R10", "R11"}
	for k := 0; k < 16; k += 4 {
		for m := range 4 {
			emit("VMOVDQU %d(%s), Y%d", 8*k, ptr[m], m)
		}
		// Y4=[a0 b0 a2 b2] Y5=[a1 b1 a3 b3] Y6=[c0 d0 c2 d2] Y7=[c1 d1 c3 d3]
		emit("VPUNPCKLQDQ Y1, Y0, Y4")
		emit("VPUNPCKHQDQ Y1, Y0, Y5")
		emit("VPUNPCKLQDQ Y3, Y2, Y6")
		emit("VPUNPCKHQDQ Y3, Y2, Y7")
		emit("VPERM2I128 $0x20, Y6, Y4, Y0")
		emit("VPERM2I128 $0x20, Y7, Y5, Y1")
		emit("VPERM2I128 $0x31, Y6, Y4, Y2")
		emit("VPERM2I128 $0x31, Y7, Y5, Y3")
		for j := range 4 {
			emit("VPXOR %d(AX), Y%d, Y%d", 32*(k+j), j, j)
			emit("VMOVDQU Y%d, %d(AX)", j, 32*(k+j))
		}
	}
	// Word 16 is the last of the 17-word rate.
	emit("VMOVQ 128(R8), X0")
	emit("VPINSRQ $1, 128(R9), X0, X0")
	emit("VMOVQ 128(R10), X1")
	emit("VPINSRQ $1, 128(R11), X1, X1")
	emit("VINSERTI128 $1, X1, Y0, Y0")
	emit("VPXOR 512(AX), Y0, Y0")
	emit("VMOVDQU Y0, 512(AX)")
}

func loadPointers() {
	emit("MOVQ a+0(FP), AX")
	emit("MOVQ blocks+%d(FP), CX", 8)
	emit("MOVQ 0(CX), R8")
	emit("MOVQ 8(CX), R9")
	emit("MOVQ 16(CX), R10")
	emit("MOVQ 24(CX), R11")
}

func genAVX2() {
	fmt.Fprintln(w, "// func keccakX4AVX2(a *[100]uint64, blocks *[4]*byte, scratch *[100]uint64)")
	fmt.Fprintln(w, "TEXT ·keccakX4AVX2(SB), NOSPLIT, $0-24")
	loadPointers()
	absorb()
	emit("MOVQ scratch+16(FP), BX")
	emit("LEAQ rcx4<>(SB), DX")
	emit("MOVQ $11, CX")
	B := [5]string{"Y0", "Y1", "Y2", "Y3", "Y4"}
	D := [5]string{"Y5", "Y6", "Y7", "Y8", "Y9"}
	T := "Y10"
	C := [5]string{"Y11", "Y12", "Y13", "Y14", "Y15"}
	for x := range 5 {
		emit("VMOVDQU %d(AX), %s", 32*x, C[x])
		for y := 1; y < 5; y++ {
			emit("VPXOR %d(AX), %s, %s", 32*(x+5*y), C[x], C[x])
		}
	}
	round := func(src, dst string, rcOff int, last bool) {
		for x := range 5 {
			c1 := C[(x+1)%5]
			emit("VPSLLQ $1, %s, %s", c1, B[0])
			emit("VPSRLQ $63, %s, %s", c1, B[1])
			emit("VPOR %s, %s, %s", B[0], B[1], B[0])
			emit("VPXOR %s, %s, %s", B[0], C[(x+4)%5], D[x])
		}
		for Y := range 5 {
			for X := range 5 {
				xs, ys := (3*(Y-3*X)%5+5)%5, X
				r := rot[xs][ys]
				emit("VPXOR %d(%s), %s, %s", 32*(xs+5*ys), src, D[xs], B[X])
				if r != 0 {
					emit("VPSLLQ $%d, %s, %s", r, B[X], T)
					emit("VPSRLQ $%d, %s, %s", 64-r, B[X], B[X])
					emit("VPOR %s, %s, %s", T, B[X], B[X])
				}
			}
			for X := range 5 {
				emit("VPANDN %s, %s, %s", B[(X+2)%5], B[(X+1)%5], T)
				emit("VPXOR %s, %s, %s", B[X], T, T)
				if X == 0 && Y == 0 {
					emit("VPXOR %d(DX), %s, %s", rcOff, T, T)
				}
				emit("VMOVDQU %s, %d(%s)", T, 32*(X+5*Y), dst)
				if !last {
					if Y == 0 {
						emit("VMOVDQA %s, %s", T, C[X])
					} else {
						emit("VPXOR %s, %s, %s", T, C[X], C[X])
					}
				}
			}
		}
	}
	fmt.Fprintln(w, "avx2loop:")
	round("AX", "BX", 0, false)
	round("BX", "AX", 32, false)
	emit("ADDQ $64, DX")
	emit("DECQ CX")
	emit("JNZ avx2loop")
	round("AX", "BX", 0, false)
	round("BX", "AX", 32, true)
	emit("VZEROUPPER")
	emit("RET")
	fmt.Fprintln(w)
}

func genAVX512() {
	xor3 := ternImm(func(a, b, c int) int { return a ^ b ^ c })
	chi := ternImm(func(a, b, c int) int { return a ^ (1-b)&c })
	// Go operand order is imm, src3, src2, dst; dst is also the first input.
	tern := func(imm, c, b, a int) { emit("VPTERNLOGQ $0x%02x, Y%d, Y%d, Y%d", imm, c, b, a) }

	fmt.Fprintln(w, "// func keccakX4AVX512(a *[100]uint64, blocks *[4]*byte)")
	fmt.Fprintln(w, "TEXT ·keccakX4AVX512(SB), NOSPLIT, $0-16")
	loadPointers()
	absorb()
	emit("LEAQ rc<>(SB), DX")
	var m [25]int // logical lane -> register
	for i := range m {
		m[i] = i
		emit("VMOVDQU64 %d(AX), Y%d", 32*i, i)
	}
	C := [5]int{25, 26, 27, 28, 29}
	const T = 30
	for r := range 24 {
		// theta: A[x,y] ^= C[x-1] ^ rol(C[x+1], 1)
		for x := range 5 {
			emit("VMOVDQA64 Y%d, Y%d", m[x], C[x])
			tern(xor3, m[x+10], m[x+5], C[x])
			tern(xor3, m[x+20], m[x+15], C[x])
		}
		for x := range 5 {
			emit("VPROLQ $1, Y%d, Y%d", C[(x+1)%5], T)
			for y := range 5 {
				tern(xor3, T, C[(x+4)%5], m[x+5*y])
			}
		}
		// rho rotates in place; pi only renames: B[y, 2x+3y] = rol(A[x,y]).
		var nm [25]int
		for x := range 5 {
			for y := range 5 {
				p := m[x+5*y]
				if rot[x][y] != 0 {
					emit("VPROLQ $%d, Y%d, Y%d", rot[x][y], p, p)
				}
				nm[y+5*((2*x+3*y)%5)] = p
			}
		}
		m = nm
		// chi in place per row, with copies of the first two lanes in the free C registers.
		for y := range 5 {
			b := [5]int{m[5*y], m[5*y+1], m[5*y+2], m[5*y+3], m[5*y+4]}
			emit("VMOVDQA64 Y%d, Y%d", b[0], C[0])
			emit("VMOVDQA64 Y%d, Y%d", b[1], C[1])
			tern(chi, b[2], b[1], b[0])
			tern(chi, b[3], b[2], b[1])
			tern(chi, b[4], b[3], b[2])
			tern(chi, C[0], b[4], b[3])
			tern(chi, C[1], C[0], b[4])
		}
		emit("VPBROADCASTQ %d(DX), Y%d", 8*r, T)
		emit("VPXORQ Y%d, Y%d, Y%d", T, m[0], m[0])
	}
	for i := range m {
		emit("VMOVDQU64 Y%d, %d(AX)", m[i], 32*i)
	}
	emit("VZEROUPPER")
	emit("RET")
}

func main() {
	f, err := os.Create("keccakx4_amd64.s")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	w = bufio.NewWriter(f)
	defer w.Flush()

	fmt.Fprintln(w, "// Code generated by gen_keccakx4.go. DO NOT EDIT.")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "//go:build amd64 && !purego")
	fmt.Fprintln(w)
	fmt.Fprintln(w, `#include "textflag.h"`)
	fmt.Fprintln(w)
	for i, c := range rc {
		fmt.Fprintf(w, "DATA rc<>+%d(SB)/8, $0x%016x\n", 8*i, c)
	}
	fmt.Fprintln(w, "GLOBL rc<>(SB), RODATA|NOPTR, $192")
	fmt.Fprintln(w)
	for i, c := range rc {
		for l := range 4 {
			fmt.Fprintf(w, "DATA rcx4<>+%d(SB)/8, $0x%016x\n", 32*i+8*l, c)
		}
	}
	fmt.Fprintln(w, "GLOBL rcx4<>(SB), RODATA|NOPTR, $768")
	fmt.Fprintln(w)
	genAVX2()
	genAVX512()
}
