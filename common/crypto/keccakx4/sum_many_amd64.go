//go:build amd64 && !purego

package keccakx4

import "golang.org/x/sys/cpu"

//go:noescape
func keccakX4AVX2(a *[100]uint64, blocks *[4]*byte, scratch *[100]uint64)

//go:noescape
func keccakX4AVX512(a *[100]uint64, blocks *[4]*byte)

func keccakX4AVX2Scratch(a *[100]uint64, blocks *[4]*byte) {
	var scratch [100]uint64
	keccakX4AVX2(a, blocks, &scratch)
}

func init() {
	switch {
	case cpu.X86.HasAVX512F && cpu.X86.HasAVX512VL:
		x4 = keccakX4AVX512
	case cpu.X86.HasAVX2:
		x4 = keccakX4AVX2Scratch
	}
	if x4 != nil && !x4SelfTest(x4) {
		x4 = nil
	}
}
