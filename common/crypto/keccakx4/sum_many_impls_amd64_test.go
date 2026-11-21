//go:build amd64 && !purego

package keccakx4

import "golang.org/x/sys/cpu"

func availableX4() map[string]func(*[100]uint64, *[4]*byte) {
	impls := map[string]func(*[100]uint64, *[4]*byte){}
	if cpu.X86.HasAVX2 {
		impls["avx2"] = keccakX4AVX2Scratch
	}
	if cpu.X86.HasAVX512F && cpu.X86.HasAVX512VL {
		impls["avx512"] = keccakX4AVX512
	}
	return impls
}
