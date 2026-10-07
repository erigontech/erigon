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

//go:build amd64

package hexutil

import (
	"os/exec"
	"strings"
	"testing"
)

// TestNoInstructionBeyondTheFeatureCheck disassembles this package and fails on an instruction
// that needs more than the CPU feature the code checks for. archsimd names the feature of each
// operation in a doc comment only, so nothing else stops a method that needs AVX-512 from being
// called behind a check for AVX2, which is a SIGILL on a CPU that has one and not the other.
func TestNoInstructionBeyondTheFeatureCheck(t *testing.T) {
	bin := t.TempDir() + "/pkg.test"
	build := exec.Command("go", "test", "-c", "-o", bin, ".")
	build.Env = append(build.Environ(), "GOEXPERIMENT=simd")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building with GOEXPERIMENT=simd: %v: %s", err, out)
	}
	out, err := exec.Command("go", "tool", "objdump", "-s", "hexutil", bin).Output()
	if err != nil {
		t.Fatalf("objdump: %v", err)
	}
	// Every AVX-512 instruction is EVEX-encoded and, in 64-bit mode, an EVEX instruction always
	// begins with the byte 0x62, which nothing else does. Naming the instructions instead would
	// miss the AVX-512VL forms, which work on the same Y registers as AVX2.
	for _, line := range strings.Split(string(out), "\n") {
		if f := strings.Fields(line); len(f) > 3 && strings.HasPrefix(f[2], "62") {
			t.Errorf("AVX-512 (EVEX) instruction reached by a path that only checks AVX2:\n%s",
				strings.TrimSpace(line))
		}
	}
}
