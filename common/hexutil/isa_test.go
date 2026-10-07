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
	"regexp"
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
	// A zmm or mask register, or one of the instructions that AVX-512 introduced. The names are
	// spelled out because the AVX2 set has near misses: VPMOVMSKB and VPMOVZXBW are not AVX-512.
	forbidden := regexp.MustCompile(`\b(Z[0-9]+|K[1-7])\b|\b(VPMOV(S|US)?(WB|DB|QB|DW|QW|QD)|VPERM[BW]|VPCOMPRESS[BWDQ]|VPEXPAND[BWDQ]|VPTERNLOG[DQ])\b`)
	for _, line := range strings.Split(string(out), "\n") {
		if m := forbidden.FindString(line); m != "" {
			t.Errorf("AVX-512 %q reached by a path that only checks AVX2:\n%s", m, strings.TrimSpace(line))
		}
	}
}
