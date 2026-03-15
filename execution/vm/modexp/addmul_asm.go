//go:build amd64

package modexp

import "golang.org/x/sys/cpu"

// supportADX is true when the CPU supports both ADX and BMI2 extensions,
// enabling the faster MULXQ/ADCXQ/ADOXQ assembly path.
var supportADX = cpu.X86.HasADX && cpu.X86.HasBMI2

// addMulVVW multiplies the multi-word value x by the single-word value y,
// adding the result to the multi-word value z and returning the final carry.
// len(x) must be >= len(z).
//
// Implemented in addmul_amd64.s with MULQ (baseline) and ADX/BMI2 (fast) paths.
//
//go:noescape
func addMulVVW(z, x []uint, y uint) (carry uint)
