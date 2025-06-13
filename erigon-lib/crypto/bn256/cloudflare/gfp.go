package bn256

import (
	"errors"
	"fmt"
	"math/bits"
)

type gfP [4]uint64

func newGFp(x int64) (out *gfP) {
	if x >= 0 {
		out = &gfP{uint64(x)}
	} else {
		out = &gfP{uint64(-x)}
		gfpNeg(out, out)
	}

	montEncode(out, out)
	return out
}

func (e *gfP) String() string {
	return fmt.Sprintf("%16.16x%16.16x%16.16x%16.16x", e[3], e[2], e[1], e[0])
}

func (e *gfP) Set(f *gfP) {
	e[0] = f[0]
	e[1] = f[1]
	e[2] = f[2]
	e[3] = f[3]
}

func (z *gfP) Add(x, y *gfP) bool {
	var c uint64
	z[0], c = bits.Add64(x[0], y[0], 0)
	z[1], c = bits.Add64(x[1], y[1], c)
	z[2], c = bits.Add64(x[2], y[2], c)
	z[3], c = bits.Add64(x[3], y[3], c)
	return c != 0
}

func (z *gfP) Sub(x, y *gfP) bool {
	var c uint64
	z[0], c = bits.Sub64(x[0], y[0], 0)
	z[1], c = bits.Sub64(x[1], y[1], c)
	z[2], c = bits.Sub64(x[2], y[2], c)
	z[3], c = bits.Sub64(x[3], y[3], c)
	return c != 0
}

func (z *gfP) IsZero() bool {
	return (z[0] | z[1] | z[2] | z[3]) == 0
}

func (z *gfP) Div2() {
	z[0] = (z[1] << 63) | (z[0] >> 1)
	z[1] = (z[2] << 63) | (z[1] >> 1)
	z[2] = (z[3] << 63) | (z[2] >> 1)
	z[3] = z[3] >> 1
}

func (e *gfP) Invert(x *gfP) {
	// Use extended binary Euclidean algorithm. This evolves variables a and b until a is 0.
	// Then GCD(x, mod) is in b. If GCD(x, mod) == 1 then the inversion exists and is in v.
	// This follows the classic algorithm (Algorithm 1) presented in
	// "Optimized Binary GCD for Modular Inversion".
	// https://eprint.iacr.org/2020/972.pdf#algorithm.1
	// TODO: The same paper has additional optimizations that could be applied.
	a := *x
	b := P_words

	// Bézout's coefficients are originally initialized to 1 and 0. But because the input x
	// is in Montgomery form XR the algorithm would compute X⁻¹R⁻¹. To get the expected X⁻¹R,
	// we need to multiply the result by R². We can achieve the same effect "for free"
	// by initializing u to R² instead of 1.
	u := *r2
	v := gfP{0}

	zero := gfP{0}
	one := gfP{1}
	d := gfP{0}
	tmp := gfP{}

	for {
		if a.IsZero() {
			break
		}

		if a[0]&1 != 0 {
			less := d.Sub(&a, &b)

			if less {
				b = a
				a.Sub(&zero, &d)

				tmp = u
				u = v
				v = tmp
			} else {
				a = d
			}

			u.Sub(&u, &v)
		}

		a.Div2()
		u.Div2()

		if u[0]&1 != 0 {
			u.Add(&u, &inv2)
		}
	}

	if b != one {
		*e = zero
		return
	}

	*e = v
}

func (e *gfP) Marshal(out []byte) {
	for w := uint(0); w < 4; w++ {
		for b := uint(0); b < 8; b++ {
			out[8*w+b] = byte(e[3-w] >> (56 - 8*b))
		}
	}
}

func (e *gfP) Unmarshal(in []byte) error {
	// Unmarshal the bytes into little endian form
	for w := uint(0); w < 4; w++ {
		for b := uint(0); b < 8; b++ {
			e[3-w] += uint64(in[8*w+b]) << (56 - 8*b)
		}
	}
	// Ensure the point respects the curve modulus
	for i := 3; i >= 0; i-- {
		if e[i] < p2[i] {
			return nil
		}
		if e[i] > p2[i] {
			return errors.New("bn256: coordinate exceeds modulus")
		}
	}
	return errors.New("bn256: coordinate equals modulus")
}

func montEncode(c, a *gfP) { gfpMul(c, a, r2) }
func montDecode(c, a *gfP) { gfpMul(c, a, &gfP{1}) }
