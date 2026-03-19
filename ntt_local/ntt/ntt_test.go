package ntt

import (
	"encoding/binary"
	"math/big"
	"testing"
)

const (
	falconQ = 12289
	falconN = 512
)

func falconPsi() uint64 {
	q := big.NewInt(falconQ)
	g := big.NewInt(11)
	exp := new(big.Int).Div(new(big.Int).Sub(q, big.NewInt(1)), big.NewInt(2*falconN))
	return new(big.Int).Exp(g, exp, q).Uint64()
}

func falconParams(t *testing.T) *FastParams {
	t.Helper()
	p, err := NewFastParams(falconQ, falconN, falconPsi())
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestFastParamsCreate(t *testing.T) {
	p := falconParams(t)
	defer p.Close()

	if p.Q() != falconQ {
		t.Fatalf("Q = %d, want %d", p.Q(), falconQ)
	}
	if p.N() != falconN {
		t.Fatalf("N = %d, want %d", p.N(), falconN)
	}
	if p.CoeffBytes() != 2 {
		t.Fatalf("CoeffBytes = %d, want 2", p.CoeffBytes())
	}
}

func TestFastParamsInvalid(t *testing.T) {
	_, err := NewFastParams(0, 4, 1)
	if err == nil {
		t.Fatal("expected error for q=0")
	}

	_, err = NewFastParams(17, 3, 9)
	if err == nil {
		t.Fatal("expected error for non-power-of-2 n")
	}
}

func TestNttRoundtrip(t *testing.T) {
	p := falconParams(t)
	defer p.Close()

	a := make([]uint64, falconN)
	for i := range a {
		a[i] = uint64(i) % falconQ
	}

	fwd := p.Forward(a)
	inv := p.Inverse(fwd)

	for i := range a {
		if inv[i] != a[i] {
			t.Fatalf("roundtrip mismatch at %d: got %d, want %d", i, inv[i], a[i])
		}
	}
}

func TestPolynomialMultiply(t *testing.T) {
	p := falconParams(t)
	defer p.Close()

	q := uint64(falconQ)
	n := falconN

	f := make([]uint64, n)
	g := make([]uint64, n)
	for i := 0; i < n; i++ {
		f[i] = uint64(i*7+3) % q
		g[i] = uint64(i*13+5) % q
	}

	nttF := p.Forward(f)
	nttG := p.Forward(g)
	nttProd := VecMulMod(nttF, nttG, q)
	product := p.Inverse(nttProd)

	expected := schoolbookMul(f, g, q, n)
	for i := 0; i < n; i++ {
		if product[i] != expected[i] {
			t.Fatalf("polymul mismatch at %d: got %d, want %d", i, product[i], expected[i])
		}
	}
}

func schoolbookMul(f, g []uint64, q uint64, n int) []uint64 {
	result := make([]uint64, n)
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			c := (f[i] * g[j]) % q
			if i+j < n {
				result[i+j] = (result[i+j] + c) % q
			} else {
				idx := i + j - n
				result[idx] = (q + result[idx] - c) % q
			}
		}
	}
	return result
}

func TestVecOps(t *testing.T) {
	q := uint64(17)
	a := []uint64{5, 10, 15, 3}
	b := []uint64{4, 8, 3, 16}

	sum := VecAddMod(a, b, q)
	expectedSum := []uint64{9, 1, 1, 2}
	for i := range sum {
		if sum[i] != expectedSum[i] {
			t.Fatalf("VecAddMod[%d] = %d, want %d", i, sum[i], expectedSum[i])
		}
	}

	prod := VecMulMod(a, b, q)
	expectedProd := []uint64{3, 12, 11, 14}
	for i := range prod {
		if prod[i] != expectedProd[i] {
			t.Fatalf("VecMulMod[%d] = %d, want %d", i, prod[i], expectedProd[i])
		}
	}
}

func encodeWord(val uint64) []byte {
	buf := make([]byte, 32)
	binary.BigEndian.PutUint64(buf[24:], val)
	return buf
}

func encodeNttInput(q, psi uint64, n int, coeffs []uint64) []byte {
	coeffByteLen := (big.NewInt(int64(q)).BitLen() + 7) / 8

	var out []byte
	out = append(out, encodeWord(uint64(n))...)
	out = append(out, encodeWord(q)...)
	out = append(out, encodeWord(psi)...)

	for _, c := range coeffs {
		cb := make([]byte, coeffByteLen)
		v := big.NewInt(int64(c))
		vb := v.Bytes()
		copy(cb[coeffByteLen-len(vb):], vb)
		out = append(out, cb...)
	}

	return out
}

func TestPrecompileRoundtrip(t *testing.T) {
	q := uint64(17)
	psi := uint64(9)
	n := 4
	coeffs := []uint64{1, 2, 3, 4}

	input := encodeNttInput(q, psi, n, coeffs)
	fwdOut, err := NttFwPrecompile(input)
	if err != nil {
		t.Fatal(err)
	}

	nttCoeffs := make([]uint64, n)
	cb := 1
	for i := 0; i < n; i++ {
		nttCoeffs[i] = uint64(fwdOut[i*cb])
	}

	invInput := encodeNttInput(q, psi, n, nttCoeffs)
	invOut, err := NttInvPrecompile(invInput)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < n; i++ {
		got := uint64(invOut[i*cb])
		if got != coeffs[i] {
			t.Fatalf("roundtrip[%d] = %d, want %d", i, got, coeffs[i])
		}
	}
}

func TestPrecompileEmptyInput(t *testing.T) {
	_, err := NttFwPrecompile([]byte{})
	if err == nil {
		t.Fatal("expected error for empty input")
	}
}

func benchPrecompileInput() []byte {
	return encodeNttInput(falconQ, falconPsi(), falconN, func() []uint64 {
		a := make([]uint64, falconN)
		for i := range a {
			a[i] = uint64(i) % falconQ
		}
		return a
	}())
}

func BenchmarkNttFwPrecompile(b *testing.B) {
	input := benchPrecompileInput()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NttFwPrecompile(input)
	}
}

func BenchmarkNttInvPrecompile(b *testing.B) {
	input := benchPrecompileInput()
	fwdOut, _ := NttFwPrecompile(input)
	// re-encode forward output as inv input
	cb := (big.NewInt(falconQ).BitLen() + 7) / 8
	nttCoeffs := make([]uint64, falconN)
	for i := 0; i < falconN; i++ {
		v := new(big.Int).SetBytes(fwdOut[i*cb : (i+1)*cb])
		nttCoeffs[i] = v.Uint64()
	}
	invInput := encodeNttInput(falconQ, falconPsi(), falconN, nttCoeffs)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NttInvPrecompile(invInput)
	}
}

func encodeVecInput(q uint64, n int, a, b []uint64) []byte {
	qBytes := big.NewInt(int64(q)).Bytes()
	cb := (big.NewInt(int64(q)).BitLen() + 7) / 8

	var out []byte
	out = append(out, encodeWord(uint64(len(qBytes)))...)
	out = append(out, encodeWord(uint64(n))...)
	out = append(out, qBytes...)

	for _, vec := range [][]uint64{a, b} {
		for _, c := range vec {
			buf := make([]byte, cb)
			vb := big.NewInt(int64(c)).Bytes()
			copy(buf[cb-len(vb):], vb)
			out = append(out, buf...)
		}
	}
	return out
}

func benchVecPrecompileInput() []byte {
	a := make([]uint64, falconN)
	b := make([]uint64, falconN)
	for i := range a {
		a[i] = uint64(i*7+3) % falconQ
		b[i] = uint64(i*13+5) % falconQ
	}
	return encodeVecInput(falconQ, falconN, a, b)
}

func BenchmarkVecMulModPrecompile(b *testing.B) {
	input := benchVecPrecompileInput()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		VecMulModPrecompile(input)
	}
}

func BenchmarkVecAddModPrecompile(b *testing.B) {
	input := benchVecPrecompileInput()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		VecAddModPrecompile(input)
	}
}

func BenchmarkNttForward(b *testing.B) {
	p, _ := NewFastParams(falconQ, falconN, falconPsi())
	defer p.Close()

	a := make([]uint64, falconN)
	for i := range a {
		a[i] = uint64(i) % falconQ
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Forward(a)
	}
}

func BenchmarkNttRoundtrip(b *testing.B) {
	p, _ := NewFastParams(falconQ, falconN, falconPsi())
	defer p.Close()

	a := make([]uint64, falconN)
	for i := range a {
		a[i] = uint64(i) % falconQ
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fwd := p.Forward(a)
		p.Inverse(fwd)
	}
}

func BenchmarkPolymul(b *testing.B) {
	p, _ := NewFastParams(falconQ, falconN, falconPsi())
	defer p.Close()

	q := uint64(falconQ)
	f := make([]uint64, falconN)
	g := make([]uint64, falconN)
	for i := range f {
		f[i] = uint64(i*7) % q
		g[i] = uint64(i*13) % q
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nf := p.Forward(f)
		ng := p.Forward(g)
		prod := VecMulMod(nf, ng, q)
		p.Inverse(prod)
	}
}
