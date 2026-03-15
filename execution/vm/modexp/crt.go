package modexp

import "math/big"

// expEven computes base^exp mod m where m is even.
// Uses math/big for correctness. CRT decomposition can be added later
// as an optimization when we have assembly for the Montgomery path.
func expEven(base, e, mod []byte) []byte {
	b := new(big.Int).SetBytes(base)
	exp := new(big.Int).SetBytes(e)
	m := new(big.Int).SetBytes(mod)

	result := new(big.Int).Exp(b, exp, m)
	return leftPad(result.Bytes(), len(mod))
}

// leftPad zero-pads b on the left to length n.
func leftPad(b []byte, n int) []byte {
	if len(b) >= n {
		return b[len(b)-n:]
	}
	out := make([]byte, n)
	copy(out[n-len(b):], b)
	return out
}
