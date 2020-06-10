package ethash

/*
for i := 0; i < 64; i++ {
	a[i] ^= b[i]
}
*/

//go:noescape
func xor64BytesSSE2(a, b *byte)
