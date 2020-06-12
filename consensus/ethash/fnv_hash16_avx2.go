package ethash

//go:noescape
func fnvHash16AVX2(data, mix *uint32, prime uint32)

/*
for i := 0; i < 16; i++ {
	data[i] = data[i] * prime ^ mix[i]
}
*/
