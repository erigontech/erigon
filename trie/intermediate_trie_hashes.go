package trie

// CompressNibbles - supports only even number of nibbles
// This method supports only arrays of even nibbles
//
// HI_NIBBLE(b) = (b >> 4) & 0x0F
// LO_NIBBLE(b) = b & 0x0F
func CompressNibbles(nibbles []byte, out *[]byte) {
	tmp := (*out)[:0]
	for i := 0; i < len(nibbles); i += 2 {
		tmp = append(tmp, nibbles[i]<<4|nibbles[i+1])
	}
	*out = tmp
}

// DecompressNibbles - supports only even number of nibbles
//
// HI_NIBBLE(b) = (b >> 4) & 0x0F
// LO_NIBBLE(b) = b & 0x0F
func DecompressNibbles(in []byte, out *[]byte) {
	tmp := (*out)[:0]
	for i := 0; i < len(in); i++ {
		tmp = append(tmp, (in[i]>>4)&0x0F, in[i]&0x0F)
	}
	*out = tmp
}
