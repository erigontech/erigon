package dbutils

// NextSubtree does []byte++. Returns false if overflow.
func NextSubtree(in []byte) ([]byte, bool) {
	r := make([]byte, len(in))
	copy(r, in)
	for i := len(r) - 1; i >= 0; i-- {
		if r[i] != 255 {
			r[i]++
			return r, true
		}

		r = r[:i] // make it shorter, because in tries after 11ff goes 12, but not 1200
	}
	return nil, false
}

// NextNibblesSubtree does []byte++. Returns false if overflow.
func NextNibblesSubtree(in []byte, out *[]byte) bool {
	r := (*out)[:len(in)]
	copy(r, in)
	for i := len(r) - 1; i >= 0; i-- {
		if r[i] != 15 { // max value of nibbles
			r[i]++
			*out = r
			return true
		}

		r = r[:i] // make it shorter, because in tries after 11ff goes 12, but not 1200
	}
	*out = r
	return false
}
