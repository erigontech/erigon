package state

//Write into 1 extra RLP level
func AddExtraRLPLevel(v []byte) []byte {
	var vv []byte

	if len(v) > 1 || v[0] >= 128 {
		vv = make([]byte, len(v)+1)
		vv[0] = byte(128 + len(v))
		copy(vv[1:], v)
	} else {
		vv = make([]byte, 1)
		vv[0] = v[0]
	}
	return vv
}
