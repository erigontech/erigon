package etl

func ProgressFromKey(k []byte) int {
	if len(k) < 1 {
		return 0
	}
	return int(float64(k[0]>>4) * 3.3)
}
