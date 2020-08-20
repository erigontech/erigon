package etl

import "time"

func progressFromKey(k []byte) int {
	if len(k) < 1 {
		return 0
	}
	return int(float64(k[0]>>4) * 3.3)
}

func printProgressIfNeeded(i int, t time.Time, k []byte, printFunc func(int)) (int, time.Time) {
	if i%1_000_000 == 0 && time.Since(t) > 30*time.Second {
		printFunc(progressFromKey(k))
		return i + 1, time.Now()
	}
	return i + 1, t
}
