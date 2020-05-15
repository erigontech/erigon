package state

import "bytes"

func cleanUpTrailingZeroes(value []byte) []byte {
	return bytes.TrimLeft(value[:], "\x00")
}
