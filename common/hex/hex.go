package hex

import (
	"encoding/hex"
)

func MustDecodeString(s string) []byte {
	r, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return r
}
