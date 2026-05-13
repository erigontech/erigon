package downloader

import (
	"os"
	"strconv"

	"github.com/anacrolix/missinggo/v2/panicif"
)

type signed interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64
}
type integer interface {
	signed | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
}

func initIntFromEnv[T signed](key string, defaultValue T, bitSize int) T {
	return strconvFromEnv(key, defaultValue, bitSize, strconv.ParseInt)
}

func strconvFromEnv[T, U integer](key string, defaultValue T, bitSize int, conv func(s string, base, bitSize int) (U, error)) T {
	s := os.Getenv(key)
	if s == "" {
		return defaultValue
	}
	i64, err := conv(s, 10, bitSize)
	panicif.Err(err)
	return T(i64)
}
