package downloader

import (
	"os"
	"strconv"

	"github.com/anacrolix/missinggo/v2/panicif"
	"golang.org/x/exp/constraints"
)

func initIntFromEnv[T constraints.Signed](key string, defaultValue T, bitSize int) T {
	return strconvFromEnv(key, defaultValue, bitSize, strconv.ParseInt)
}

func strconvFromEnv[T constraints.Integer](key string, defaultValue T, bitSize int, conv func(s string, base, bitSize int) (int64, error)) T {
	s := os.Getenv(key)
	if s == "" {
		return defaultValue
	}
	i64, err := conv(s, 10, bitSize)
	panicif.Err(err)
	return T(i64)
}
