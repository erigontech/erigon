package scalar

import (
	"fmt"
	"io"
	"strconv"

	"github.com/99designs/gqlgen/graphql"
)

type Uint64 = uint64

func MarshalUint64(v uint64) graphql.Marshaler {
	return graphql.WriterFunc(func(w io.Writer) {
		fmt.Fprintf(w, `"0x%x"`, v)
	})
}

func UnmarshalUint64(v any) (uint64, error) {
	switch val := v.(type) {
	case string:
		if len(val) > 2 && val[0] == '0' && (val[1] == 'x' || val[1] == 'X') {
			return strconv.ParseUint(val[2:], 16, 64)
		}
		return strconv.ParseUint(val, 10, 64)
	case int64:
		if val < 0 {
			return 0, fmt.Errorf("Long cannot be negative: %d", val)
		}
		return uint64(val), nil
	case float64:
		return uint64(val), nil
	default:
		return graphql.UnmarshalUint64(v)
	}
}
