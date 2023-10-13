package base_encoding

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test64(t *testing.T) {
	number := uint64(0x1234567890abcd)

	buf := make([]byte, 8)
	out := Encode64(number, buf)
	require.Equal(t, len(out), 7)
	require.Equal(t, Decode64(out), number)
}
