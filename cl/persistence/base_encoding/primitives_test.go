package base_encoding

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test64(t *testing.T) {
	number := uint64(0x1234567890abcd)

	out := Encode64(number)
	require.Equal(t, len(out), 7)
	require.Equal(t, Decode64(out), number)
}
