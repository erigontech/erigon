package base_encoding

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test64(t *testing.T) {
	number := uint64(9992)

	out := Encode64ToBytes4(number)
	require.Equal(t, Decode64FromBytes4(out), number)

	out = EncodeCompactUint64(number)
	require.Equal(t, DecodeCompactUint64(out), number)
}

func TestDiff64(t *testing.T) {
	old := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9}
	new := []uint64{1, 2, 3, 4, 5, 61, 45, 8, 9, 10}

	var b bytes.Buffer

	err := ComputeCompressedSerializedUint64ListDiff(&b, old, new)
	require.NoError(t, err)

	out := b.Bytes()

	new2, err := ApplyCompressedSerializedUint64ListDiff(old, nil, out)
	require.NoError(t, err)

	require.Equal(t, new, new2)
}
