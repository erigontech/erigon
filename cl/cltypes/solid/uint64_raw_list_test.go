package solid_test

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes/solid"
)

func TestRawUint64ListDecodeSSZRejectsInvalidSize(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"partial element", make([]byte, 7)},
		{"over limit", make([]byte, 16)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			list := solid.NewRawUint64List(1, nil)
			require.Error(t, list.DecodeSSZ(test.data, 0))
		})
	}
}

func TestRawUint64ListDecodeSSZAcceptsLimit(t *testing.T) {
	list := solid.NewRawUint64List(1, nil)
	require.NoError(t, list.DecodeSSZ(make([]byte, 8), 0))
	require.Equal(t, 1, list.Length())
}

func encodeUint64s(values ...uint64) []byte {
	buf := make([]byte, 0, len(values)*8)
	for _, v := range values {
		buf = binary.LittleEndian.AppendUint64(buf, v)
	}
	return buf
}

func TestRawUint64ListHashSSZAfterShrink(t *testing.T) {
	reused := solid.NewRawUint64List(16, nil)
	require.NoError(t, reused.DecodeSSZ(encodeUint64s(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12), 0))
	_, err := reused.HashSSZ()
	require.NoError(t, err)

	require.NoError(t, reused.DecodeSSZ(encodeUint64s(1, 2, 3, 4, 5), 0))
	got, err := reused.HashSSZ()
	require.NoError(t, err)

	want, err := solid.NewRawUint64List(16, []uint64{1, 2, 3, 4, 5}).HashSSZ()
	require.NoError(t, err)
	require.Equal(t, want, got)
}
