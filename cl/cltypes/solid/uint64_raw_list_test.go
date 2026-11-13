package solid_test

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
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

func TestRawUint64ListHashSSZAfterDecodeSSZ(t *testing.T) {
	values := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	var encoded []byte
	for _, value := range values {
		encoded = binary.LittleEndian.AppendUint64(encoded, value)
	}
	tests := []struct {
		name   string
		length int
		root   string
	}{
		{"empty", 0, "0x28ba1834a3a7b657460ce79fa3a1d909ab8828fd557659d4d0554a9bdbc0ec30"},
		{"one element", 1, "0x76f9439b26367975bb97a1010ef4309789d1814af63402a273b9db692dc89f48"},
		{"full chunk", 4, "0x26b975baf4a42c649f506161857b8f8ddd65426b3dc0bcb538298cc4af7ae90e"},
		{"five elements", 5, "0x51509d69c60305791262a3f804384b122542b8641ff82119515ab4a21868218a"},
		{"nine elements", 9, "0x1535114d36c6866a2b58772cafb3222bda3a5a921751c22069d40598851926a7"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			list := solid.NewRawUint64List(16, nil)
			require.NoError(t, list.DecodeSSZ(encoded, 0))
			_, err := list.HashSSZ()
			require.NoError(t, err)

			require.NoError(t, list.DecodeSSZ(encoded[:test.length*8], 0))
			root, err := list.HashSSZ()
			require.NoError(t, err)
			require.Equal(t, common.HexToHash(test.root), common.Hash(root))

			fresh := solid.NewRawUint64List(16, values[:test.length])
			freshRoot, err := fresh.HashSSZ()
			require.NoError(t, err)
			require.Equal(t, freshRoot, root)
		})
	}
}
