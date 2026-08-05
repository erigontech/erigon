package solid_test

import (
	"testing"

	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/stretchr/testify/require"
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
