package solid_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes/solid"
)

func TestRawUint64ListJSONLimit(t *testing.T) {
	for _, size := range []int{0, 2, 3} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			list := solid.NewRawUint64List(2, nil)
			values := make([]string, size)
			for i := range values {
				values[i] = "1"
			}
			data, err := json.Marshal(values)
			require.NoError(t, err)
			err = json.Unmarshal(data, list)
			if size > 2 {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, size, list.Length())
			}
		})
	}
}

func TestRawUint64ListJSONReplacesHashState(t *testing.T) {
	list := solid.NewRawUint64List(16, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
	_, err := list.HashSSZ()
	require.NoError(t, err)
	for _, values := range [][]uint64{{5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, {1, 2, 3}, {}} {
		strs := make([]string, len(values))
		for i, v := range values {
			strs[i] = fmt.Sprint(v)
		}
		data, err := json.Marshal(strs)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(data, list))
		root, err := list.HashSSZ()
		require.NoError(t, err)
		expected, err := solid.NewRawUint64List(16, values).HashSSZ()
		require.NoError(t, err)
		require.Equal(t, expected, root)
	}
}

func TestRawUint64ListJSONFailurePreservesValues(t *testing.T) {
	for _, data := range []string{`["2","3","4"]`, `["2","invalid"]`} {
		list := solid.NewRawUint64List(2, []uint64{1})
		root, err := list.HashSSZ()
		require.NoError(t, err)
		require.Error(t, json.Unmarshal([]byte(data), list))
		require.Equal(t, 1, list.Length())
		require.Equal(t, uint64(1), list.Get(0))
		after, err := list.HashSSZ()
		require.NoError(t, err)
		require.Equal(t, root, after)
	}
}
