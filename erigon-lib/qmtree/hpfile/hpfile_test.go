package hpfile

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHpFileNew(t *testing.T) {
	dir, err := NewTempDir("hp_file_test")
	require.NoError(t, err)
	bufferSize := 64
	segmentSize := uint64(128)
	hp, err := NewFile(bufferSize, segmentSize, dir.String())
	require.Equal(t, hp.bufferSize, bufferSize)
	require.Equal(t, hp.segmentSize, segmentSize)
	require.Equal(t, len(hp.fileMap.files), 1)

	slice0 := slices.Repeat([]byte{1}, 44)
	pos, err := hp.Append(slice0)
	require.NoError(t, err)
	require.Equal(t, 0, pos)
	require.Equal(t, 44, hp.Size())

	slice1a := slices.Repeat([]byte{2}, 16)
	slice1b := slices.Repeat([]byte{3}, 10)
	slice1 := slice1a
	slice1 = append(slice1, slice1b...)
	slice1 = append(slice1, slice1b...)
	pos, err = hp.Append(slice1)
	require.NoError(t, err)
	require.Equal(t, 44, pos)
	require.Equal(t, 70, hp.Size())

	slice2a := slices.Repeat([]byte{4}, 25)
	slice2b := slices.Repeat([]byte{5}, 25)
	slice2 := slice2a
	slice2 = append(slice2, slice2b...)
	pos, err = hp.Append(slice2)
	require.NoError(t, err)
	require.Equal(t, 70, pos)
	require.Equal(t, 120, hp.Size())

	check0 := make([]byte, 44)
	_, err = hp.ReadAt(check0, 0)
	require.NoError(t, err)
	require.Equal(t, slice0, check0)

	check1 := make([]byte, 26)
	_, err = hp.ReadAt(check1, 44)
	require.NoError(t, err)
	require.Equal(t, slice1, check1)

	check2 := make([]byte, 50)
	_, err = hp.ReadAt(check2, 70)
	require.NoError(t, err)
	require.Equal(t, slice2, check2)

	slice3 := make([]byte, 16)
	pos, err = hp.Append(slice3)
	require.NoError(t, err)
	require.Equal(t, 120, pos)
	require.Equal(t, 136, hp.Size())

	hp.Close()

	hpNew, err := NewFile(64, 128, dir.String())

	_, err = hpNew.ReadAt(check0, 0)
	require.NoError(t, err)
	require.Equal(t, slice0, check0)

	_, err = hpNew.ReadAt(check1, 44)
	require.NoError(t, err)
	require.Equal(t, slice1, check1)

	_, err = hpNew.ReadAt(check2, 70)
	require.NoError(t, err)
	require.Equal(t, slice2, check2)

	check3 := make([]byte, 16)
	_, err = hpNew.ReadAt(check3, 120)
	require.NoError(t, err)
	require.Equal(t, slice3, check3)

	err = hpNew.PruneHead(64)
	require.NoError(t, err)
	err = hpNew.Truncate(120)
	require.NoError(t, err)
	require.Equal(t, hpNew.Size(), 120)
	slice4 := make([]byte, 120)
	_, err = hpNew.ReadAt(slice4, 120)
	require.NoError(t, err)
	require.Equal(t, len(slice4), 0)
}
