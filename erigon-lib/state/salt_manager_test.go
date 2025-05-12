package state

import (
	"os"
	"path"
	"testing"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/stretchr/testify/require"
)

func TestSaltManager_NoGenNew(t *testing.T) {
	dirs, i := setupSM(t, false, false)
	require.Nil(t, i.BlockSalt())
	require.Nil(t, i.StateSalt())

	saltFile := path.Join(dirs.Snap, "salt-state.txt")
	defaultSaltBytes := []byte{0x00, 0x00, 0x00, 0x64}
	err := dir.WriteFileWithFsync(saltFile, defaultSaltBytes, os.ModePerm)
	require.NoError(t, err)

	require.Nil(t, i.BlockSalt())
	require.Equal(t, uint32(100), *i.StateSalt())

	saltFile = path.Join(dirs.Snap, "salt-block.txt")
	defaultSaltBytes = []byte{0x00, 0x00, 0x00, 0x65}
	err = dir.WriteFileWithFsync(saltFile, defaultSaltBytes, os.ModePerm)
	require.NoError(t, err)

	require.Equal(t, uint32(101), *i.BlockSalt())
	require.Equal(t, uint32(100), *i.StateSalt())
}

func TestSaltManager_GenNew(t *testing.T) {
	dirs, i := setupSM(t, true, true)
	bs := i.BlockSalt()
	require.NotNil(t, bs)
	require.NotNil(t, i.StateSalt())

	saltFile := path.Join(dirs.Snap, "salt-state.txt")
	defaultSaltBytes := []byte{0x00, 0x00, 0x00, 0x64}
	err := dir.WriteFileWithFsync(saltFile, defaultSaltBytes, os.ModePerm)
	require.NoError(t, err)

	require.Equal(t, bs, i.BlockSalt())
	require.Equal(t, uint32(100), *i.StateSalt())

	saltFile = path.Join(dirs.Snap, "salt-block.txt")
	defaultSaltBytes = []byte{0x00, 0x00, 0x00, 0x65}
	err = dir.WriteFileWithFsync(saltFile, defaultSaltBytes, os.ModePerm)
	require.NoError(t, err)

	require.Equal(t, uint32(101), *i.BlockSalt())
	require.Equal(t, uint32(100), *i.StateSalt())
}

func setupSM(t *testing.T, genState, genBlock bool) (datadir.Dirs, *SaltManager) {
	dirs := datadir.New(t.TempDir())
	sm := NewSaltManager(dirs, genState, genBlock, log.New())
	t.Cleanup(func() {
		sm.Close()
	})
	return dirs, sm
}
