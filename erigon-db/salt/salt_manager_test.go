package salt

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
	dirs, e3, block := setupSM(t, false, false)
	require.Nil(t, e3.Salt())
	require.Nil(t, block.Salt())

	saltFile := path.Join(dirs.Snap, "salt-state.txt")
	defaultSaltBytes := []byte{0x00, 0x00, 0x00, 0x64}
	err := dir.WriteFileWithFsync(saltFile, defaultSaltBytes, os.ModePerm)
	require.NoError(t, err)

	require.Nil(t, block.Salt())
	require.Equal(t, uint32(100), *e3.Salt())

	saltFile = path.Join(dirs.Snap, "salt-blocks.txt")
	defaultSaltBytes = []byte{0x00, 0x00, 0x00, 0x65}
	err = dir.WriteFileWithFsync(saltFile, defaultSaltBytes, os.ModePerm)
	require.NoError(t, err)

	require.Equal(t, uint32(101), *block.Salt())
	require.Equal(t, uint32(100), *e3.Salt())
}

func TestSaltManager_GenNew(t *testing.T) {
	dirs, e3, block := setupSM(t, true, true)
	bs := block.Salt()
	require.NotNil(t, bs)
	require.NotNil(t, e3.Salt())

	saltFile := path.Join(dirs.Snap, "salt-state.txt")
	defaultSaltBytes := []byte{0x00, 0x00, 0x00, 0x64}
	err := dir.WriteFileWithFsync(saltFile, defaultSaltBytes, os.ModePerm)
	require.NoError(t, err)

	require.Equal(t, bs, block.Salt())
	require.Equal(t, uint32(100), *e3.Salt())

	saltFile = path.Join(dirs.Snap, "salt-blocks.txt")
	defaultSaltBytes = []byte{0x00, 0x00, 0x00, 0x65}
	err = dir.WriteFileWithFsync(saltFile, defaultSaltBytes, os.ModePerm)
	require.NoError(t, err)

	require.Equal(t, uint32(101), *block.Salt())
	require.Equal(t, uint32(100), *e3.Salt())
}

func setupSM(t *testing.T, genState, genBlock bool) (datadir.Dirs, *SaltManager, *SaltManager) {
	t.Helper()
	dirs := datadir.New(t.TempDir())
	logger := log.New()
	sm1, sm2 := NewE3SaltManager(dirs, genState, logger), NewBlockSaltManager(dirs, genBlock, logger)
	t.Cleanup(func() {
		sm1.Close()
		sm2.Close()
	})
	return dirs, sm1, sm2
}
