package state

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/seg"
)

// TestDirtyFilesRoTx_CloseCallsCloseAndRemoveWhenRefCntDropsToZero tests the "refcnt > 0" bug:
// when a dirtyFilesRoTx is the LAST holder of a canDelete=true file (refcount 1→0 on Close),
// Close() must call closeFilesAndRemove() so the FD is released and the disk file unlinked.
//
// Without the fix, Close() only decrements refcount without checking canDelete, leaving the
// decompressor FD open and the file on disk indefinitely (inode count=0 but not freed).
// This in turn causes OpenFolder to re-create a new FilesItem instance for the same range,
// and BuildMissedAccessors to build fresh accessors for it — an unbreakable cycle.
func TestDirtyFilesRoTx_CloseCallsCloseAndRemoveWhenRefCntDropsToZero(t *testing.T) {
	tmp := t.TempDir()
	fPath := filepath.Join(tmp, "v1-foo.0-1.kv")

	// Create a real compressed segment file so seg.NewDecompressor can open it.
	comp, err := seg.NewCompressor(context.Background(), t.Name(), fPath, tmp, seg.DefaultCfg, log.LvlDebug, log.New())
	require.NoError(t, err)
	comp.DisableFsync()
	require.NoError(t, comp.AddWord([]byte("word")))
	require.NoError(t, comp.Compress())
	comp.Close()

	dec, err := seg.NewDecompressor(fPath)
	require.NoError(t, err)

	item := &FilesItem{startTxNum: 0, endTxNum: 10}
	item.decompressor = dec

	// Simulate: file was merged away → removed from BTree and marked canDelete=true.
	// The dirtyFilesRoTx still owns refcount=1 (incremented by DebugBeginDirtyFilesRo).
	item.canDelete.Store(true)
	item.refcount.Store(1)

	// Construct iiDirtyFilesRoTx as DebugBeginDirtyFilesRo() would.
	// ii must be non-nil so Close() doesn't short-circuit; the actual InvertedIndex
	// fields are irrelevant for this test.
	rotx := &iiDirtyFilesRoTx{
		ii:    &InvertedIndex{},
		files: []*FilesItem{item},
	}

	// Close() is the last reader releasing the file (refcount 1→0, canDelete=true).
	// Must call closeFilesAndRemove(): closes FDs and unlinks the disk file.
	rotx.Close()

	exists, err := dir.FileExist(fPath)
	require.NoError(t, err)
	require.False(t, exists,
		"file must be deleted from disk when dirtyFilesRoTx.Close() is the last reader with canDelete=true")
}
