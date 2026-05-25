package state

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/seg"
)

// TestDirtyFilesRoTx_CloseIsNotADeleter verifies the reclamation ownership
// invariant: a debug dirtyFilesRoTx.Close() must NOT physically delete files —
// even with canDelete=true. Files captured by a DebugBeginDirtyFilesRo are
// protected by the pinned generation, and physical deletion is owned solely by
// the aggregator reclaimer (reclaimDrainedLocked). A second deleter here would
// re-introduce the double-free this design removes. (End-to-end coverage of the
// deferred deletion: TestAggregatorRetireDeferredWhileDebugPins.)
func TestDirtyFilesRoTx_CloseIsNotADeleter(t *testing.T) {
	tmp := t.TempDir()
	fPath := filepath.Join(tmp, "v1-foo.0-1.kv")

	// Create a real compressed segment file so seg.NewDecompressor can open it.
	comp, err := seg.NewCompressor(t.Context(), t.Name(), fPath, tmp, seg.DefaultCfg, log.LvlDebug, log.New())
	require.NoError(t, err)
	comp.DisableFsync()
	require.NoError(t, comp.AddWord([]byte("word")))
	require.NoError(t, comp.Compress())
	comp.Close()

	dec, err := seg.NewDecompressor(fPath)
	require.NoError(t, err)
	defer dec.Close() // Close() under test must not delete; release the FD so TempDir cleanup can unlink (Windows)

	item := &FilesItem{startTxNum: 0, endTxNum: 10}
	item.decompressor = dec
	item.canDelete.Store(true) // even with canDelete set, Close must not delete

	rotx := &iiDirtyFilesRoTx{
		ii:    &InvertedIndex{},
		files: []*FilesItem{item},
	}

	rotx.Close()

	exists, err := dir.FileExist(fPath)
	require.NoError(t, err)
	require.True(t, exists,
		"debug dirtyFilesRoTx.Close() must NOT delete the file; only the aggregator reclaimer deletes")
}
