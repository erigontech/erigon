package state

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	btree2 "github.com/tidwall/btree"

	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon/db/kv"
)

func TestFileItemWithMissedAccessor(t *testing.T) {
	tmp := t.TempDir()

	// filesItem
	f1 := &FilesItem{
		startTxNum: 1,
		endTxNum:   10,
	}
	f2 := &FilesItem{
		startTxNum: 11,
		endTxNum:   20,
	}
	f3 := &FilesItem{
		startTxNum: 31,
		endTxNum:   40,
	}
	aggStep := uint64(10)

	btree := btree2.NewBTreeGOptions(filesItemLess, btree2.Options{Degree: 128, NoLocks: false})
	btree.Set(f1)
	btree.Set(f2)
	btree.Set(f3)

	accessorFor := func(fromStep, toStep kv.Step) []string {
		return []string{
			filepath.Join(tmp, fmt.Sprintf("testacc_%d_%d.bin", fromStep, toStep)),
			filepath.Join(tmp, fmt.Sprintf("testacc2_%d_%d.bin", fromStep, toStep)),
		}
	}

	// create accesssor files for f1, f2
	for _, fname := range accessorFor(kv.Step(f1.startTxNum/aggStep), kv.Step(f1.endTxNum/aggStep)) {
		os.WriteFile(fname, []byte("test"), 0644)
		defer dir.RemoveFile(fname)
	}

	for _, fname := range accessorFor(kv.Step(f2.startTxNum/aggStep), kv.Step(f2.endTxNum/aggStep)) {
		os.WriteFile(fname, []byte("test"), 0644)
		defer dir.RemoveFile(fname)
	}

	fileItems := fileItemsWithMissedAccessors(btree.Items(), aggStep, accessorFor)
	require.Len(t, fileItems, 1)
	require.Equal(t, f3, fileItems[0])
}
