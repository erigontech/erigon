package state

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	btree2 "github.com/tidwall/btree"
)

func TestFileItemWithMissingAccessor(t *testing.T) {
	tmp := t.TempDir()

	// filesItem
	f1 := &filesItem{
		startTxNum: 1,
		endTxNum:   10,
	}
	f2 := &filesItem{
		startTxNum: 11,
		endTxNum:   20,
	}
	f3 := &filesItem{
		startTxNum: 31,
		endTxNum:   40,
	}
	aggStep := uint64(10)

	btree := btree2.NewBTreeGOptions(filesItemLess, btree2.Options{Degree: 128, NoLocks: false})
	btree.Set(f1)
	btree.Set(f2)
	btree.Set(f3)

	accessorFor := func(fromStep, toStep uint64) []string {
		return []string{
			filepath.Join(tmp, fmt.Sprintf("testacc_%d_%d.bin", fromStep, toStep)),
			filepath.Join(tmp, fmt.Sprintf("testacc2_%d_%d.bin", fromStep, toStep)),
		}
	}

	// create accesssor files for f1, f2
	for _, fname := range accessorFor(f1.startTxNum/aggStep, f1.endTxNum/aggStep) {
		os.WriteFile(fname, []byte("test"), 0644)
		defer os.Remove(fname)
	}

	for _, fname := range accessorFor(f2.startTxNum/aggStep, f2.endTxNum/aggStep) {
		os.WriteFile(fname, []byte("test"), 0644)
		defer os.Remove(fname)
	}

	fileItems := fileItemsWithMissingAccessors(btree, aggStep, accessorFor)
	require.Equal(t, 1, len(fileItems))
	require.Equal(t, f3, fileItems[0])
}
