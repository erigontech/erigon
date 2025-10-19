package state

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	btree2 "github.com/tidwall/btree"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/kv"
)

func TestStepRange(t *testing.T) {
	stepSize := uint64(4)

	t.Run("simple range", func(t *testing.T) {
		f := &FilesItem{
			startTxNum: 1,
			endTxNum:   10,
		}

		startStep, endStep := f.StepRange(stepSize)
		require.Equal(t, kv.Step(0), startStep)
		require.Equal(t, kv.Step(2), endStep)
		require.Equal(t, uint64(2), f.StepCount(4))
	})

	t.Run("inner boundaries", func(t *testing.T) {
		f := &FilesItem{
			startTxNum: 4,
			endTxNum:   7,
		}

		startStep, endStep := f.StepRange(stepSize)
		require.Equal(t, kv.Step(1), startStep)
		require.Equal(t, kv.Step(1), endStep)
		require.Equal(t, uint64(0), f.StepCount(stepSize))
	})

	t.Run("outer boundaries", func(t *testing.T) {
		f := &FilesItem{
			startTxNum: 3,
			endTxNum:   8,
		}
		startStep, endStep := f.StepRange(stepSize)
		require.Equal(t, kv.Step(0), startStep)
		require.Equal(t, kv.Step(2), endStep)
		require.Equal(t, uint64(2), f.StepCount(stepSize))
	})
}

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
	for _, fname := range accessorFor(f1.StepRange(aggStep)) {
		os.WriteFile(fname, []byte("test"), 0644)
		defer dir.RemoveFile(fname)
	}

	for _, fname := range accessorFor(f2.StepRange(aggStep)) {
		os.WriteFile(fname, []byte("test"), 0644)
		defer dir.RemoveFile(fname)
	}

	fileItems := fileItemsWithMissedAccessors(btree.Items(), aggStep, accessorFor)
	require.Len(t, fileItems, 1)
	require.Equal(t, f3, fileItems[0])
}
