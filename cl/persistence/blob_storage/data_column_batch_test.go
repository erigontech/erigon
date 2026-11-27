package blob_storage

import (
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

const (
	batchTestSlot    = 1000
	batchTestColumns = 8
)

type batchHarness struct {
	fs       afero.Fs
	storage  DataColumnStorage
	emitters *beaconevents.EventEmitter
	events   chan *beaconevents.EventStream
}

func newBatchHarness(t testing.TB, fs afero.Fs) *batchHarness {
	t.Helper()
	cfg := *globalBeaconConfig
	cfg.NumberOfColumns = 128
	h := &batchHarness{
		fs:       fs,
		emitters: beaconevents.NewEventEmitter(),
		events:   make(chan *beaconevents.EventStream, 1024),
	}
	subscription := h.emitters.Operation().Subscribe(h.events)
	t.Cleanup(subscription.Unsubscribe)
	h.storage = NewDataColumnStore(fs, &cfg, h.emitters)
	return h
}

func (h *batchHarness) drainEvents() []*beaconevents.DataColumnSidecarData {
	var out []*beaconevents.DataColumnSidecarData
	for {
		select {
		case e := <-h.events:
			out = append(out, e.Data.(*beaconevents.DataColumnSidecarData))
		default:
			return out
		}
	}
}

func (h *batchHarness) drainNotifications() []common.Hash {
	var out []common.Hash
	for {
		select {
		case root := <-h.emitters.DataColumnStored():
			out = append(out, root)
		default:
			return out
		}
	}
}

func batchTestSidecars(slot uint64, indices ...int64) []*cltypes.DataColumnSidecar {
	out := make([]*cltypes.DataColumnSidecar, len(indices))
	for i, index := range indices {
		out[i] = createTestDataColumnSidecar(slot, index)
	}
	return out
}

func batchTestIndices(n int) []int64 {
	out := make([]int64, n)
	for i := range out {
		out[i] = int64(i)
	}
	return out
}

func listFiles(t testing.TB, fs afero.Fs) []string {
	t.Helper()
	var files []string
	require.NoError(t, afero.Walk(fs, ".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			files = append(files, filepath.ToSlash(path))
		}
		return nil
	}))
	return files
}

func requireAllReadable(t testing.TB, storage DataColumnStorage, slot uint64, root common.Hash, indices ...int64) {
	t.Helper()
	for _, index := range indices {
		sidecar, err := storage.ReadColumnSidecarByColumnIndex(t.Context(), slot, root, index)
		require.NoError(t, err, "column %d", index)
		require.Equal(t, uint64(index), sidecar.Index)
	}
}

func TestWriteColumnSidecarsBatchStoresAll(t *testing.T) {
	filesystems := map[string]func() afero.Fs{
		"MemMapFs":     afero.NewMemMapFs,
		"openHandleFs": func() afero.Fs { return newOpenHandleFs(afero.NewMemMapFs()) },
	}
	for name, newFs := range filesystems {
		t.Run(name, func(t *testing.T) {
			fs := newCountingFs(newFs())
			h := newBatchHarness(t, fs)
			root := common.HexToHash("0x1234567890abcdef")
			indices := batchTestIndices(batchTestColumns)

			require.NoError(t, h.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(batchTestSlot, indices...)))

			require.Equal(t, 1, fs.count(opMkdirAll))
			require.Equal(t, 1, fs.count(opCreate))
			require.Equal(t, 1, fs.count(opSync))
			require.Equal(t, 1, fs.count(opRename))
			require.Equal(t, []string{testPackFile(batchTestSlot, root)}, listFiles(t, fs))
			requireAllReadable(t, h.storage, batchTestSlot, root, indices...)
			saved, err := h.storage.GetSavedColumnIndex(t.Context(), batchTestSlot, root)
			require.NoError(t, err)
			require.Equal(t, []uint64{0, 1, 2, 3, 4, 5, 6, 7}, saved)
			events := h.drainEvents()
			require.Len(t, events, batchTestColumns)
			for _, event := range events {
				require.Equal(t, root, event.BlockRoot)
				require.Equal(t, uint64(batchTestSlot), event.Slot)
			}
			require.Equal(t, []common.Hash{root}, h.drainNotifications())
		})
	}
}

func TestWriteColumnSidecarsBatchSkipsStored(t *testing.T) {
	fs := newCountingFs(afero.NewMemMapFs())
	h := newBatchHarness(t, fs)
	root := common.HexToHash("0x1234567890abcdef")
	indices := batchTestIndices(batchTestColumns)
	for _, sidecar := range batchTestSidecars(batchTestSlot, indices[:6]...) {
		require.NoError(t, h.storage.WriteColumnSidecars(t.Context(), root, int64(sidecar.Index), sidecar))
	}
	fs.reset()
	h.drainEvents()
	h.drainNotifications()

	require.NoError(t, h.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(batchTestSlot, indices...)))

	require.Equal(t, 1, fs.count(opCreate))
	require.Equal(t, 1, fs.count(opRename))
	require.Equal(t, []uint64{6, 7}, packedColumns(t, fs, batchTestSlot, root, 128))
	events := h.drainEvents()
	require.Len(t, events, 2)
	got := []uint64{events[0].Index, events[1].Index}
	slices.Sort(got)
	require.Equal(t, []uint64{6, 7}, got)
	require.Equal(t, []common.Hash{root}, h.drainNotifications())
	requireAllReadable(t, h.storage, batchTestSlot, root, indices...)
	saved, err := h.storage.GetSavedColumnIndex(t.Context(), batchTestSlot, root)
	require.NoError(t, err)
	require.Len(t, saved, batchTestColumns)
}

func TestWriteColumnSidecarsBatchOfOneMissingColumnStaysLoose(t *testing.T) {
	fs := afero.NewMemMapFs()
	h := newBatchHarness(t, fs)
	root := common.HexToHash("0x1234567890abcdef")
	indices := batchTestIndices(batchTestColumns)
	for _, sidecar := range batchTestSidecars(batchTestSlot, indices[:7]...) {
		require.NoError(t, h.storage.WriteColumnSidecars(t.Context(), root, int64(sidecar.Index), sidecar))
	}

	require.NoError(t, h.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(batchTestSlot, indices...)))

	_, last := h.storage.(*dataColumnStorageImpl).path(batchTestSlot, root, 7)
	files := listFiles(t, fs)
	require.Len(t, files, batchTestColumns)
	require.Contains(t, files, last)
	packsExist, err := afero.DirExists(fs, path.Dir(testPackFile(batchTestSlot, root)))
	require.NoError(t, err)
	require.False(t, packsExist)
}

func TestWriteColumnSidecarsBatchWiderThanAPackHeaderStaysLoose(t *testing.T) {
	fs := afero.NewMemMapFs()
	cfg := *globalBeaconConfig
	cfg.NumberOfColumns = math.MaxUint16 + 1
	storage := NewDataColumnStore(fs, &cfg, beaconevents.NewEventEmitter())
	root := common.HexToHash("0x1234567890abcdef")

	require.NoError(t, storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(batchTestSlot, 0, 1)))

	impl := storage.(*dataColumnStorageImpl)
	_, first := impl.path(batchTestSlot, root, 0)
	_, second := impl.path(batchTestSlot, root, 1)
	require.ElementsMatch(t, []string{first, second}, listFiles(t, fs))
	packsExist, err := afero.DirExists(fs, path.Dir(testPackFile(batchTestSlot, root)))
	require.NoError(t, err)
	require.False(t, packsExist)
	requireAllReadable(t, storage, batchTestSlot, root, 0, 1)
}

type oversizedMarshaler struct{}

func (oversizedMarshaler) EncodingSizeSSZ() int                 { return int(clparams.MaxPayloadSize) + 1 }
func (oversizedMarshaler) EncodeSSZ(dst []byte) ([]byte, error) { return append(dst, 1, 2, 3), nil }

func TestWriteMissingAdmittedKeepsAColumnAboveMaxPayloadSizeLoose(t *testing.T) {
	fs := afero.NewMemMapFs()
	storage := newBatchHarness(t, fs).storage.(*dataColumnStorageImpl)
	root := common.HexToHash("0x1234567890abcdef")
	entries := []batchEntry{{idx: 0, v: createTestDataColumnSidecar(batchTestSlot, 0)}, {idx: 1, v: oversizedMarshaler{}}}

	existing, created, err := storage.writeMissingAdmitted(batchTestSlot, root, entries)

	require.NoError(t, err)
	require.Empty(t, existing)
	require.Equal(t, []int{0, 1}, created)
	_, first := storage.path(batchTestSlot, root, 0)
	_, second := storage.path(batchTestSlot, root, 1)
	require.ElementsMatch(t, []string{first, second}, listFiles(t, fs))
	packsExist, err := afero.DirExists(fs, path.Dir(testPackFile(batchTestSlot, root)))
	require.NoError(t, err)
	require.False(t, packsExist)
}

type panickingMarshaler struct{}

func (panickingMarshaler) EncodingSizeSSZ() int                 { return 1 }
func (panickingMarshaler) EncodeSSZ(dst []byte) ([]byte, error) { panic("encode") }

func TestWriteMissingAdmittedClosesThePackTempWhenEncodingPanics(t *testing.T) {
	handles := newOpenHandleFs(afero.NewMemMapFs())
	storage := newBatchHarness(t, handles).storage.(*dataColumnStorageImpl)
	root := common.HexToHash("0x1234567890abcdef")
	entries := []batchEntry{{idx: 0, v: createTestDataColumnSidecar(batchTestSlot, 0)}, {idx: 1, v: panickingMarshaler{}}}

	require.PanicsWithValue(t, "encode", func() { _, _, _ = storage.writeMissingAdmitted(batchTestSlot, root, entries) })

	require.Empty(t, handles.openHandles())
}

func TestWriteColumnSidecarsBatchOfStoredColumnsWritesNothing(t *testing.T) {
	fs := newCountingFs(afero.NewMemMapFs())
	h := newBatchHarness(t, fs)
	root := common.HexToHash("0x1234567890abcdef")
	sidecars := batchTestSidecars(batchTestSlot, batchTestIndices(batchTestColumns)...)
	require.NoError(t, h.storage.WriteColumnSidecarsBatch(t.Context(), root, sidecars))
	fs.reset()
	h.drainEvents()
	h.drainNotifications()

	require.NoError(t, h.storage.WriteColumnSidecarsBatch(t.Context(), root, sidecars))

	require.Zero(t, fs.count(opCreate))
	require.Zero(t, fs.count(opMkdirAll))
	require.Empty(t, h.drainEvents())
	require.Empty(t, h.drainNotifications())
}

func TestWriteColumnSidecarsBatchEmptyIsNoop(t *testing.T) {
	fs := newCountingFs(afero.NewMemMapFs())
	h := newBatchHarness(t, fs)

	require.NoError(t, h.storage.WriteColumnSidecarsBatch(t.Context(), common.Hash{1}, nil))

	require.Zero(t, fs.count(opCreate))
	require.Empty(t, h.drainNotifications())
}

func TestWriteColumnSidecarSkipsExistingTarget(t *testing.T) {
	fs := newCountingFs(afero.NewMemMapFs())
	h := newBatchHarness(t, fs)
	root := common.HexToHash("0x1234567890abcdef")
	sidecar := createTestDataColumnSidecar(batchTestSlot, 1)
	require.NoError(t, h.storage.WriteColumnSidecars(t.Context(), root, 1, sidecar))
	fs.reset()
	h.drainEvents()
	h.drainNotifications()

	require.NoError(t, h.storage.WriteColumnSidecars(t.Context(), root, 1, sidecar))

	require.Zero(t, fs.count(opCreate))
	require.Empty(t, h.drainEvents())
	require.Empty(t, h.drainNotifications())
	saved, err := h.storage.GetSavedColumnIndex(t.Context(), batchTestSlot, root)
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, saved)
}

func TestWriteColumnSidecarsBatchRejectsBadInput(t *testing.T) {
	cases := map[string]func() []*cltypes.DataColumnSidecar{
		"nil sidecar": func() []*cltypes.DataColumnSidecar {
			return append(batchTestSidecars(batchTestSlot, 0, 1), nil)
		},
		"mixed slots": func() []*cltypes.DataColumnSidecar {
			return []*cltypes.DataColumnSidecar{
				createTestDataColumnSidecar(batchTestSlot, 0),
				createTestDataColumnSidecar(batchTestSlot+1, 1),
			}
		},
		"duplicate index": func() []*cltypes.DataColumnSidecar {
			return batchTestSidecars(batchTestSlot, 0, 1, 1)
		},
		"index out of range": func() []*cltypes.DataColumnSidecar {
			return batchTestSidecars(batchTestSlot, 0, 128)
		},
	}
	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			fs := newCountingFs(afero.NewMemMapFs())
			h := newBatchHarness(t, fs)

			err := h.storage.WriteColumnSidecarsBatch(t.Context(), common.Hash{1}, build())

			require.Error(t, err)
			require.Zero(t, fs.count(opCreate))
			require.Zero(t, fs.count(opMkdirAll))
			require.Empty(t, listFiles(t, fs))
			require.Empty(t, h.drainEvents())
			require.Empty(t, h.drainNotifications())
		})
	}
}

func TestWriteColumnSidecarsBatchFailureLeavesNothing(t *testing.T) {
	root := common.HexToHash("0x1234567890abcdef")
	cases := map[string]struct {
		want         error
		renameFails  bool
		injectAtTemp func(fs *failingFs, tmpPath string)
	}{
		"sync": {want: errInducedFailure, injectAtTemp: func(fs *failingFs, tmpPath string) { fs.failSyncAt(tmpPath, errInducedFailure) }},
		"write": {want: errInducedFailure, injectAtTemp: func(fs *failingFs, tmpPath string) {
			fs.failWritesAfter(tmpPath, 0, errInducedFailure)
		}},
		"short write": {want: io.ErrShortWrite, injectAtTemp: func(fs *failingFs, tmpPath string) { fs.failShortWrite(tmpPath) }},
		"rename":      {want: errInducedFailure, renameFails: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			failing := newFailingFs(afero.NewMemMapFs())
			var fs afero.Fs = failing
			if tc.renameFails {
				fs = newRenameFailingFs(failing, errInducedFailure)
			}
			h := newBatchHarness(t, fs)
			if tc.injectAtTemp != nil {
				tc.injectAtTemp(failing, testPackFile(batchTestSlot, root)+tmpSuffix)
			}

			err := h.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(batchTestSlot, batchTestIndices(batchTestColumns)...))

			require.ErrorIs(t, err, tc.want)
			require.Empty(t, listFiles(t, fs))
			require.Empty(t, h.drainEvents())
			require.Empty(t, h.drainNotifications())
			saved, err := h.storage.GetSavedColumnIndex(t.Context(), batchTestSlot, root)
			require.NoError(t, err)
			require.Empty(t, saved)
		})
	}
}

func TestWriteColumnSidecarsBatchLooseFailureLeavesNothing(t *testing.T) {
	root := common.HexToHash("0x1234567890abcdef")
	cases := map[string]struct {
		want   error
		inject func(fs *failingFs, tmpPath string)
	}{
		"sync": {want: errInducedFailure, inject: func(fs *failingFs, tmpPath string) { fs.failSyncAt(tmpPath, errInducedFailure) }},
		"write": {want: errInducedFailure, inject: func(fs *failingFs, tmpPath string) {
			fs.failWritesAfter(tmpPath, 0, errInducedFailure)
		}},
		"short write": {want: io.ErrShortWrite, inject: func(fs *failingFs, tmpPath string) { fs.failShortWrite(tmpPath) }},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			failing := newFailingFs(afero.NewMemMapFs())
			h := newBatchHarness(t, failing)
			require.NoError(t, h.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(batchTestSlot, 0, 1)))
			packBefore, err := afero.ReadFile(failing, testPackFile(batchTestSlot, root))
			require.NoError(t, err)
			h.drainEvents()
			h.drainNotifications()
			_, blocked := h.storage.(*dataColumnStorageImpl).path(batchTestSlot, root, 3)
			tc.inject(failing, blocked+tmpSuffix)

			err = h.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(batchTestSlot, batchTestIndices(batchTestColumns)...))

			require.ErrorIs(t, err, tc.want)
			require.Equal(t, []string{testPackFile(batchTestSlot, root)}, listFiles(t, failing))
			packAfter, err := afero.ReadFile(failing, testPackFile(batchTestSlot, root))
			require.NoError(t, err)
			require.Equal(t, packBefore, packAfter)
			require.Empty(t, h.drainEvents())
			require.Empty(t, h.drainNotifications())
			saved, err := h.storage.GetSavedColumnIndex(t.Context(), batchTestSlot, root)
			require.NoError(t, err)
			require.Equal(t, []uint64{0, 1}, saved)
		})
	}
}

type renamePartiallyFailingFs struct {
	afero.Fs
	renames   int
	failAfter int
}

func (f *renamePartiallyFailingFs) Rename(oldname, newname string) error {
	f.renames++
	if f.renames > f.failAfter {
		return errInducedFailure
	}
	return f.Fs.Rename(oldname, newname)
}

func TestWriteColumnSidecarsBatchLooseBatchAfterAPackKeepsItsRenamedPrefix(t *testing.T) {
	fs := &renamePartiallyFailingFs{Fs: afero.NewMemMapFs(), failAfter: 3}
	h := newBatchHarness(t, fs)
	root := common.HexToHash("0x1234567890abcdef")
	require.NoError(t, h.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(batchTestSlot, 0, 1)))
	packBefore, err := afero.ReadFile(fs, testPackFile(batchTestSlot, root))
	require.NoError(t, err)
	h.drainEvents()
	h.drainNotifications()

	err = h.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(batchTestSlot, batchTestIndices(batchTestColumns)...))

	require.ErrorIs(t, err, errInducedFailure)
	packAfter, err := afero.ReadFile(fs, testPackFile(batchTestSlot, root))
	require.NoError(t, err)
	require.Equal(t, packBefore, packAfter)
	files := listFiles(t, fs)
	require.Len(t, files, 3)
	for _, file := range files {
		require.False(t, strings.HasSuffix(file, tmpSuffix), file)
	}
	saved, err := h.storage.GetSavedColumnIndex(t.Context(), batchTestSlot, root)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1, 2, 3}, saved)
	requireAllReadable(t, h.storage, batchTestSlot, root, 0, 1, 2, 3)
	var eventIndices []uint64
	for _, event := range h.drainEvents() {
		eventIndices = append(eventIndices, event.Index)
	}
	require.ElementsMatch(t, []uint64{2, 3}, eventIndices)
	require.Equal(t, []common.Hash{root}, h.drainNotifications())
}

type syncHookFs struct {
	afero.Fs
	onSync func(name string)
}

func (f *syncHookFs) Create(name string) (afero.File, error) {
	fh, err := f.Fs.Create(name)
	if err != nil {
		return nil, err
	}
	return &syncHookFile{File: fh, name: name, hook: f.onSync}, nil
}

type syncHookFile struct {
	afero.File
	name string
	hook func(name string)
}

func (f *syncHookFile) Sync() error {
	if f.hook != nil {
		f.hook(f.name)
	}
	return f.File.Sync()
}

func TestWriteColumnSidecarsBatchVisibleAllOrNothing(t *testing.T) {
	root := common.HexToHash("0x1234567890abcdef")
	cases := map[string]struct {
		prewritten []int64
		heldTemp   func(impl *dataColumnStorageImpl) string
		hidden     []int64
	}{
		"pack": {
			heldTemp: func(*dataColumnStorageImpl) string { return testPackFile(batchTestSlot, root) + tmpSuffix },
			hidden:   []int64{3},
		},
		"loose": {
			prewritten: []int64{0, 1},
			heldTemp: func(impl *dataColumnStorageImpl) string {
				_, file := impl.path(batchTestSlot, root, 3)
				return file + tmpSuffix
			},
			hidden: []int64{2, 3, 4, 5, 6, 7},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			entered := make(chan struct{})
			release := make(chan struct{})
			var once, releaseOnce sync.Once
			closeRelease := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(closeRelease)
			fs := &syncHookFs{Fs: afero.NewMemMapFs()}
			h := newBatchHarness(t, fs)
			if tc.prewritten != nil {
				require.NoError(t, h.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(batchTestSlot, tc.prewritten...)))
			}
			heldTemp := tc.heldTemp(h.storage.(*dataColumnStorageImpl))
			fs.onSync = func(name string) {
				if name == heldTemp {
					once.Do(func() { close(entered) })
					<-release
				}
			}

			writeDone := make(chan error, 1)
			go func() {
				writeDone <- h.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(batchTestSlot, batchTestIndices(batchTestColumns)...))
			}()
			select {
			case <-entered:
			case err := <-writeDone:
				t.Fatalf("the batch returned before syncing %s: %v", heldTemp, err)
			case <-time.After(10 * time.Second):
				t.Fatalf("%s was never synced", heldTemp)
			}

			for _, index := range tc.hidden {
				exists, err := h.storage.ColumnSidecarExists(t.Context(), batchTestSlot, root, index)
				require.NoError(t, err)
				require.False(t, exists, "column %d", index)
				_, err = h.storage.ReadColumnSidecarByColumnIndex(t.Context(), batchTestSlot, root, index)
				require.ErrorIs(t, err, os.ErrNotExist, "column %d", index)
			}
			scanDone := make(chan []uint64, 1)
			go func() {
				saved, err := h.storage.GetSavedColumnIndex(t.Context(), batchTestSlot, root)
				assert.NoError(t, err)
				scanDone <- saved
			}()
			select {
			case saved := <-scanDone:
				t.Fatalf("a scan saw %v while the batch was still being written", saved)
			case <-time.After(100 * time.Millisecond):
			}
			closeRelease()

			require.NoError(t, <-writeDone)
			require.Len(t, <-scanDone, batchTestColumns)
		})
	}
}

func TestWriteColumnSidecarsBatchBelowPruneFloor(t *testing.T) {
	fs := newCountingFs(afero.NewMemMapFs())
	h := newBatchHarness(t, fs)
	require.NoError(t, h.storage.PruneBelow(3*subdivisionSlot))

	err := h.storage.WriteColumnSidecarsBatch(t.Context(), common.Hash{1}, batchTestSidecars(batchTestSlot, 0, 1, 2))

	require.NoError(t, err)
	require.Zero(t, fs.count(opCreate))
	require.Empty(t, listFiles(t, fs))
	require.Empty(t, h.drainEvents())
	require.Empty(t, h.drainNotifications())
}

func TestWriteColumnSidecarsBatchConcurrentWithSingleWrites(t *testing.T) {
	fs := afero.NewBasePathFs(afero.NewOsFs(), t.TempDir())
	h := newBatchHarness(t, fs)
	const roots = 20
	indices := batchTestIndices(batchTestColumns)

	var wg sync.WaitGroup
	for r := range roots {
		root := common.BytesToHash([]byte{byte(r + 1)})
		wg.Go(func() {
			assert.NoError(t, h.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(batchTestSlot, indices...)))
		})
		for _, index := range indices {
			wg.Go(func() {
				assert.NoError(t, h.storage.WriteColumnSidecars(t.Context(), root, index, createTestDataColumnSidecar(batchTestSlot, index)))
			})
		}
	}
	wg.Wait()

	type key struct {
		root  common.Hash
		index uint64
	}
	seen := map[key]int{}
	for _, event := range h.drainEvents() {
		seen[key{event.BlockRoot, event.Index}]++
	}
	require.Len(t, seen, roots*batchTestColumns)
	for k, n := range seen {
		require.Equal(t, 1, n, "%v", k)
	}
	for r := range roots {
		requireAllReadable(t, h.storage, batchTestSlot, common.BytesToHash([]byte{byte(r + 1)}), indices...)
	}
	for _, file := range listFiles(t, fs) {
		require.False(t, strings.HasSuffix(file, tmpSuffix), file)
	}
}

func TestWriteColumnSidecarsBatchWaitsForTheBucketLock(t *testing.T) {
	fs := &blockingColumnFs{Fs: afero.NewMemMapFs(), createCalled: make(chan struct{})}
	h := newBatchHarness(t, fs)
	root := common.HexToHash("0x1234567890abcdef")
	fs.createPath = testPackFile(batchTestSlot, root) + tmpSuffix
	bucket := h.storage.(*dataColumnStorageImpl).forBucket(batchTestSlot / subdivisionSlot)
	bucket.Lock()
	unlock := sync.OnceFunc(bucket.Unlock)
	t.Cleanup(unlock)

	writeDone := make(chan error, 1)
	go func() {
		writeDone <- h.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(batchTestSlot, 0, 1))
	}()
	select {
	case <-fs.createCalled:
		t.Fatal("the pack was written while the bucket was locked")
	case <-time.After(100 * time.Millisecond):
	}
	unlock()

	select {
	case err := <-writeDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the batch never finished after the bucket was unlocked")
	}
	require.Equal(t, []uint64{0, 1}, packedColumns(t, fs, batchTestSlot, root, 128))
}
