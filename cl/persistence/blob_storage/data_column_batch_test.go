package blob_storage

import (
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

const (
	batchTestSlot    = 1000
	batchTestColumns = 8
)

type batchHarness struct {
	storage  DataColumnStorage
	emitters *beaconevents.EventEmitter
	events   chan *beaconevents.EventStream
}

func newBatchHarness(t testing.TB, fs afero.Fs) *batchHarness {
	t.Helper()
	cfg := *globalBeaconConfig
	cfg.NumberOfColumns = 128
	h := &batchHarness{
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
			files = append(files, path)
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
	fs := newCountingFs(afero.NewMemMapFs())
	h := newBatchHarness(t, fs)
	root := common.HexToHash("0x1234567890abcdef")
	indices := batchTestIndices(batchTestColumns)

	require.NoError(t, h.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(batchTestSlot, indices...)))

	require.Equal(t, 1, fs.count(opMkdirAll))
	require.Equal(t, batchTestColumns, fs.count(opCreate))
	require.Equal(t, batchTestColumns, fs.count(opSync))
	require.Equal(t, batchTestColumns, fs.count(opRename))
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

	require.Equal(t, 2, fs.count(opCreate))
	require.Equal(t, 2, fs.count(opRename))
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
	cases := map[string]func(fs *failingFs, tmpPath string){
		"sync": func(fs *failingFs, tmpPath string) { fs.failSyncAt(tmpPath, errInducedFailure) },
		"write": func(fs *failingFs, tmpPath string) {
			fs.failWritesAfter(tmpPath, 0, errInducedFailure)
		},
	}
	for name, inject := range cases {
		t.Run(name, func(t *testing.T) {
			fs := newFailingFs(afero.NewMemMapFs())
			h := newBatchHarness(t, fs)
			impl := h.storage.(*dataColumnStorageImpl)
			_, file := impl.path(batchTestSlot, root, 3)
			inject(fs, file+tmpSuffix)

			err := h.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(batchTestSlot, batchTestIndices(batchTestColumns)...))

			require.ErrorIs(t, err, errInducedFailure)
			require.Empty(t, listFiles(t, fs))
			require.Empty(t, h.drainEvents())
			require.Empty(t, h.drainNotifications())
			saved, err := h.storage.GetSavedColumnIndex(t.Context(), batchTestSlot, root)
			require.NoError(t, err)
			require.Empty(t, saved)
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

func TestWriteColumnSidecarsBatchRenameFailureKeepsRenamedColumns(t *testing.T) {
	fs := &renamePartiallyFailingFs{Fs: afero.NewMemMapFs(), failAfter: 3}
	h := newBatchHarness(t, fs)
	root := common.HexToHash("0x1234567890abcdef")

	err := h.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(batchTestSlot, batchTestIndices(batchTestColumns)...))

	require.ErrorIs(t, err, errInducedFailure)
	files := listFiles(t, fs)
	require.Len(t, files, 3)
	for _, file := range files {
		require.False(t, strings.HasSuffix(file, tmpSuffix), file)
	}
	saved, err := h.storage.GetSavedColumnIndex(t.Context(), batchTestSlot, root)
	require.NoError(t, err)
	require.Len(t, saved, 3)
	requireAllReadable(t, h.storage, batchTestSlot, root, int64(saved[0]), int64(saved[1]), int64(saved[2]))
	require.Len(t, h.drainEvents(), 3)
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
	entered := make(chan struct{})
	release := make(chan struct{})
	var once, releaseOnce sync.Once
	closeRelease := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(closeRelease)
	fs := &syncHookFs{Fs: afero.NewMemMapFs()}
	h := newBatchHarness(t, fs)
	impl := h.storage.(*dataColumnStorageImpl)
	_, blockedFile := impl.path(batchTestSlot, root, 3)
	fs.onSync = func(name string) {
		if name == blockedFile+tmpSuffix {
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
		t.Fatalf("the batch returned before syncing column 3: %v", err)
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
