// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package blob_storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"maps"
	"math"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

var errOpenHandle = errors.New("path has an open handle")

type packHeaderRow struct {
	name    string
	header  []byte
	size    int64
	width   uint64
	lengths []uint32 // nil for a row that must be rejected
}

func wantPackHeaderSize(width int) int { return 11 + 4*width }

func packTestLengths(width int, present map[int]uint32) []uint32 {
	lengths := make([]uint32, width)
	for i, n := range present {
		lengths[i] = n
	}
	return lengths
}

func packHeaderFor(t testing.TB, lengths []uint32) []byte {
	t.Helper()
	header := appendPackHeader(nil, lengths)
	require.Len(t, header, wantPackHeaderSize(len(lengths)))
	return header
}

func refreshPackHeaderCRC(header []byte) {
	body := len(header) - packCRCSize
	binary.LittleEndian.PutUint32(header[body:], crc32.Checksum(header[:body], castagnoli))
}

func packRowOf(t testing.TB, name string, lengths []uint32) packHeaderRow {
	t.Helper()
	size := int64(wantPackHeaderSize(len(lengths)))
	for _, n := range lengths {
		size += int64(n)
	}
	return packHeaderRow{name: name, header: packHeaderFor(t, lengths), size: size, width: uint64(len(lengths)), lengths: lengths}
}

func validPackRow(t testing.TB, width int) packHeaderRow {
	t.Helper()
	present := map[int]uint32{0: 3}
	if width > 1 {
		present[width-1] = 4
	}
	return packRowOf(t, fmt.Sprintf("accepts width %d", width), packTestLengths(width, present))
}

func rejectedPackRows(t testing.TB) []packHeaderRow {
	t.Helper()
	valid := validPackRow(t, 4)
	reject := func(name string, mutate func(row *packHeaderRow)) packHeaderRow {
		row := valid
		row.name = name
		row.header = bytes.Clone(valid.header)
		row.lengths = nil
		mutate(&row)
		return row
	}
	widest := validPackRow(t, math.MaxUint16)
	tooLong := packRowOf(t, "a frame above maxFrame", packTestLengths(4, map[int]uint32{0: maxFrame + 1}))
	tooLong.lengths = nil
	trailing := validPackRow(t, 4)
	trailing.name, trailing.size, trailing.lengths = "one trailing byte", trailing.size+1, nil
	truncated := validPackRow(t, 4)
	truncated.name, truncated.size, truncated.lengths = "frames end one byte past the file", truncated.size-1, nil
	empty := packRowOf(t, "no column present", packTestLengths(4, nil))
	empty.lengths = nil
	return []packHeaderRow{
		tooLong, trailing, truncated, empty,
		reject("fewer than 7 bytes", func(row *packHeaderRow) { row.header, row.size = row.header[:6], 6 }),
		reject("bad magic", func(row *packHeaderRow) {
			row.header[0] ^= 0xff
			refreshPackHeaderCRC(row.header)
		}),
		reject("unknown version", func(row *packHeaderRow) {
			row.header[4] = 2
			refreshPackHeaderCRC(row.header)
		}),
		reject("header width differs from the configured width", func(row *packHeaderRow) { row.width++ }),
		reject("configured width above MaxUint16", func(row *packHeaderRow) {
			row.header, row.size, row.width = widest.header, widest.size, math.MaxUint16+1
		}),
		reject("size below the header", func(row *packHeaderRow) { row.size = int64(wantPackHeaderSize(4)) - 1 }),
		reject("one length bit flipped", func(row *packHeaderRow) {
			row.header[7+4*1] ^= 1
			row.size++
		}),
	}
}

func TestParsePackHeader(t *testing.T) {
	rows := rejectedPackRows(t)
	for _, width := range []int{1, 128, math.MaxUint16} {
		rows = append(rows, validPackRow(t, width))
	}
	rows = append(rows, packRowOf(t, "a frame of exactly maxFrame", packTestLengths(4, map[int]uint32{0: maxFrame})))
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			frames, err := parsePackHeader(bytes.NewReader(row.header), row.size, row.width)
			if row.lengths == nil {
				require.ErrorIs(t, err, errCorruptPack)
				return
			}
			require.NoError(t, err)
			require.Len(t, frames, len(row.lengths))
			off := uint64(wantPackHeaderSize(len(row.lengths)))
			for i, n := range row.lengths {
				if n == 0 {
					require.Zero(t, frames[i].n, "column %d is absent", i)
					continue
				}
				require.Equal(t, packFrame{off: off, n: n}, frames[i], "column %d", i)
				off += uint64(n)
			}
		})
	}
}

func packFileBytes(row packHeaderRow) []byte {
	data := bytes.Clone(row.header[:min(int64(len(row.header)), row.size)])
	return append(data, make([]byte, max(0, row.size-int64(len(data))))...)
}

func FuzzParsePackHeader(f *testing.F) {
	for _, width := range []int{1, 2, 128, 255, 256, math.MaxUint16} {
		row := validPackRow(f, width)
		f.Add(packFileBytes(row), row.width, false)
	}
	for _, row := range rejectedPackRows(f) {
		if row.size <= 1<<20 { // a row declaring a larger file is covered by TestParsePackHeader
			f.Add(packFileBytes(row), row.width, false)
		}
	}
	f.Fuzz(func(t *testing.T, data []byte, width uint64, fixCRC bool) {
		h := 11 + 4*width
		if fixCRC && width <= math.MaxUint16 && uint64(len(data)) >= h {
			data = bytes.Clone(data)
			binary.LittleEndian.PutUint32(data[h-4:], crc32.Checksum(data[:h-4], crc32.MakeTable(crc32.Castagnoli)))
		}
		frames, err := parsePackHeader(bytes.NewReader(data), int64(len(data)), width)
		if err != nil {
			require.ErrorIs(t, err, errCorruptPack)
			return
		}
		require.Len(t, frames, int(width))
		end, present := h, 0
		for i, frame := range frames {
			if frame.n == 0 {
				continue
			}
			require.LessOrEqual(t, frame.n, maxFrame, "column %d", i)
			require.Equal(t, end, frame.off, "column %d", i)
			end += uint64(frame.n)
			present++
		}
		require.Positive(t, present)
		require.Equal(t, uint64(len(data)), end)
		crc := crc32.Checksum(data[:h-4], crc32.MakeTable(crc32.Castagnoli))
		require.Equal(t, binary.LittleEndian.Uint32(data[h-4:h]), crc)
	})
}

// openHandleFs counts the open handles per path. Like Windows, it refuses to rename a path
// onto or from a path that has one, and to remove a path that has one.
type openHandleFs struct {
	afero.Fs
	mu   sync.Mutex
	open map[string]int
}

func newOpenHandleFs(fs afero.Fs) *openHandleFs {
	return &openHandleFs{Fs: fs, open: map[string]int{}}
}

func (f *openHandleFs) openCount(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.open[name]
}

func (f *openHandleFs) openHandles() map[string]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return maps.Clone(f.open)
}

func (f *openHandleFs) track(name string, fh afero.File, err error) (afero.File, error) {
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.open[name]++
	return &trackedFile{File: fh, fs: f, name: name}, nil
}

func (f *openHandleFs) Open(name string) (afero.File, error) {
	fh, err := f.Fs.Open(name)
	return f.track(name, fh, err)
}

func (f *openHandleFs) Create(name string) (afero.File, error) {
	fh, err := f.Fs.Create(name)
	return f.track(name, fh, err)
}

func (f *openHandleFs) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	fh, err := f.Fs.OpenFile(name, flag, perm)
	return f.track(name, fh, err)
}

func (f *openHandleFs) Rename(oldname, newname string) error {
	if f.openCount(oldname) > 0 || f.openCount(newname) > 0 {
		return fmt.Errorf("rename %s to %s: %w", oldname, newname, errOpenHandle)
	}
	return f.Fs.Rename(oldname, newname)
}

func (f *openHandleFs) Remove(name string) error {
	if f.openCount(name) > 0 {
		return fmt.Errorf("remove %s: %w", name, errOpenHandle)
	}
	return f.Fs.Remove(name)
}

type trackedFile struct {
	afero.File
	fs     *openHandleFs
	name   string
	closed sync.Once
}

func (f *trackedFile) Close() error {
	f.closed.Do(func() {
		f.fs.mu.Lock()
		defer f.fs.mu.Unlock()
		if f.fs.open[f.name]--; f.fs.open[f.name] == 0 {
			delete(f.fs.open, f.name)
		}
	})
	return f.File.Close()
}

func testPackFile(slot uint64, root common.Hash) string {
	return fmt.Sprintf("%d/packs/%s.pack", slot/subdivisionSlot, root.String())
}

func packFrames(t testing.TB, fs afero.Fs, slot uint64, root common.Hash, width uint64) []packFrame {
	t.Helper()
	fh, err := fs.Open(testPackFile(slot, root))
	require.NoError(t, err)
	defer fh.Close()
	info, err := fh.Stat()
	require.NoError(t, err)
	frames, err := parsePackHeader(fh, info.Size(), width)
	require.NoError(t, err)
	return frames
}

func packedColumns(t testing.TB, fs afero.Fs, slot uint64, root common.Hash, width uint64) []uint64 {
	t.Helper()
	var columns []uint64
	for i, frame := range packFrames(t, fs, slot, root, width) {
		if frame.n != 0 {
			columns = append(columns, uint64(i))
		}
	}
	return columns
}

// packFaultFs makes the pack at path fail: Open returns openErr, and the handles it opens fail
// Stat and ReadAt with the configured errors. ReadAt returns io.EOF for a range that passes
// eofAfter when that is not negative.
type packFaultFs struct {
	afero.Fs
	path     string
	openErr  error
	statErr  error
	readErr  error
	eofAfter int64
}

func newPackFaultFs(fs afero.Fs, path string) *packFaultFs {
	return &packFaultFs{Fs: fs, path: path, eofAfter: -1}
}

func (f *packFaultFs) Open(name string) (afero.File, error) {
	if name == f.path && f.openErr != nil {
		return nil, f.openErr
	}
	fh, err := f.Fs.Open(name)
	if err != nil || name != f.path {
		return fh, err
	}
	return &packFaultFile{File: fh, fs: f}, nil
}

type packFaultFile struct {
	afero.File
	fs *packFaultFs
}

func (f *packFaultFile) Stat() (os.FileInfo, error) {
	if f.fs.statErr != nil {
		return nil, f.fs.statErr
	}
	return f.File.Stat()
}

func (f *packFaultFile) ReadAt(p []byte, off int64) (int, error) {
	if f.fs.readErr != nil {
		return 0, f.fs.readErr
	}
	if limit := f.fs.eofAfter; limit >= 0 && off+int64(len(p)) > limit {
		n := 0
		if off < limit {
			n, _ = f.File.ReadAt(p[:limit-off], off)
		}
		return n, io.EOF
	}
	return f.File.ReadAt(p, off)
}

func flipPackMagic(t testing.TB, fs afero.Fs, slot uint64, root common.Hash) {
	t.Helper()
	data, err := afero.ReadFile(fs, testPackFile(slot, root))
	require.NoError(t, err)
	data[0] ^= 0xff
	require.NoError(t, afero.WriteFile(fs, testPackFile(slot, root), data, 0o644))
}

func TestOpenPackClosesTheFileUnlessItReturnsIt(t *testing.T) {
	const slot = batchTestSlot
	root := common.HexToHash("0x1234567890abcdef")
	cases := []struct {
		name    string
		indices []int64
		fault   func(t *testing.T, mem afero.Fs, faulty *packFaultFs)
	}{
		{"magic flipped", []int64{0, 1, 2, 3}, func(t *testing.T, mem afero.Fs, _ *packFaultFs) { flipPackMagic(t, mem, slot, root) }},
		{"stat fails", []int64{0, 1, 2, 3}, func(_ *testing.T, _ afero.Fs, faulty *packFaultFs) { faulty.statErr = errInducedFailure }},
		{"read fails", []int64{0, 1, 2, 3}, func(_ *testing.T, _ afero.Fs, faulty *packFaultFs) { faulty.readErr = errInducedFailure }},
		{"no fault", []int64{0, 1, 2, 3}, func(*testing.T, afero.Fs, *packFaultFs) {}},
		{"column absent from a valid pack", []int64{0, 2, 3}, func(*testing.T, afero.Fs, *packFaultFs) {}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mem := afero.NewMemMapFs()
			faulty := newPackFaultFs(mem, testPackFile(slot, root))
			handles := newOpenHandleFs(faulty)
			require.NoError(t, newBatchHarness(t, handles).storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(slot, tc.indices...)))
			requireAllReadable(t, newBatchHarness(t, handles).storage, slot, root, tc.indices...)
			tc.fault(t, mem, faulty)

			operations := map[string]func(storage DataColumnStorage){
				"read": func(storage DataColumnStorage) {
					_, _ = storage.ReadColumnSidecarByColumnIndex(t.Context(), slot, root, 1)
				},
				"exists": func(storage DataColumnStorage) { _, _ = storage.ColumnSidecarExists(t.Context(), slot, root, 1) },
				"scan":   func(storage DataColumnStorage) { _, _ = storage.GetSavedColumnIndex(t.Context(), slot, root) },
				"batch": func(storage DataColumnStorage) {
					_ = storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(slot, 4, 5))
				},
				"remove a column the pack does not hold": func(storage DataColumnStorage) {
					_ = storage.RemoveColumnSidecars(t.Context(), slot, root, 9)
				},
			}
			for name, operate := range operations {
				operate(newBatchHarness(t, handles).storage)
				require.Zero(t, handles.openCount(faulty.path), name)
			}
		})
	}
}

func blobSidecar(slot uint64, index int64) *cltypes.DataColumnSidecar {
	sidecar := createTestDataColumnSidecar(slot, index)
	addTestBlob(sidecar)
	return sidecar
}

func TestPackedColumnStreamsTheBytesOfALooseFile(t *testing.T) {
	const slot = batchTestSlot
	h := newBatchHarness(t, afero.NewMemMapFs())
	looseRoot, packedRoot := common.Hash{1}, common.Hash{2}
	require.NoError(t, h.storage.WriteColumnSidecars(t.Context(), looseRoot, 1, blobSidecar(slot, 1)))
	require.NoError(t, h.storage.WriteColumnSidecarsBatch(t.Context(), packedRoot, []*cltypes.DataColumnSidecar{blobSidecar(slot, 1), blobSidecar(slot, 2)}))
	require.Equal(t, []uint64{1, 2}, packedColumns(t, h.fs, slot, packedRoot, 128))

	var loose, packed, want bytes.Buffer
	require.NoError(t, h.storage.WriteStream(&loose, slot, looseRoot, 1))
	require.NoError(t, h.storage.WriteStream(&packed, slot, packedRoot, 1))
	require.NoError(t, ssz_snappy.EncodeAndWrite(&want, blobSidecar(slot, 1)))

	require.Equal(t, want.Bytes(), loose.Bytes())
	require.Equal(t, want.Bytes(), packed.Bytes())
}

func TestPackedLookupsRejectOutOfRangeIndices(t *testing.T) {
	const slot = batchTestSlot
	root := common.HexToHash("0x1234567890abcdef")
	mem := afero.NewMemMapFs()
	require.NoError(t, newBatchHarness(t, mem).storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(slot, 0, 1, 2, 3)))
	storage := newBatchHarness(t, mem).storage

	for _, index := range []int64{-1, 128} {
		require.NotPanics(t, func() {
			_, err := storage.ReadColumnSidecarByColumnIndex(t.Context(), slot, root, index)
			require.ErrorIs(t, err, os.ErrNotExist, "read %d", index)
			exists, err := storage.ColumnSidecarExists(t.Context(), slot, root, index)
			require.NoError(t, err, "exists %d", index)
			require.False(t, exists, "exists %d", index)
		})
	}
	require.NotPanics(t, func() {
		require.ErrorIs(t, storage.WriteStream(io.Discard, slot, root, 128), os.ErrNotExist)
	})
}

func columnWarns(run func()) []string {
	records := make(chan *log.Record, 1024)
	previous := log.Root().GetHandler()
	defer log.Root().SetHandler(previous)
	log.Root().SetHandler(log.FilterHandler(func(r *log.Record) bool {
		return r.Lvl == log.LvlWarn && strings.HasPrefix(r.Msg, "[blob_storage]")
	}, log.ChannelHandler(records)))
	run()
	warns := make([]string, len(records))
	for i := range warns {
		warns[i] = (<-records).Msg
	}
	return warns
}

func eventIndices(h *batchHarness) []uint64 {
	var indices []uint64
	for _, event := range h.drainEvents() {
		indices = append(indices, event.Index)
	}
	return indices
}

func TestCorruptPackReadsAsMissing(t *testing.T) {
	const slot = batchTestSlot
	const corruptWarn = "[blob_storage] ignoring corrupt column pack"
	root := common.HexToHash("0x1234567890abcdef")
	mem := afero.NewMemMapFs()
	require.NoError(t, newBatchHarness(t, mem).storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(slot, batchTestIndices(8)...)))
	seeded := newBatchHarness(t, mem).storage
	requireAllReadable(t, seeded, slot, root, batchTestIndices(8)...)
	saved, err := seeded.GetSavedColumnIndex(t.Context(), slot, root)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1, 2, 3, 4, 5, 6, 7}, saved)
	flipPackMagic(t, mem, slot, root)
	packBytes, err := afero.ReadFile(mem, testPackFile(slot, root))
	require.NoError(t, err)

	require.Equal(t, []string{corruptWarn}, columnWarns(func() {
		_, err := seeded.ReadColumnSidecarByColumnIndex(t.Context(), slot, root, 5)
		require.ErrorIs(t, err, os.ErrNotExist)
	}))
	exists, err := seeded.ColumnSidecarExists(t.Context(), slot, root, 5)
	require.NoError(t, err)
	require.False(t, exists, "the bitmap entry seeded from the valid pack must be forgotten")
	saved, err = seeded.GetSavedColumnIndex(t.Context(), slot, root)
	require.NoError(t, err)
	require.Empty(t, saved)

	lookups := map[string]func(storage DataColumnStorage){
		"read": func(storage DataColumnStorage) {
			_, err := storage.ReadColumnSidecarByColumnIndex(t.Context(), slot, root, 1)
			require.ErrorIs(t, err, os.ErrNotExist)
		},
		"exists": func(storage DataColumnStorage) {
			exists, err := storage.ColumnSidecarExists(t.Context(), slot, root, 1)
			require.NoError(t, err)
			require.False(t, exists)
		},
		"stream": func(storage DataColumnStorage) {
			require.ErrorIs(t, storage.WriteStream(io.Discard, slot, root, 1), os.ErrNotExist)
		},
		"scan": func(storage DataColumnStorage) {
			saved, err := storage.GetSavedColumnIndex(t.Context(), slot, root)
			require.NoError(t, err)
			require.Empty(t, saved)
		},
	}
	for name, lookup := range lookups {
		storage := newBatchHarness(t, mem).storage
		require.Equal(t, []string{corruptWarn}, columnWarns(func() { lookup(storage) }), name)
		require.Empty(t, columnWarns(func() { lookup(storage) }), "%s: a pack already reported as corrupt is not warned about again", name)
	}
	other := common.HexToHash("0xfedcba")
	require.NoError(t, newBatchHarness(t, mem).storage.WriteColumnSidecarsBatch(t.Context(), other, batchTestSidecars(slot, 0, 1)))
	flipPackMagic(t, mem, slot, other)
	require.Equal(t, []string{corruptWarn}, columnWarns(func() {
		_, err := seeded.ReadColumnSidecarByColumnIndex(t.Context(), slot, other, 0)
		require.ErrorIs(t, err, os.ErrNotExist)
	}), "each corrupt pack is warned about once")

	h := newBatchHarness(t, mem)
	require.NoError(t, h.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(slot, 0, 1, 2, 3)))

	after, err := afero.ReadFile(mem, testPackFile(slot, root))
	require.NoError(t, err)
	require.Equal(t, packBytes, after)
	for _, file := range listFiles(t, mem) {
		require.False(t, strings.HasSuffix(file, tmpSuffix), file)
	}
	require.ElementsMatch(t, []uint64{0, 1, 2, 3}, eventIndices(h))
	saved, err = h.storage.GetSavedColumnIndex(t.Context(), slot, root)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1, 2, 3}, saved)

	require.NoError(t, h.storage.RemoveColumnSidecars(t.Context(), slot, root, 1))
	after, err = afero.ReadFile(mem, testPackFile(slot, root))
	require.NoError(t, err)
	require.Equal(t, packBytes, after, "Remove leaves a corrupt pack in place")
}

func TestPruneForgetsTheWarnedCorruptPacksItRemoves(t *testing.T) {
	mem := afero.NewMemMapFs()
	h := newBatchHarness(t, mem)
	pruned, kept := common.Hash{1}, common.Hash{2}
	for root, slot := range map[common.Hash]uint64{pruned: batchTestSlot, kept: subdivisionSlot + batchTestSlot} {
		require.NoError(t, h.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(slot, 0, 1)))
		flipPackMagic(t, mem, slot, root)
		exists, err := h.storage.ColumnSidecarExists(t.Context(), slot, root, 0)
		require.NoError(t, err)
		require.False(t, exists)
	}

	require.NoError(t, h.storage.PruneBelow(subdivisionSlot))

	_, warned := implOf(h).warnedCorruptPacks.Load(pruned)
	require.False(t, warned)
	_, warned = implOf(h).warnedCorruptPacks.Load(kept)
	require.True(t, warned)
}

func TestPackInspectionErrors(t *testing.T) {
	const slot = batchTestSlot
	root := common.HexToHash("0x1234567890abcdef")
	faults := map[string]struct {
		err    error
		inject func(faulty *packFaultFs)
	}{
		"open fails": {os.ErrPermission, func(faulty *packFaultFs) { faulty.openErr = os.ErrPermission }},
		"stat fails": {errInducedFailure, func(faulty *packFaultFs) { faulty.statErr = errInducedFailure }},
		"read fails": {errInducedFailure, func(faulty *packFaultFs) { faulty.readErr = errInducedFailure }},
	}
	for name, tc := range faults {
		t.Run(name, func(t *testing.T) {
			mem := afero.NewMemMapFs()
			faulty := newPackFaultFs(mem, testPackFile(slot, root))
			writer := newBatchHarness(t, faulty).storage
			require.NoError(t, writer.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(slot, 0, 1, 2, 3)))
			require.NoError(t, writer.WriteColumnSidecars(t.Context(), root, 4, createTestDataColumnSidecar(slot, 4)))
			requireAllReadable(t, writer, slot, root, 0, 1, 2, 3, 4)
			packBytes, err := afero.ReadFile(mem, faulty.path)
			require.NoError(t, err)
			seeded := newBatchHarness(t, faulty).storage
			saved, err := seeded.GetSavedColumnIndex(t.Context(), slot, root)
			require.NoError(t, err)
			require.Equal(t, []uint64{0, 1, 2, 3, 4}, saved)
			tc.inject(faulty)

			h := newBatchHarness(t, faulty)
			_, err = h.storage.ReadColumnSidecarByColumnIndex(t.Context(), slot, root, 1)
			require.ErrorIs(t, err, tc.err)
			require.ErrorIs(t, h.storage.WriteStream(io.Discard, slot, root, 1), tc.err)
			_, err = h.storage.ColumnSidecarExists(t.Context(), slot, root, 1)
			require.ErrorIs(t, err, tc.err)
			requireAllReadable(t, h.storage, slot, root, 4)
			require.NoError(t, h.storage.WriteStream(io.Discard, slot, root, 4))
			exists, err := h.storage.ColumnSidecarExists(t.Context(), slot, root, 4)
			require.NoError(t, err)
			require.True(t, exists)
			exists, err = seeded.ColumnSidecarExists(t.Context(), slot, root, 1)
			require.NoError(t, err)
			require.True(t, exists, "a bitmap seeded before the fault answers from memory")
			_, err = h.storage.GetSavedColumnIndex(t.Context(), slot, root)
			require.ErrorIs(t, err, tc.err)
			require.NotContains(t, h.storage.(*dataColumnStorageImpl).bitmaps.entries, root)

			require.NoError(t, h.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(slot, batchTestIndices(8)...)))

			after, err := afero.ReadFile(mem, faulty.path)
			require.NoError(t, err)
			require.Equal(t, packBytes, after)
			wantFiles := []string{faulty.path}
			for i := range uint64(8) {
				_, file := h.storage.(*dataColumnStorageImpl).path(slot, root, i)
				wantFiles = append(wantFiles, file)
			}
			require.ElementsMatch(t, wantFiles, listFiles(t, mem))
			require.ElementsMatch(t, []uint64{0, 1, 2, 3, 5, 6, 7}, eventIndices(h))
		})
	}
}

func TestSavedColumnScanOpensThePackOnceAndStatsOnlyUncoveredColumns(t *testing.T) {
	const slot = batchTestSlot
	root := common.HexToHash("0x1234567890abcdef")
	fs := newCountingFs(afero.NewMemMapFs())
	writer := newBatchHarness(t, fs).storage
	require.NoError(t, writer.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(slot, 0, 1, 2, 3, 4, 5)))
	require.NoError(t, writer.WriteColumnSidecars(t.Context(), root, 6, createTestDataColumnSidecar(slot, 6)))
	require.Equal(t, []uint64{0, 1, 2, 3, 4, 5}, packedColumns(t, fs, slot, root, 128))
	fs.reset()

	storage := newBatchHarness(t, fs).storage
	saved, err := storage.GetSavedColumnIndex(t.Context(), slot, root)

	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1, 2, 3, 4, 5, 6}, saved)
	packOpens := 0
	for _, path := range fs.paths(opOpen) {
		if path == testPackFile(slot, root) {
			packOpens++
		}
	}
	require.Equal(t, 1, packOpens)
	require.Equal(t, 128-6, fs.count(opStat))
	exists, err := storage.ColumnSidecarExists(t.Context(), slot, root, 3)
	require.NoError(t, err)
	require.True(t, exists)
	requireAllReadable(t, storage, slot, root, 3)
}

func TestEarliestStoredSlotCountsABucketHoldingOnlyAPack(t *testing.T) {
	handles := newOpenHandleFs(afero.NewMemMapFs())
	h := newBatchHarness(t, handles)
	packRoot, tempRoot, looseRoot := common.Hash{3}, common.Hash{4}, common.Hash{5}
	packSlot, looseSlot := uint64(3*subdivisionSlot+1), uint64(5*subdivisionSlot+1)
	require.NoError(t, h.storage.WriteColumnSidecarsBatch(t.Context(), packRoot, batchTestSidecars(packSlot, 0, 1)))
	require.Equal(t, []uint64{0, 1}, packedColumns(t, handles, packSlot, packRoot, 128))
	tempFile := testPackFile(4*subdivisionSlot, tempRoot) + tmpSuffix
	require.NoError(t, handles.MkdirAll(path.Dir(tempFile), 0o755))
	require.NoError(t, afero.WriteFile(handles, tempFile, []byte{1}, 0o644))
	require.NoError(t, h.storage.WriteColumnSidecars(t.Context(), looseRoot, 0, createTestDataColumnSidecar(looseSlot, 0)))

	earliest, err := h.storage.EarliestStoredSlot()

	require.NoError(t, err)
	require.Equal(t, uint64(3*subdivisionSlot), earliest)
	olderBinaryHolds, err := h.storage.(*dataColumnStorageImpl).holdsSidecar("3")
	require.NoError(t, err)
	require.False(t, olderBinaryHolds)
	require.Empty(t, handles.openHandles())
}

func TestEarliestStoredSlotSkipsAPackItCannotServe(t *testing.T) {
	const badSlot, goodSlot = 3*subdivisionSlot + 1, 5*subdivisionSlot + 1
	badRoot, goodRoot := common.HexToHash("0xabcdef1234567890"), common.Hash{5}
	writePack := func(t *testing.T, fs afero.Fs, slot uint64, root common.Hash) {
		t.Helper()
		require.NoError(t, newBatchHarness(t, fs).storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(slot, 0, 1)))
		require.Equal(t, []uint64{0, 1}, packedColumns(t, fs, slot, root, 128))
	}

	t.Run("a corrupt pack is skipped for a later bucket", func(t *testing.T) {
		mem := afero.NewMemMapFs()
		writePack(t, mem, badSlot, badRoot)
		flipPackMagic(t, mem, badSlot, badRoot)
		writePack(t, mem, goodSlot, goodRoot)

		earliest, err := newBatchHarness(t, mem).storage.EarliestStoredSlot()

		require.NoError(t, err)
		require.Equal(t, uint64(5*subdivisionSlot), earliest)
	})

	t.Run("a corrupt pack alone holds nothing", func(t *testing.T) {
		mem := afero.NewMemMapFs()
		writePack(t, mem, badSlot, badRoot)
		flipPackMagic(t, mem, badSlot, badRoot)

		earliest, err := newBatchHarness(t, mem).storage.EarliestStoredSlot()

		require.NoError(t, err)
		require.Zero(t, earliest)
	})

	t.Run("an unreadable pack is skipped for a later bucket", func(t *testing.T) {
		mem := afero.NewMemMapFs()
		writePack(t, mem, badSlot, badRoot)
		writePack(t, mem, goodSlot, goodRoot)

		earliest, err := newBatchHarness(t, openFailingFs{Fs: mem, name: testPackFile(badSlot, badRoot)}).storage.EarliestStoredSlot()

		require.NoError(t, err)
		require.Equal(t, uint64(5*subdivisionSlot), earliest)
	})

	t.Run("an unreadable pack alone returns its error", func(t *testing.T) {
		mem := afero.NewMemMapFs()
		writePack(t, mem, badSlot, badRoot)

		_, err := newBatchHarness(t, openFailingFs{Fs: mem, name: testPackFile(badSlot, badRoot)}).storage.EarliestStoredSlot()

		require.ErrorIs(t, err, os.ErrPermission)
	})

	t.Run("an unreadable pack does not hide a valid pack in the same bucket", func(t *testing.T) {
		mem := afero.NewMemMapFs()
		unreadableRoot, validRoot := common.Hash{1}, common.Hash{2}
		writePack(t, mem, badSlot, unreadableRoot)
		writePack(t, mem, badSlot, validRoot)
		infos, err := afero.ReadDir(mem, path.Dir(testPackFile(badSlot, unreadableRoot)))
		require.NoError(t, err)
		require.Equal(t, unreadableRoot.String()+packSuffix, infos[0].Name(), "the unreadable pack comes first in directory order")

		earliest, err := newBatchHarness(t, openFailingFs{Fs: mem, name: testPackFile(badSlot, unreadableRoot)}).storage.EarliestStoredSlot()

		require.NoError(t, err)
		require.Equal(t, uint64(3*subdivisionSlot), earliest)
	})

	t.Run("a corrupt pack found by the scan drops the cached columns", func(t *testing.T) {
		mem := afero.NewMemMapFs()
		require.NoError(t, newBatchHarness(t, mem).storage.WriteColumnSidecarsBatch(t.Context(), badRoot, batchTestSidecars(badSlot, 0, 1, 2, 3)))
		storage := newBatchHarness(t, mem).storage
		saved, err := storage.GetSavedColumnIndex(t.Context(), badSlot, badRoot)
		require.NoError(t, err)
		require.Equal(t, []uint64{0, 1, 2, 3}, saved)
		flipPackMagic(t, mem, badSlot, badRoot)

		_, err = storage.EarliestStoredSlot()

		require.NoError(t, err)
		exists, err := storage.ColumnSidecarExists(t.Context(), badSlot, badRoot, 1)
		require.NoError(t, err)
		require.False(t, exists)
		saved, err = storage.GetSavedColumnIndex(t.Context(), badSlot, badRoot)
		require.NoError(t, err)
		require.Empty(t, saved)
	})

	t.Run("a pack under a name that is not its root's is not counted", func(t *testing.T) {
		mem := afero.NewMemMapFs()
		writePack(t, mem, badSlot, badRoot)
		packBytes, err := afero.ReadFile(mem, testPackFile(badSlot, badRoot))
		require.NoError(t, err)
		require.NoError(t, mem.Remove(testPackFile(badSlot, badRoot)))
		names := []string{
			"x",
			badRoot.String()[:len(badRoot.String())-1],
			"0x" + strings.ToUpper(badRoot.String()[2:]),
		}
		for _, name := range names {
			require.NoError(t, afero.WriteFile(mem, "3/packs/"+name+".pack", packBytes, 0o644))
		}

		earliest, err := newBatchHarness(t, mem).storage.EarliestStoredSlot()

		require.NoError(t, err)
		require.Zero(t, earliest)
	})
}

func TestWriteColumnSidecarsBatchEmitsEventsInInputOrder(t *testing.T) {
	const slot = batchTestSlot
	root := common.HexToHash("0x1234567890abcdef")
	mem := afero.NewMemMapFs()
	h := newBatchHarness(t, mem)

	require.NoError(t, h.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(slot, 5, 1, 3)))

	require.Equal(t, []uint64{5, 1, 3}, eventIndices(h))
	frames := packFrames(t, mem, slot, root, 128)
	require.Equal(t, frames[1].off+uint64(frames[1].n), frames[3].off)
	require.Equal(t, frames[3].off+uint64(frames[3].n), frames[5].off)
	requireAllReadable(t, h.storage, slot, root, 5, 1, 3)
}

func writeLooseCopy(t testing.TB, fs afero.Fs, slot uint64, root common.Hash, sidecar *cltypes.DataColumnSidecar) {
	t.Helper()
	dir := fmt.Sprintf("%d", slot/subdivisionSlot)
	require.NoError(t, fs.MkdirAll(dir, 0o755))
	fh, err := fs.Create(fmt.Sprintf("%s/%s_%d", dir, root.String(), sidecar.Index))
	require.NoError(t, err)
	require.NoError(t, ssz_snappy.EncodeAndWrite(fh, sidecar))
	require.NoError(t, fh.Close())
}

func TestACorruptLooseFileShadowsAPackedCopy(t *testing.T) {
	const slot = batchTestSlot
	root := common.HexToHash("0x1234567890abcdef")
	mem := afero.NewMemMapFs()
	require.NoError(t, newBatchHarness(t, mem).storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(slot, 0, 1, 2, 3)))
	require.Equal(t, []uint64{0, 1, 2, 3}, packedColumns(t, mem, slot, root, 128))
	requireAllReadable(t, newBatchHarness(t, mem).storage, slot, root, 0, 1, 2, 3)
	garbage := []byte{0x01, 0xff, 0xff}
	require.NoError(t, afero.WriteFile(mem, fmt.Sprintf("0/%s_1", root.String()), garbage, 0o644))
	storage := newBatchHarness(t, mem).storage

	_, err := storage.ReadColumnSidecarByColumnIndex(t.Context(), slot, root, 1)
	require.ErrorIs(t, err, os.ErrNotExist)
	var streamed bytes.Buffer
	require.NoError(t, storage.WriteStream(&streamed, slot, root, 1))
	require.Equal(t, garbage, streamed.Bytes())
	exists, err := storage.ColumnSidecarExists(t.Context(), slot, root, 1)
	require.NoError(t, err)
	require.True(t, exists)
}

func TestColumnsStoredInBothALooseFileAndThePack(t *testing.T) {
	const slot = batchTestSlot
	root := common.HexToHash("0x1234567890abcdef")
	fs := newCountingFs(afero.NewMemMapFs())
	a := newBatchHarness(t, fs)
	require.NoError(t, a.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(slot, 0, 1, 2, 3)))
	olderBinaryStoresColumn, err := a.storage.(*dataColumnStorageImpl).bucketStore.exists(slot, root, 1)
	require.NoError(t, err)
	require.False(t, olderBinaryStoresColumn)
	writeLooseCopy(t, fs, slot, root, blobSidecar(slot, 1))
	writeLooseCopy(t, fs, slot, root, blobSidecar(slot, 2))
	fs.reset()

	b := newBatchHarness(t, fs)
	read, err := b.storage.ReadColumnSidecarByColumnIndex(t.Context(), slot, root, 1)
	require.NoError(t, err)
	require.Equal(t, 1, read.Column.Len(), "the loose copy is read, not the packed one")
	var streamed, want bytes.Buffer
	require.NoError(t, b.storage.WriteStream(&streamed, slot, root, 1))
	require.NoError(t, ssz_snappy.EncodeAndWrite(&want, blobSidecar(slot, 1)))
	require.Equal(t, want.Bytes(), streamed.Bytes())
	exists, err := b.storage.ColumnSidecarExists(t.Context(), slot, root, 1)
	require.NoError(t, err)
	require.True(t, exists)
	saved, err := b.storage.GetSavedColumnIndex(t.Context(), slot, root)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1, 2, 3}, saved)

	fs.reset()
	require.NoError(t, b.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(slot, 0, 1, 2, 3)))
	require.Zero(t, fs.count(opCreate))
	require.Empty(t, b.drainEvents())

	require.NoError(t, b.storage.PruneBelow(subdivisionSlot))
	require.Empty(t, listFiles(t, fs))
	packsExist, err := afero.DirExists(fs, path.Dir(testPackFile(slot, root)))
	require.NoError(t, err)
	require.False(t, packsExist)
}

func implOf(h *batchHarness) *dataColumnStorageImpl { return h.storage.(*dataColumnStorageImpl) }

func seededColumns(t testing.TB, h *batchHarness, slot uint64, root common.Hash) []uint64 {
	t.Helper()
	columns, known := implOf(h).bitmaps.columns(implOf(h).columnKey(slot, root))
	require.True(t, known, "the bitmap entry is gone")
	return columns
}

func TestRemoveColumnSidecarsRewritesThePack(t *testing.T) {
	const slot = batchTestSlot
	root := common.HexToHash("0x1234567890abcdef")
	fs := newCountingFs(afero.NewMemMapFs())
	h := newBatchHarness(t, fs)
	require.NoError(t, h.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(slot, 0, 1, 2, 3)))
	writeLooseCopy(t, fs, slot, root, blobSidecar(slot, 1))
	require.NoError(t, h.storage.WriteColumnSidecars(t.Context(), root, 4, createTestDataColumnSidecar(slot, 4)))
	saved, err := h.storage.GetSavedColumnIndex(t.Context(), slot, root)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1, 2, 3, 4}, saved)
	fs.reset()

	require.NoError(t, h.storage.RemoveColumnSidecars(t.Context(), slot, root, 1, 4))

	require.Equal(t, []uint64{0, 2, 3}, packedColumns(t, fs, slot, root, 128))
	require.Equal(t, []string{testPackFile(slot, root) + tmpSuffix}, fs.paths(opCreate))
	require.Equal(t, 1, fs.count(opSync))
	require.Equal(t, []string{testPackFile(slot, root) + tmpSuffix}, fs.paths(opRename))
	require.Equal(t, []string{testPackFile(slot, root)}, listFiles(t, fs))
	_, err = h.storage.ReadColumnSidecarByColumnIndex(t.Context(), slot, root, 1)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Equal(t, []uint64{0, 2, 3}, seededColumns(t, h, slot, root))
	scan, err := newBatchHarness(t, fs).storage.GetSavedColumnIndex(t.Context(), slot, root)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 2, 3}, scan)
}

func TestRemoveColumnSidecarsWithAnUnreadablePack(t *testing.T) {
	const slot = batchTestSlot
	root := common.HexToHash("0x1234567890abcdef")
	mem := afero.NewMemMapFs()
	faulty := newPackFaultFs(mem, testPackFile(slot, root))
	h := newBatchHarness(t, faulty)
	require.NoError(t, h.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(slot, 0, 1, 2, 3)))
	writeLooseCopy(t, faulty, slot, root, blobSidecar(slot, 1))
	saved, err := h.storage.GetSavedColumnIndex(t.Context(), slot, root)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1, 2, 3}, saved)
	packBytes, err := afero.ReadFile(mem, faulty.path)
	require.NoError(t, err)
	faulty.openErr = os.ErrPermission

	err = h.storage.RemoveColumnSidecars(t.Context(), slot, root, 1)

	require.ErrorIs(t, err, os.ErrPermission)
	require.Equal(t, []string{faulty.path}, listFiles(t, mem), "the loose copy is gone and the pack stays")
	after, err := afero.ReadFile(mem, faulty.path)
	require.NoError(t, err)
	require.Equal(t, packBytes, after)
	require.NotContains(t, implOf(h).bitmaps.entries, root)
}

func TestRemoveColumnSidecarsOutOfRangeSkipsAnUnreadablePack(t *testing.T) {
	const slot = batchTestSlot
	root := common.HexToHash("0x1234567890abcdef")
	mem := afero.NewMemMapFs()
	faulty := newPackFaultFs(mem, testPackFile(slot, root))
	h := newBatchHarness(t, faulty)
	require.NoError(t, h.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(slot, 0, 1, 2, 3)))
	faulty.openErr = os.ErrPermission

	require.NoError(t, h.storage.RemoveColumnSidecars(t.Context(), slot, root, -1))
	require.NoError(t, h.storage.RemoveColumnSidecars(t.Context(), slot, root, 128))
}

func TestRemoveColumnSidecarsKeepsTheFirstLooseErrorOverAPackError(t *testing.T) {
	const slot = batchTestSlot
	root := common.HexToHash("0x1234567890abcdef")
	mem := afero.NewMemMapFs()
	faulty := newPackFaultFs(mem, testPackFile(slot, root))
	removing := newRemoveFailingFs(faulty)
	h := newBatchHarness(t, removing)
	require.NoError(t, h.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(slot, 0, 1, 2, 3)))
	writeLooseCopy(t, removing, slot, root, blobSidecar(slot, 1))
	removing.failOn[fmt.Sprintf("0/%s_1", root.String())] = errInducedFailure
	faulty.openErr = os.ErrPermission

	err := h.storage.RemoveColumnSidecars(t.Context(), slot, root, 1)

	require.ErrorIs(t, err, errInducedFailure)
	require.NotErrorIs(t, err, os.ErrPermission)
}

func TestRemoveColumnSidecarsDeletesTheEmptyPack(t *testing.T) {
	const slot = batchTestSlot
	root := common.HexToHash("0x1234567890abcdef")
	filesystems := map[string]func() afero.Fs{
		"MemMapFs":     afero.NewMemMapFs,
		"openHandleFs": func() afero.Fs { return newOpenHandleFs(afero.NewMemMapFs()) },
	}
	for name, newFs := range filesystems {
		t.Run(name, func(t *testing.T) {
			fs := newFs()
			h := newBatchHarness(t, fs)
			require.NoError(t, h.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(slot, 2, 3)))
			saved, err := h.storage.GetSavedColumnIndex(t.Context(), slot, root)
			require.NoError(t, err)
			require.Equal(t, []uint64{2, 3}, saved)

			require.NoError(t, h.storage.RemoveColumnSidecars(t.Context(), slot, root, 2, 3))

			require.Empty(t, listFiles(t, fs), "neither the pack nor its temp is left")
			saved, err = h.storage.GetSavedColumnIndex(t.Context(), slot, root)
			require.NoError(t, err)
			require.Empty(t, saved)
		})
	}
}

func TestRemoveColumnSidecarsKeepsThePackWhenItsDeleteFails(t *testing.T) {
	const slot = batchTestSlot
	root := common.HexToHash("0x1234567890abcdef")
	mem := afero.NewMemMapFs()
	removing := newRemoveFailingFs(mem)
	h := newBatchHarness(t, removing)
	require.NoError(t, h.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(slot, 2, 3)))
	saved, err := h.storage.GetSavedColumnIndex(t.Context(), slot, root)
	require.NoError(t, err)
	require.Equal(t, []uint64{2, 3}, saved)
	packBytes, err := afero.ReadFile(mem, testPackFile(slot, root))
	require.NoError(t, err)
	removing.failOn[testPackFile(slot, root)] = errInducedFailure

	err = h.storage.RemoveColumnSidecars(t.Context(), slot, root, 2, 3)

	require.ErrorIs(t, err, errInducedFailure)
	after, err := afero.ReadFile(mem, testPackFile(slot, root))
	require.NoError(t, err)
	require.Equal(t, packBytes, after)
	require.NotContains(t, implOf(h).bitmaps.entries, root)
	saved, err = h.storage.GetSavedColumnIndex(t.Context(), slot, root)
	require.NoError(t, err)
	require.Equal(t, []uint64{2, 3}, saved)
}

func TestRemoveColumnSidecarsRewriteFailsOnAShortSource(t *testing.T) {
	const slot = batchTestSlot
	root := common.HexToHash("0x1234567890abcdef")
	mem := afero.NewMemMapFs()
	faulty := newPackFaultFs(mem, testPackFile(slot, root))
	h := newBatchHarness(t, faulty)
	require.NoError(t, h.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(slot, 0, 1, 2, 3)))
	packBytes, err := afero.ReadFile(mem, faulty.path)
	require.NoError(t, err)
	frames := packFrames(t, mem, slot, root, 128)
	faulty.eofAfter = int64(frames[0].off) + int64(frames[0].n)

	err = h.storage.RemoveColumnSidecars(t.Context(), slot, root, 0)

	require.Error(t, err)
	after, err := afero.ReadFile(mem, faulty.path)
	require.NoError(t, err)
	require.Equal(t, packBytes, after)
}

// renameHoldFs holds the rename onto path, once armed, until it is let go.
type renameHoldFs struct {
	afero.Fs
	path    string
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	let     func()
}

func newRenameHoldFs(t testing.TB, fs afero.Fs, path string) *renameHoldFs {
	f := &renameHoldFs{Fs: fs, path: path, entered: make(chan struct{}), release: make(chan struct{})}
	f.let = sync.OnceFunc(func() { close(f.release) })
	t.Cleanup(f.let)
	return f
}

func (f *renameHoldFs) Rename(oldname, newname string) error {
	if newname == f.path && f.armed.Load() {
		f.once.Do(func() { close(f.entered) })
		<-f.release
	}
	return f.Fs.Rename(oldname, newname)
}

func (f *renameHoldFs) waitForRename(t testing.TB) {
	t.Helper()
	select {
	case <-f.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the rename was never reached")
	}
}

func waitForResult(t testing.TB, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("the operation never returned")
		return nil
	}
}

func TestRemoveColumnSidecarsKeepsPackedColumnsVisibleUntilTheRewriteLands(t *testing.T) {
	const slot = batchTestSlot
	root := common.HexToHash("0x1234567890abcdef")
	hold := newRenameHoldFs(t, afero.NewMemMapFs(), testPackFile(slot, root))
	h := newBatchHarness(t, hold)
	require.NoError(t, h.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(slot, 0, 1, 2, 3)))
	saved, err := h.storage.GetSavedColumnIndex(t.Context(), slot, root)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1, 2, 3}, saved)
	hold.armed.Store(true)

	removeDone := make(chan error, 1)
	go func() { removeDone <- h.storage.RemoveColumnSidecars(t.Context(), slot, root, 1) }()
	hold.waitForRename(t)

	require.True(t, columnExists(t, implOf(h), slot, root, 1), "the packed column is still served while the rewrite is in flight")
	requireAllReadable(t, h.storage, slot, root, 0, 1, 2, 3)
	hold.let()
	require.NoError(t, waitForResult(t, removeDone))
	require.False(t, columnExists(t, implOf(h), slot, root, 1))
}

func TestRemoveColumnSidecarsWhoseLooseUnlinkFailsKeepsTheColumnVisible(t *testing.T) {
	const slot = batchTestSlot
	root := common.HexToHash("0x1234567890abcdef")
	hold := newRenameHoldFs(t, afero.NewMemMapFs(), testPackFile(slot, root))
	removing := newRemoveFailingFs(hold)
	h := newBatchHarness(t, removing)
	require.NoError(t, h.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(slot, 0, 1, 2, 3)))
	writeLooseCopy(t, removing, slot, root, blobSidecar(slot, 1))
	saved, err := h.storage.GetSavedColumnIndex(t.Context(), slot, root)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1, 2, 3}, saved)
	removing.failOn[fmt.Sprintf("0/%s_1", root.String())] = errInducedFailure
	hold.armed.Store(true)

	removeDone := make(chan error, 1)
	go func() { removeDone <- h.storage.RemoveColumnSidecars(t.Context(), slot, root, 1) }()
	hold.waitForRename(t)

	_, known := implOf(h).bitmaps.columns(implOf(h).columnKey(slot, root))
	require.False(t, known, "the failed unlink forgets the entry before the pack is rewritten")
	require.True(t, columnExists(t, implOf(h), slot, root, 1))
	hold.let()
	require.ErrorIs(t, waitForResult(t, removeDone), errInducedFailure)
	require.Equal(t, []uint64{0, 2, 3}, packedColumns(t, removing, slot, root, 128))
	require.True(t, columnExists(t, implOf(h), slot, root, 1))
	read, err := h.storage.ReadColumnSidecarByColumnIndex(t.Context(), slot, root, 1)
	require.NoError(t, err)
	require.Equal(t, 1, read.Column.Len(), "the loose copy is served")
	scan, err := newBatchHarness(t, removing).storage.GetSavedColumnIndex(t.Context(), slot, root)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1, 2, 3}, scan)
}

func TestRemoveColumnSidecarsReturnsTheFirstErrorInCallerOrder(t *testing.T) {
	const slot = batchTestSlot
	root := common.HexToHash("0x1234567890abcdef")
	removing := newRemoveFailingFs(afero.NewMemMapFs())
	h := newBatchHarness(t, removing)
	for _, column := range []int64{1, 5} {
		require.NoError(t, h.storage.WriteColumnSidecars(t.Context(), root, column, createTestDataColumnSidecar(slot, column)))
	}
	errFive, errOne := errors.New("unlink of column 5 failed"), errors.New("unlink of column 1 failed")
	removing.failOn[fmt.Sprintf("0/%s_5", root.String())] = errFive
	removing.failOn[fmt.Sprintf("0/%s_1", root.String())] = errOne

	err := h.storage.RemoveColumnSidecars(t.Context(), slot, root, 5, 1)

	require.ErrorIs(t, err, errFive)
	require.NotErrorIs(t, err, errOne)
}

func TestRemoveColumnSidecarsDeduplicatesIndices(t *testing.T) {
	const slot = batchTestSlot
	root := common.HexToHash("0x1234567890abcdef")
	mem := afero.NewMemMapFs()
	h := newBatchHarness(t, mem)
	require.NoError(t, h.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(slot, 3, 5)))

	require.NoError(t, h.storage.RemoveColumnSidecars(t.Context(), slot, root, 3, 3))

	require.Equal(t, []uint64{5}, packedColumns(t, mem, slot, root, 128))
}

func TestPackRewriteFailuresKeepTheOldPack(t *testing.T) {
	const slot = batchTestSlot
	root := common.HexToHash("0x1234567890abcdef")
	mem := afero.NewMemMapFs()
	renaming := newRenameFailingFs(mem, nil)
	h := newBatchHarness(t, renaming)
	require.NoError(t, h.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(slot, 0, 1, 2, 3)))
	saved, err := h.storage.GetSavedColumnIndex(t.Context(), slot, root)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1, 2, 3}, saved)
	packBytes, err := afero.ReadFile(mem, testPackFile(slot, root))
	require.NoError(t, err)
	renaming.err = errInducedFailure

	err = h.storage.RemoveColumnSidecars(t.Context(), slot, root, 1)

	require.ErrorIs(t, err, errInducedFailure)
	after, err := afero.ReadFile(mem, testPackFile(slot, root))
	require.NoError(t, err)
	require.Equal(t, packBytes, after)
	require.Equal(t, []string{testPackFile(slot, root)}, listFiles(t, mem), "no temp is left")
	require.NotContains(t, implOf(h).bitmaps.entries, root)
	saved, err = h.storage.GetSavedColumnIndex(t.Context(), slot, root)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1, 2, 3}, saved)

	renaming.err = nil
	require.NoError(t, h.storage.RemoveColumnSidecars(t.Context(), slot, root, 1))
	require.Equal(t, []uint64{0, 2, 3}, packedColumns(t, mem, slot, root, 128))
}

func TestRemoveColumnSidecarsClosesThePackBeforeRenamingOverIt(t *testing.T) {
	const slot = batchTestSlot
	root := common.HexToHash("0x1234567890abcdef")
	handles := newOpenHandleFs(afero.NewMemMapFs())
	h := newBatchHarness(t, handles)
	require.NoError(t, h.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(slot, 0, 1, 2, 3)))

	require.NoError(t, h.storage.RemoveColumnSidecars(t.Context(), slot, root, 2))

	require.Equal(t, []uint64{0, 1, 3}, packedColumns(t, handles, slot, root, 128))
	require.Empty(t, handles.openHandles())
}

func closeOnce(t testing.TB, ch chan struct{}) func() {
	closeIt := sync.OnceFunc(func() { close(ch) })
	t.Cleanup(closeIt)
	return closeIt
}

func requireNotReached(t testing.TB, reached <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-reached:
		t.Fatal(what)
	case <-time.After(100 * time.Millisecond):
	}
}

func requireReached(t testing.TB, reached <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-reached:
	case <-time.After(10 * time.Second):
		t.Fatal(what)
	}
}

func TestPackRewriteDoesNotInterleaveWithPrune(t *testing.T) {
	const slot = batchTestSlot
	root := common.HexToHash("0x1234567890abcdef")
	fs := newPruneRaceFs(afero.NewBasePathFs(afero.NewOsFs(), t.TempDir()))
	h := newBatchHarness(t, fs)
	require.NoError(t, h.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(slot, 0, 1, 2, 3)))
	fs.createPath = testPackFile(slot, root) + tmpSuffix
	fs.removePath = "0"
	releaseCreate, releaseRemove := closeOnce(t, fs.createRelease), closeOnce(t, fs.removeRelease)

	removeDone := make(chan error, 1)
	go func() { removeDone <- h.storage.RemoveColumnSidecars(t.Context(), slot, root, 1) }()
	requireReached(t, fs.createEntered, "the rewrite never created its temp")
	pruneDone := make(chan error, 1)
	go func() { pruneDone <- h.storage.PruneBelow(subdivisionSlot) }()

	requireNotReached(t, fs.removeEntered, "the prune removed the bucket under a rewrite")
	releaseCreate()
	require.NoError(t, waitForResult(t, removeDone))
	requireReached(t, fs.removeEntered, "the prune never reached the bucket")
	releaseRemove()
	require.NoError(t, waitForResult(t, pruneDone))
	require.Empty(t, listFiles(t, fs))
}

func TestPruneHoldingTheBucketDelaysThePackRewrite(t *testing.T) {
	const slot = batchTestSlot
	root := common.HexToHash("0x1234567890abcdef")
	fs := newPruneRaceFs(afero.NewBasePathFs(afero.NewOsFs(), t.TempDir()))
	h := newBatchHarness(t, fs)
	require.NoError(t, h.storage.WriteColumnSidecarsBatch(t.Context(), root, batchTestSidecars(slot, 0, 1, 2, 3)))
	fs.createPath = testPackFile(slot, root) + tmpSuffix
	fs.removePath = "0"
	closeOnce(t, fs.createRelease)
	releaseRemove := closeOnce(t, fs.removeRelease)

	pruneDone := make(chan error, 1)
	go func() { pruneDone <- h.storage.PruneBelow(subdivisionSlot) }()
	requireReached(t, fs.removeEntered, "the prune never reached the bucket")
	removeDone := make(chan error, 1)
	go func() { removeDone <- h.storage.RemoveColumnSidecars(t.Context(), slot, root, 1) }()

	requireNotReached(t, fs.createEntered, "the rewrite ran while the prune held the bucket")
	releaseRemove()
	require.NoError(t, waitForResult(t, pruneDone))
	require.NoError(t, waitForResult(t, removeDone))
	exists, err := afero.DirExists(fs, "0")
	require.NoError(t, err)
	require.False(t, exists)
}

func TestAnOpenPackedColumnKeepsReadingAcrossARewrite(t *testing.T) {
	const slot = batchTestSlot
	root := common.HexToHash("0x1234567890abcdef")
	fs := afero.NewBasePathFs(afero.NewOsFs(), t.TempDir())
	h := newBatchHarness(t, fs)
	sidecars := make([]*cltypes.DataColumnSidecar, 4)
	for i := range sidecars {
		sidecars[i] = blobSidecar(slot, int64(i))
	}
	require.NoError(t, h.storage.WriteColumnSidecarsBatch(t.Context(), root, sidecars))
	var want bytes.Buffer
	require.NoError(t, h.storage.WriteStream(&want, slot, root, 3))
	held, closer, err := implOf(h).openColumn(slot, root, 3)
	require.NoError(t, err)
	t.Cleanup(func() { closer.Close() })

	err = h.storage.RemoveColumnSidecars(t.Context(), slot, root, 1)

	if runtime.GOOS == "windows" {
		require.Error(t, err, "an open handle blocks the rewrite")
		requireAllReadable(t, h.storage, slot, root, 1)
	} else {
		require.NoError(t, err)
	}
	got, readErr := io.ReadAll(held)
	require.NoError(t, readErr)
	require.Equal(t, want.Bytes(), got)

	require.NoError(t, closer.Close())
	require.NoError(t, h.storage.RemoveColumnSidecars(t.Context(), slot, root, 1))
	require.Equal(t, []uint64{0, 2, 3}, packedColumns(t, fs, slot, root, 128))
}

func TestReadersSeeACompletePackDuringARewrite(t *testing.T) {
	const slot, roots, columns = batchTestSlot, 4, 8
	mem := afero.NewMemMapFs()
	h := newBatchHarness(t, mem)
	kept := []int64{0, 1}
	want := map[int64][]byte{}
	for _, column := range kept {
		var frame bytes.Buffer
		require.NoError(t, ssz_snappy.EncodeAndWrite(&frame, blobSidecar(slot, column)))
		want[column] = frame.Bytes()
	}
	rootOf := func(r int) common.Hash { return common.BytesToHash([]byte{byte(r + 1)}) }
	for r := range roots {
		sidecars := make([]*cltypes.DataColumnSidecar, columns)
		for c := range sidecars {
			sidecars[c] = blobSidecar(slot, int64(c))
		}
		require.NoError(t, h.storage.WriteColumnSidecarsBatch(t.Context(), rootOf(r), sidecars))
	}

	stop := make(chan struct{})
	var readers, firstPass sync.WaitGroup
	defer readers.Wait()
	defer close(stop)
	firstPass.Add(4)
	for range 4 {
		firstPassDone := sync.OnceFunc(firstPass.Done)
		readers.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				for r := range roots {
					for _, column := range kept {
						var got bytes.Buffer
						assert.NoError(t, h.storage.WriteStream(&got, slot, rootOf(r), uint64(column)))
						assert.Equal(t, want[column], got.Bytes())
						sidecar, err := h.storage.ReadColumnSidecarByColumnIndex(t.Context(), slot, rootOf(r), column)
						if assert.NoError(t, err) {
							assert.Equal(t, uint64(column), sidecar.Index)
						}
					}
				}
				firstPassDone()
			}
		})
	}
	firstPass.Wait()
	for r := range roots {
		for c := int64(len(kept)); c < columns; c++ {
			require.NoError(t, h.storage.RemoveColumnSidecars(t.Context(), slot, rootOf(r), c))
		}
	}

	for r := range roots {
		require.Equal(t, []uint64{0, 1}, packedColumns(t, mem, slot, rootOf(r), 128))
	}
}
