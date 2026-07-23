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

package app

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

func TestPreparePreimagesOutputRemovesStaleMetadata(t *testing.T) {
	outDir := t.TempDir()
	metaPath := filepath.Join(outDir, "preimages.meta.json")
	require.NoError(t, os.WriteFile(metaPath, []byte("stale"), 0o644))

	framedPath, gotMetaPath, err := preparePreimagesOutput(outDir)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(outDir, "framed.bin"), framedPath)
	require.Equal(t, metaPath, gotMetaPath)
	_, err = os.Stat(metaPath)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestOpenExportDirsDoesNotCreateMissingDatadir(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "missing")

	_, err := openExportDirs(dataDir)
	require.ErrorContains(t, err, "datadir does not exist")
	_, statErr := os.Stat(dataDir)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestCheckRootPin(t *testing.T) {
	root := common.HexToHash("0x01")
	otherRoot := common.HexToHash("0x02")

	require.NoError(t, checkRootPin(root, &types.Header{Root: root}, 5))

	err := checkRootPin(root, &types.Header{Root: otherRoot}, 5)
	require.ErrorContains(t, err, root.Hex())
	require.ErrorContains(t, err, otherRoot.Hex())

	require.ErrorContains(t, checkRootPin(root, nil, 5), "header")
}

type kvPair struct {
	key   []byte
	value []byte
}

type sliceKV struct {
	pairs []kvPair
	next  int
}

func (iterator *sliceKV) HasNext() bool { return iterator.next < len(iterator.pairs) }
func (iterator *sliceKV) Next() ([]byte, []byte, error) {
	pair := iterator.pairs[iterator.next]
	iterator.next++
	return pair.key, pair.value, nil
}
func (iterator *sliceKV) Close() {}

type cancelOnNthNextKV struct {
	*sliceKV
	cancel    context.CancelFunc
	remaining int
}

func (iterator *cancelOnNthNextKV) Next() ([]byte, []byte, error) {
	key, value, err := iterator.sliceKV.Next()
	iterator.remaining--
	if iterator.remaining == 0 {
		iterator.cancel()
	}
	return key, value, err
}

func addr(fill byte) []byte { return bytes.Repeat([]byte{fill}, 20) }
func slot(fill byte) []byte { return bytes.Repeat([]byte{fill}, 32) }

func storageKey(address, slotKey []byte) []byte {
	return append(append([]byte{}, address...), slotKey...)
}

func TestWriteFramedPreimages_AccountWithSlots(t *testing.T) {
	address := addr(0xaa)
	accounts := &sliceKV{pairs: []kvPair{{address, []byte{1}}}}
	storage := &sliceKV{pairs: []kvPair{
		{storageKey(address, slot(0x01)), []byte{1}},
		{storageKey(address, slot(0x02)), []byte{1}},
	}}
	var out bytes.Buffer

	stats, err := writeFramedPreimages(context.Background(), accounts, storage, &out, nil)
	require.NoError(t, err)

	want := append([]byte{}, address...)
	want = append(want, 0, 0, 0, 2)
	want = append(want, slot(0x01)...)
	want = append(want, slot(0x02)...)
	require.Equal(t, want, out.Bytes())
	require.Equal(t, uint64(1), stats.Accounts)
	require.Equal(t, uint64(2), stats.Slots)
}

func TestWriteFramedPreimages_RejectsOrphanedStorage(t *testing.T) {
	accounts := &sliceKV{pairs: []kvPair{{addr(0xbb), []byte{1}}}}
	storage := &sliceKV{pairs: []kvPair{
		{storageKey(addr(0x11), slot(0x01)), []byte{1}},
		{storageKey(addr(0xbb), slot(0x02)), []byte{1}},
		{storageKey(addr(0xcc), slot(0x03)), []byte{1}},
	}}
	var out bytes.Buffer

	_, err := writeFramedPreimages(context.Background(), accounts, storage, &out, nil)
	require.ErrorContains(t, err, "no matching account")
}

func TestWriteFramedPreimages_MalformedAccountKey(t *testing.T) {
	accounts := &sliceKV{pairs: []kvPair{{[]byte{0xaa, 0xbb}, []byte{1}}}}
	var out bytes.Buffer

	_, err := writeFramedPreimages(context.Background(), accounts, &sliceKV{}, &out, nil)
	require.ErrorContains(t, err, "key length")
}

func TestWriteFramedPreimages_MalformedStorageKey(t *testing.T) {
	address := addr(0xaa)
	accounts := &sliceKV{pairs: []kvPair{{address, []byte{1}}}}
	tooShort := &sliceKV{pairs: []kvPair{{[]byte{0xaa}, []byte{1}}}}
	var out bytes.Buffer

	_, err := writeFramedPreimages(context.Background(), accounts, tooShort, &out, nil)
	require.ErrorContains(t, err, "key length")

	wrongTotal := &sliceKV{pairs: []kvPair{{storageKey(address, []byte{0x01}), []byte{1}}}}
	out.Reset()
	_, err = writeFramedPreimages(context.Background(), &sliceKV{pairs: []kvPair{{address, []byte{1}}}}, wrongTotal, &out, nil)
	require.ErrorContains(t, err, "key length")
}

func TestWriteFramedPreimages_InterleavesMultipleAccounts(t *testing.T) {
	accounts := &sliceKV{pairs: []kvPair{
		{addr(0xaa), []byte{1}},
		{addr(0xbb), []byte{1}},
		{addr(0xcc), []byte{1}},
	}}
	storage := &sliceKV{pairs: []kvPair{
		{storageKey(addr(0xaa), slot(0x01)), []byte{1}},
		{storageKey(addr(0xaa), slot(0x02)), []byte{1}},
		{storageKey(addr(0xcc), slot(0x03)), []byte{1}},
	}}
	var out bytes.Buffer

	stats, err := writeFramedPreimages(context.Background(), accounts, storage, &out, nil)
	require.NoError(t, err)

	want := append([]byte{}, addr(0xaa)...)
	want = append(want, 0, 0, 0, 2)
	want = append(want, slot(0x01)...)
	want = append(want, slot(0x02)...)
	want = append(want, addr(0xbb)...)
	want = append(want, 0, 0, 0, 0)
	want = append(want, addr(0xcc)...)
	want = append(want, 0, 0, 0, 1)
	want = append(want, slot(0x03)...)
	require.Equal(t, want, out.Bytes())
	require.Equal(t, uint64(3), stats.Accounts)
	require.Equal(t, uint64(3), stats.Slots)

	wantSize := stats.Accounts*(20+4) + stats.Slots*32
	require.Equal(t, wantSize, uint64(out.Len()))
}

func TestWriteFramedPreimages_SingleAccountNoStorage(t *testing.T) {
	accounts := &sliceKV{pairs: []kvPair{{addr(0xaa), []byte{1}}}}
	storage := &sliceKV{}
	var out bytes.Buffer

	stats, err := writeFramedPreimages(context.Background(), accounts, storage, &out, nil)
	require.NoError(t, err)

	want := append(addr(0xaa), 0, 0, 0, 0)
	require.Equal(t, want, out.Bytes())
	require.Equal(t, uint64(1), stats.Accounts)
	require.Equal(t, uint64(0), stats.Slots)
}

func TestWriteFramedPreimages_CancellationWhileScanningStorage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	address := addr(0xaa)
	accounts := &sliceKV{pairs: []kvPair{{address, []byte{1}}}}
	storage := &cancelOnNthNextKV{
		sliceKV: &sliceKV{pairs: []kvPair{
			{storageKey(address, slot(0x01)), []byte{1}},
			{storageKey(address, slot(0x02)), []byte{1}},
		}},
		cancel:    cancel,
		remaining: 2,
	}
	var out bytes.Buffer

	_, err := writeFramedPreimages(ctx, accounts, storage, &out, nil)
	require.ErrorIs(t, err, context.Canceled)
}

func TestWriteFramedPreimages_ReportsCompletedAccounts(t *testing.T) {
	addressA := addr(0xaa)
	addressB := addr(0xbb)
	accounts := &sliceKV{pairs: []kvPair{{addressA, []byte{1}}, {addressB, []byte{1}}}}
	storage := &sliceKV{pairs: []kvPair{
		{storageKey(addressA, slot(0x01)), []byte{1}},
		{storageKey(addressA, slot(0x02)), []byte{1}},
		{storageKey(addressB, slot(0x03)), []byte{1}},
	}}
	var out bytes.Buffer
	var reports []exportPreimagesStats

	_, err := writeFramedPreimages(context.Background(), accounts, storage, &out, func(stats exportPreimagesStats) {
		reports = append(reports, stats)
	})
	require.NoError(t, err)
	require.Equal(t, []exportPreimagesStats{
		{Accounts: 1, Slots: 2},
		{Accounts: 2, Slots: 3},
	}, reports)
}
