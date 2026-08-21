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
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv/stream"
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

type exportOpts struct {
	bufferSize datasize.ByteSize
	onCollect  func(exportPreimagesStats)
	onWrite    func(exportPreimagesStats)
}

func exportPreimages(t *testing.T, ctx context.Context, accounts, storage stream.KV, writer io.Writer, opts exportOpts) (exportPreimagesStats, error) {
	t.Helper()
	if opts.bufferSize == 0 {
		opts.bufferSize = etl.BufferOptimalSize
	}
	collector := etl.NewCollector(t.Name(), t.TempDir(), etl.NewSortableBuffer(opts.bufferSize), log.New())
	defer collector.Close()

	collected, err := collectHashedPreimages(ctx, accounts, storage, collector, opts.onCollect)
	if err != nil {
		return collected, err
	}
	written, err := writeHashedPreimages(collector, writer, ctx.Done(), opts.onWrite)
	if err != nil {
		return written, err
	}
	require.Equal(t, collected, written, "sort must neither drop nor invent keys")
	return written, nil
}

// keccak256 order of the 20-byte repeated addresses is 0xaa < 0x11, the reverse
// of their plain-key order; of the 32-byte repeated slot keys it is 0x03 < 0x01 <
// 0x02. Both fixtures are fed in plain order so a plain-key sort cannot pass.
func TestExportPreimages_OrdersByKeccakNotPlainKey(t *testing.T) {
	accounts := &sliceKV{pairs: []kvPair{
		{addr(0x11), []byte{1}},
		{addr(0xaa), []byte{1}},
	}}
	storage := &sliceKV{pairs: []kvPair{
		{storageKey(addr(0xaa), slot(0x01)), []byte{1}},
		{storageKey(addr(0xaa), slot(0x02)), []byte{1}},
		{storageKey(addr(0xaa), slot(0x03)), []byte{1}},
	}}
	var out bytes.Buffer

	stats, err := exportPreimages(t, context.Background(), accounts, storage, &out, exportOpts{})
	require.NoError(t, err)

	want := append([]byte{}, addr(0xaa)...)
	want = append(want, 0, 0, 0, 3)
	want = append(want, slot(0x03)...)
	want = append(want, slot(0x01)...)
	want = append(want, slot(0x02)...)
	want = append(want, addr(0x11)...)
	want = append(want, 0, 0, 0, 0)
	require.Equal(t, want, out.Bytes())
	require.Equal(t, exportPreimagesStats{Accounts: 2, Slots: 3}, stats)
}

// A buffer this small flushes every few entries, so the load runs as a real
// multi-file merge instead of sorting one in-RAM buffer.
func TestExportPreimages_OrderSurvivesSpillToDisk(t *testing.T) {
	var accountPairs, storagePairs []kvPair
	for fill := 1; fill <= 200; fill++ {
		address := addr(byte(fill))
		accountPairs = append(accountPairs, kvPair{address, []byte{1}})
		storagePairs = append(storagePairs, kvPair{storageKey(address, slot(byte(fill))), []byte{1}})
	}
	var out bytes.Buffer

	stats, err := exportPreimages(t, context.Background(),
		&sliceKV{pairs: accountPairs}, &sliceKV{pairs: storagePairs}, &out,
		exportOpts{bufferSize: 1 * datasize.KB})
	require.NoError(t, err)
	require.Equal(t, exportPreimagesStats{Accounts: 200, Slots: 200}, stats)
	requireHashedOrder(t, out.Bytes())
}

func TestExportPreimages_AccountWithSlots(t *testing.T) {
	address := addr(0xaa)
	accounts := &sliceKV{pairs: []kvPair{{address, []byte{1}}}}
	storage := &sliceKV{pairs: []kvPair{
		{storageKey(address, slot(0x01)), []byte{1}},
		{storageKey(address, slot(0x02)), []byte{1}},
	}}
	var out bytes.Buffer

	stats, err := exportPreimages(t, context.Background(), accounts, storage, &out, exportOpts{})
	require.NoError(t, err)

	want := append([]byte{}, address...)
	want = append(want, 0, 0, 0, 2)
	want = append(want, slot(0x01)...)
	want = append(want, slot(0x02)...)
	require.Equal(t, want, out.Bytes())
	require.Equal(t, exportPreimagesStats{Accounts: 1, Slots: 2}, stats)
}

func TestExportPreimages_RejectsOrphanedStorage(t *testing.T) {
	// keccak256: addr(0x11) sorts before addr(0xbb), addr(0xcc) after it, so this
	// covers an orphan hitting the load with and without a record already open.
	for name, orphan := range map[string][]byte{"before": addr(0x11), "after": addr(0xcc)} {
		t.Run(name, func(t *testing.T) {
			accounts := &sliceKV{pairs: []kvPair{{addr(0xbb), []byte{1}}}}
			storage := &sliceKV{pairs: []kvPair{
				{storageKey(orphan, slot(0x01)), []byte{1}},
				{storageKey(addr(0xbb), slot(0x02)), []byte{1}},
			}}
			var out bytes.Buffer

			_, err := exportPreimages(t, context.Background(), accounts, storage, &out, exportOpts{})
			require.ErrorContains(t, err, "no matching account")
		})
	}
}

func TestCollectHashedPreimages_MalformedAccountKey(t *testing.T) {
	accounts := &sliceKV{pairs: []kvPair{{[]byte{0xaa, 0xbb}, []byte{1}}}}
	var out bytes.Buffer

	_, err := exportPreimages(t, context.Background(), accounts, &sliceKV{}, &out, exportOpts{})
	require.ErrorContains(t, err, "key length")
}

func TestCollectHashedPreimages_MalformedStorageKey(t *testing.T) {
	address := addr(0xaa)
	accountsOf := func() *sliceKV { return &sliceKV{pairs: []kvPair{{address, []byte{1}}}} }
	var out bytes.Buffer

	tooShort := &sliceKV{pairs: []kvPair{{[]byte{0xaa}, []byte{1}}}}
	_, err := exportPreimages(t, context.Background(), accountsOf(), tooShort, &out, exportOpts{})
	require.ErrorContains(t, err, "key length")

	wrongTotal := &sliceKV{pairs: []kvPair{{storageKey(address, []byte{0x01}), []byte{1}}}}
	out.Reset()
	_, err = exportPreimages(t, context.Background(), accountsOf(), wrongTotal, &out, exportOpts{})
	require.ErrorContains(t, err, "key length")
}

func TestExportPreimages_InterleavesMultipleAccounts(t *testing.T) {
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

	stats, err := exportPreimages(t, context.Background(), accounts, storage, &out, exportOpts{})
	require.NoError(t, err)

	// keccak256 leaves 0xaa < 0xbb < 0xcc, so record order matches plain order here.
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
	require.Equal(t, exportPreimagesStats{Accounts: 3, Slots: 3}, stats)
	require.Equal(t, stats.sizeBytes(), uint64(out.Len()))
}

func TestExportPreimages_SingleAccountNoStorage(t *testing.T) {
	accounts := &sliceKV{pairs: []kvPair{{addr(0xaa), []byte{1}}}}
	var out bytes.Buffer

	stats, err := exportPreimages(t, context.Background(), accounts, &sliceKV{}, &out, exportOpts{})
	require.NoError(t, err)

	require.Equal(t, append(addr(0xaa), 0, 0, 0, 0), out.Bytes())
	require.Equal(t, exportPreimagesStats{Accounts: 1, Slots: 0}, stats)
}

func TestCollectHashedPreimages_CancellationWhileScanningStorage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	address := addr(0xaa)
	accounts := &sliceKV{pairs: []kvPair{{address, []byte{1}}}}
	storage := &cancelOnNthNextKV{
		sliceKV: &sliceKV{pairs: []kvPair{
			{storageKey(address, slot(0x01)), []byte{1}},
			{storageKey(address, slot(0x02)), []byte{1}},
		}},
		cancel:    cancel,
		remaining: 1,
	}
	var out bytes.Buffer

	_, err := exportPreimages(t, ctx, accounts, storage, &out, exportOpts{})
	require.ErrorIs(t, err, context.Canceled)
}

func TestWriteHashedPreimages_ReportsCompletedAccounts(t *testing.T) {
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

	_, err := exportPreimages(t, context.Background(), accounts, storage, &out, exportOpts{
		onWrite: func(stats exportPreimagesStats) { reports = append(reports, stats) },
	})
	require.NoError(t, err)
	require.Equal(t, []exportPreimagesStats{
		{Accounts: 1, Slots: 2},
		{Accounts: 2, Slots: 3},
	}, reports)
}

// requireHashedOrder re-parses the framed file and checks the EIP-8347 ordering:
// records ascending by keccak256(address), slots ascending by keccak256(slotKey).
func requireHashedOrder(t *testing.T, framed []byte) {
	t.Helper()
	var prevAccount, prevSlot common.Hash
	haveAccount := false
	for len(framed) > 0 {
		require.GreaterOrEqual(t, len(framed), preimageAddrLen+preimageCountLen)
		address := framed[:preimageAddrLen]
		slotCount := int(binary.BigEndian.Uint32(framed[preimageAddrLen:]))
		framed = framed[preimageAddrLen+preimageCountLen:]
		require.GreaterOrEqual(t, len(framed), slotCount*preimageSlotLen)

		accountHash := crypto.Keccak256Hash(address)
		if haveAccount {
			require.Negative(t, bytes.Compare(prevAccount[:], accountHash[:]), "accounts out of keccak order")
		}
		prevAccount, haveAccount = accountHash, true

		haveSlot := false
		for range slotCount {
			slotHash := crypto.Keccak256Hash(framed[:preimageSlotLen])
			framed = framed[preimageSlotLen:]
			if haveSlot {
				require.Negative(t, bytes.Compare(prevSlot[:], slotHash[:]), "slots out of keccak order")
			}
			prevSlot, haveSlot = slotHash, true
		}
	}
}

// The accounts domain is walked to exhaustion before the first storage key, so on
// mainnet this guard is the only thing that answers a Ctrl-C for hours.
func TestCollectHashedPreimages_CancellationWhileScanningAccounts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	accounts := &cancelOnNthNextKV{
		sliceKV: &sliceKV{pairs: []kvPair{
			{addr(0xaa), []byte{1}},
			{addr(0xbb), []byte{1}},
		}},
		cancel:    cancel,
		remaining: 1,
	}
	var out bytes.Buffer

	_, err := exportPreimages(t, ctx, accounts, &sliceKV{}, &out, exportOpts{})
	require.ErrorIs(t, err, context.Canceled)
}

func TestCollectHashedPreimages_ReportsBothScans(t *testing.T) {
	accounts := &sliceKV{pairs: []kvPair{{addr(0xaa), []byte{1}}, {addr(0xbb), []byte{1}}}}
	storage := &sliceKV{pairs: []kvPair{
		{storageKey(addr(0xaa), slot(0x01)), []byte{1}},
		{storageKey(addr(0xbb), slot(0x02)), []byte{1}},
	}}
	var out bytes.Buffer
	var reports []exportPreimagesStats

	_, err := exportPreimages(t, context.Background(), accounts, storage, &out, exportOpts{
		onCollect: func(stats exportPreimagesStats) { reports = append(reports, stats) },
	})
	require.NoError(t, err)
	// Every account is collected before the first slot, so the counts advance in
	// two runs rather than interleaving.
	require.Equal(t, []exportPreimagesStats{
		{Accounts: 1, Slots: 0},
		{Accounts: 2, Slots: 0},
		{Accounts: 2, Slots: 1},
		{Accounts: 2, Slots: 2},
	}, reports)
}
