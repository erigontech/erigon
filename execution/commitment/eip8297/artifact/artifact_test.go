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

package artifact

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

type PreimageIterator func(func(Preimage) error) error

type Preimage struct {
	Address common.Address
	Slots   [][32]byte
}

type Storage struct {
	AddressHash common.Hash
	Groups      []Group
}

type Snapshot struct {
	Root           common.Hash
	Headers        []Header
	CodeGroups     []Group
	StorageGroups  []Storage
	SnapshotDigest common.Hash
}

func WriteSnapshot(dst io.Writer, root common.Hash, leaves KVIterator) (common.Hash, error) {
	return WriteSnapshotStream(dst, leaves, func() (common.Hash, error) { return root, nil })
}

func writePreimages(t *testing.T, dst io.Writer, iterate PreimageIterator) error {
	if iterate == nil {
		return ErrPreimages
	}
	return WritePreimagesStreamWithScratch(dst, func(yield func(common.Address, func(func([32]byte) error) error) error) error {
		return iterate(func(record Preimage) error {
			return yield(record.Address, func(slotYield func([32]byte) error) error {
				for _, slot := range record.Slots {
					if err := slotYield(slot); err != nil {
						return err
					}
				}
				return nil
			})
		})
	}, t.TempDir())
}

func preimageSliceIterator(records []Preimage) PreimageIterator {
	return func(yield func(Preimage) error) error {
		for _, record := range records {
			if err := yield(record); err != nil {
				return err
			}
		}
		return nil
	}
}

type goldenArtifact struct {
	Bytes          string `json:"bytes"`
	SnapshotDigest string `json:"snapshotDigest"`
}

type heapSampler struct {
	peak atomic.Uint64
	stop chan struct{}
	done chan struct{}
}

func startHeapSampler() *heapSampler {
	sampler := &heapSampler{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(sampler.done)
		samples := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
		read := func() {
			metrics.Read(samples)
			value := samples[0].Value.Uint64()
			for {
				previous := sampler.peak.Load()
				if value <= previous || sampler.peak.CompareAndSwap(previous, value) {
					return
				}
			}
		}
		read()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				read()
			case <-sampler.stop:
				read()
				return
			}
		}
	}()
	return sampler
}

func heapObjects() uint64 {
	samples := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(samples)
	return samples[0].Value.Uint64()
}

func liveHeap() uint64 {
	samples := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	metrics.Read(samples)
	return samples[0].Value.Uint64()
}

func allocatedBytes(run func() error) (uint64, error) {
	runtime.GC()
	samples := []metrics.Sample{{Name: "/gc/heap/allocs:bytes"}}
	metrics.Read(samples)
	before := samples[0].Value.Uint64()
	err := run()
	metrics.Read(samples)
	return samples[0].Value.Uint64() - before, err
}

func heapPeakDelta(run func() error) (uint64, error) {
	runtime.GC()
	base := heapObjects()
	sampler := startHeapSampler()
	err := run()
	peak := sampler.stopAndRead()
	if peak < base {
		return 0, err
	}
	return peak - base, err
}

func (s *heapSampler) stopAndRead() uint64 {
	close(s.stop)
	<-s.done
	return s.peak.Load()
}

func readSnapshot(t *testing.T, data []byte) (Snapshot, error) {
	t.Helper()
	var snapshot Snapshot
	meta, err := ReadSnapshotStreamAt(bytes.NewReader(data), int64(len(data)), SnapshotStreamCallbacks{
		Header: func(header Header) error {
			snapshot.Headers = append(snapshot.Headers, header)
			return nil
		},
		Code: func(group Group) error {
			snapshot.CodeGroups = append(snapshot.CodeGroups, group)
			return nil
		},
		Storage: func(address common.Hash, groups func(func(Group) error) error) error {
			storage := Storage{AddressHash: address}
			if err := groups(func(group Group) error {
				storage.Groups = append(storage.Groups, group)
				return nil
			}); err != nil {
				return err
			}
			snapshot.StorageGroups = append(snapshot.StorageGroups, storage)
			return nil
		},
	})
	if err != nil {
		return Snapshot{}, err
	}
	snapshot.Root = meta.Root
	snapshot.SnapshotDigest = meta.SnapshotDigest
	return snapshot, nil
}

func readPreimages(t *testing.T, data []byte) ([]Preimage, error) {
	t.Helper()
	var records []Preimage
	err := ReadPreimagesStream(bytes.NewReader(data), int64(len(data)), func(address common.Address, slots func(func([32]byte) error) error) error {
		record := Preimage{Address: address}
		if err := slots(func(slot [32]byte) error {
			record.Slots = append(record.Slots, slot)
			return nil
		}); err != nil {
			return err
		}
		records = append(records, record)
		return nil
	})
	return records, err
}

func TestWriterReproducesHandWrittenGolden(t *testing.T) {
	golden := readGolden(t)
	want, err := hex.DecodeString(golden.Bytes)
	require.NoError(t, err)
	root := common.BytesToHash([]byte("\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f"))
	leaves := goldenLeaves(t)
	var got bytes.Buffer
	digest, err := WriteSnapshot(&got, root, func(emit func([]byte, []byte) error) error {
		for _, leaf := range leaves {
			if err := emit(leaf.Key, leaf.Value); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, want, got.Bytes(), "the writer must reproduce the hand-written artifact")
	require.Equal(t, golden.SnapshotDigest, hex.EncodeToString(digest[:]))
}

func TestReaderAcceptsHandWrittenGolden(t *testing.T) {
	golden := readGolden(t)
	want, err := hex.DecodeString(golden.Bytes)
	require.NoError(t, err)
	snapshot, err := readSnapshot(t, want)
	require.NoError(t, err)
	require.Len(t, snapshot.Headers, 3)
	require.Len(t, snapshot.CodeGroups, 1)
	require.Len(t, snapshot.StorageGroups, 1)
	require.Equal(t, byte(0), snapshot.Headers[0].Kind)
	require.Equal(t, byte(1), snapshot.Headers[1].Kind)
	require.Equal(t, byte(2), snapshot.Headers[2].Kind)
	require.Equal(t, golden.SnapshotDigest, hex.EncodeToString(snapshot.SnapshotDigest[:]))
}

func TestStreamingReadersUseCallbacks(t *testing.T) {
	golden := readGolden(t)
	data, err := hex.DecodeString(golden.Bytes)
	require.NoError(t, err)
	var headers, groups, storage uint64
	meta, err := ReadSnapshotStreamAt(bytes.NewReader(data), int64(len(data)), SnapshotStreamCallbacks{
		Header: func(Header) error { headers++; return nil },
		Code:   func(Group) error { groups++; return nil },
		Storage: func(_ common.Hash, groups func(func(Group) error) error) error {
			storage++
			return groups(nil)
		},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(3), headers)
	require.Equal(t, uint64(1), groups)
	require.Equal(t, uint64(1), storage)
	require.Equal(t, headers, meta.HeaderCount)

	var empty bytes.Buffer
	_, err = WriteSnapshot(&empty, common.Hash{}, func(func([]byte, []byte) error) error { return nil })
	require.NoError(t, err)
	require.NoError(t, JoinAt(bytes.NewReader(empty.Bytes()), int64(empty.Len()), bytes.NewReader(nil), 0, eip8297.HashBytes, nil, t.TempDir()))

	var preimages bytes.Buffer
	require.NoError(t, writePreimages(t, &preimages, PreimageIterator(func(yield func(Preimage) error) error {
		return yield(Preimage{Address: common.Address{1}})
	})))
	var count int
	require.NoError(t, ReadPreimagesStream(bytes.NewReader(preimages.Bytes()), int64(preimages.Len()), func(common.Address, func(func([32]byte) error) error) error {
		count++
		return nil
	}))
	require.Equal(t, 1, count)
}

func TestArtifactReaderRejectsMaximumFieldsWithoutAllocating(t *testing.T) {
	golden := readGolden(t)
	data, err := hex.DecodeString(golden.Bytes)
	require.NoError(t, err)
	for _, test := range []struct {
		name   string
		mutate func([]byte)
	}{
		{name: "header slot count", mutate: func(b []byte) { b[1+32+2+2] = 0xff }},
		{name: "code group count", mutate: func(b []byte) {
			index := bytes.Index(b, append([]byte{3}, bytes.Repeat([]byte{0xcc}, 32)...))
			require.NotEqual(t, -1, index)
			b[index+33] = 0xff
		}},
		{name: "storage group count", mutate: func(b []byte) {
			index := bytes.Index(b, append([]byte{6}, bytes.Repeat([]byte{0xee}, 32)...))
			require.NotEqual(t, -1, index)
			b[index+33] = 0xff
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			broken := bytes.Clone(data)
			test.mutate(broken)
			_, err := readSnapshot(t, broken)
			require.Error(t, err)
		})
	}
}

func TestArtifactReaderRejectsStorageWithoutHeader(t *testing.T) {
	leaves := goldenLeaves(t)
	leaves[len(leaves)-1].Key = bytes.Clone(leaves[len(leaves)-1].Key)
	copy(leaves[len(leaves)-1].Key[1:33], bytes.Repeat([]byte{4}, 32))
	var encoded bytes.Buffer
	_, err := WriteSnapshot(&encoded, common.Hash{}, func(emit func([]byte, []byte) error) error {
		for _, leaf := range leaves {
			if err := emit(leaf.Key, leaf.Value); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	_, err = readSnapshot(t, encoded.Bytes())
	require.Error(t, err, "a storage record without a header must be rejected")
}

func TestArtifactReaderRejectsZeroGroupValue(t *testing.T) {
	golden := readGolden(t)
	data, err := hex.DecodeString(golden.Bytes)
	require.NoError(t, err)
	index := bytes.Index(data, []byte{1, 0, 1, 0x55})
	require.NotEqual(t, -1, index)
	data[index+2] = 0
	_, err = readSnapshot(t, data)
	require.ErrorContains(t, err, "invalid group value", "a code group with a zero value must be rejected")
}

func TestArtifactReaderRejectsUnknownTag(t *testing.T) {
	data := append([]byte{8, 7}, make([]byte, 32)...)
	_, err := readSnapshot(t, data)
	require.ErrorContains(t, err, "unknown tag")
}

func TestArtifactReaderRejectsUpdatedRecordRules(t *testing.T) {
	root := append([]byte{7}, make([]byte, 32)...)
	address := append(make([]byte, 31), 1)
	header := func() []byte {
		data := append([]byte{0}, address...)
		return append(data, 1, 1, 0, 0)
	}
	code := func() []byte {
		data := append([]byte{3}, bytes.Repeat([]byte{2}, 32)...)
		return append(data, 0, 0, 1, 1)
	}
	storage := func() []byte {
		data := append([]byte{4}, address...)
		data = append(data, 5)
		data = append(data, bytes.Repeat([]byte{3}, 32)...)
		return append(data, 0, 1, 1)
	}
	storageRecord := func(address, stems []byte) []byte {
		data := append([]byte{4}, address...)
		for _, stem := range stems {
			data = append(data, 5)
			data = append(data, bytes.Repeat([]byte{stem}, 32)...)
			data = append(data, 0, 1, 1)
		}
		return data
	}
	account := func(address []byte) []byte { return append([]byte{4}, address...) }
	address2 := append(make([]byte, 31), 2)
	header2 := func() []byte {
		data := append([]byte{0}, address2...)
		return append(data, 1, 1, 0, 0)
	}
	withTrailer := func(records ...[]byte) []byte {
		data := make([]byte, 0, 64)
		for _, record := range records {
			data = append(data, record...)
		}
		data = append(data, root...)
		return data
	}
	tests := []struct {
		name string
		data []byte
	}{
		{name: "unknown tag 08", data: withTrailer([]byte{8})},
		{name: "unknown tag ff", data: withTrailer([]byte{0xff})},
		{name: "missing end tag", data: append(append(append([]byte{}, header()...), 0), make([]byte, 32)...)},
		{name: "truncated root", data: append([]byte{7}, make([]byte, 31)...)},
		{name: "trailing byte", data: append(withTrailer(header()), 0)},
		{name: "header after code", data: withTrailer(code(), header())},
		{name: "code after storage", data: withTrailer(header(), storage(), code())},
		{name: "header after storage", data: withTrailer(header(), storage(), header())},
		{name: "storage account without group", data: withTrailer(header(), append([]byte{4}, address...))},
		{name: "storage account followed by account", data: withTrailer(header(), account(address), account(address2))},
		{name: "multi group with one leaf", data: withTrailer(header(), append(append([]byte{4}, address...), append(append([]byte{6}, bytes.Repeat([]byte{3}, 32)...), 0, 0, 1, 1)...))},
		{name: "group before storage account", data: withTrailer(header(), append([]byte{5}, bytes.Repeat([]byte{3}, 32)...))},
		{name: "storage accounts out of order", data: withTrailer(header(), header2(), storageRecord(address2, []byte{2}), storageRecord(address, []byte{1}))},
		{name: "storage groups out of order", data: withTrailer(header(), storageRecord(address, []byte{2, 1}))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := readSnapshot(t, test.data)
			if test.name == "header after code" {
				require.ErrorContains(t, err, "header after zone")
				return
			}
			require.Error(t, err)
		})
	}
}

func TestArtifactReaderRejectsZeroKindZeroAccount(t *testing.T) {
	data := minimalHeaderArtifact(0, false)
	_, err := readSnapshot(t, data)
	require.ErrorIs(t, err, ErrInvalidAccount)
}

func TestArtifactReaderRejectsMalformedInputs(t *testing.T) {
	golden := readGolden(t)
	data, err := hex.DecodeString(golden.Bytes)
	require.NoError(t, err)
	tests := []struct {
		name string
		data func() []byte
	}{
		{name: "leading zero integer", data: func() []byte {
			broken := []byte{0}
			broken = append(broken, make([]byte, 32)...)
			broken = append(broken, 2, 0, 1, 0, 0, 7)
			return append(broken, make([]byte, 32)...)
		}},
		{name: "header slot is not below 64", data: func() []byte {
			broken := bytes.Clone(data)
			index := bytes.Index(broken, []byte{2, 0, 1, 0x11})
			require.NotEqual(t, -1, index)
			broken[index] = 1
			broken[index+1] = 64
			return broken
		}},
		{name: "trailing byte", data: func() []byte {
			return append(bytes.Clone(data), 0)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := readSnapshot(t, test.data())
			if test.name == "header slot is not below 64" {
				require.ErrorContains(t, err, "header slot")
				return
			}
			if test.name == "leading zero integer" {
				require.ErrorContains(t, err, "leading zero")
				return
			}
			require.Error(t, err, "the reader must reject %s", test.name)
		})
	}
}

func TestArtifactRoundTripAndEmptySnapshot(t *testing.T) {
	root := common.Hash{1}
	leaves := goldenLeaves(t)
	for _, n := range []int{0, len(leaves)} {
		var encoded bytes.Buffer
		_, err := WriteSnapshot(&encoded, root, func(emit func([]byte, []byte) error) error {
			for _, leaf := range leaves[:n] {
				if err := emit(leaf.Key, leaf.Value); err != nil {
					return err
				}
			}
			return nil
		})
		if n == 0 {
			require.NoError(t, err)
			snapshot, readErr := readSnapshot(t, encoded.Bytes())
			require.NoError(t, readErr)
			require.Empty(t, snapshot.Headers)
			require.Equal(t, root, snapshot.Root)
			continue
		}
		require.NoError(t, err)
		_, err = readSnapshot(t, encoded.Bytes())
		require.NoError(t, err)
	}
	for seed := range 5 {
		rng := rand.New(rand.NewSource(int64(seed)))
		basic, err := eip8297.EncodeBasicData(uint64(seed+1), newBalance(uint64(seed+1)), 0)
		require.NoError(t, err)
		key := eip8297.TreeKey(eip8297.AccountZone, bytes.Repeat([]byte{byte(rng.Intn(255) + 1)}, 32), eip8297.BasicDataLeafKey)
		var encoded bytes.Buffer
		_, err = WriteSnapshot(&encoded, root, func(emit func([]byte, []byte) error) error { return emit(key, basic[:]) })
		require.NoError(t, err)
		_, err = readSnapshot(t, encoded.Bytes())
		require.NoError(t, err)
	}
}

func TestStreamingArtifactMemoryStaysBounded(t *testing.T) {
	const groupCount = 16_384
	const entriesPerGroup = 256
	file, err := os.CreateTemp(t.TempDir(), "artifact-memory-")
	require.NoError(t, err)
	value := bytes.Repeat([]byte{1}, eip8297.ValueLength)
	runtime.GC()
	writerBase := liveHeap()
	var writerPeak uint64
	digest, err := WriteSnapshot(file, common.Hash{}, func(emit func([]byte, []byte) error) error {
		for group := range groupCount {
			var stem [32]byte
			binary.BigEndian.PutUint64(stem[24:], uint64(group))
			for entry := range entriesPerGroup {
				key := eip8297.TreeKey(eip8297.CodeZone, stem[:], byte(entry))
				if err := emit(key, value); err != nil {
					return err
				}
			}
			if group&255 == 255 {
				runtime.GC()
				if current := liveHeap(); current > writerPeak {
					writerPeak = current
				}
			}
		}
		return nil
	})
	if writerPeak < writerBase {
		writerPeak = writerBase
	}
	writerLive := writerPeak - writerBase
	require.NoError(t, err)
	require.NotEqual(t, common.Hash{}, digest)
	require.NoError(t, file.Sync())
	info, err := file.Stat()
	require.NoError(t, err)
	require.GreaterOrEqual(t, info.Size(), int64(128<<20))
	require.NoError(t, file.Close())
	reader, err := os.Open(file.Name())
	require.NoError(t, err)
	defer reader.Close()
	runtime.GC()
	readerBase := liveHeap()
	_, err = ReadSnapshotStreamAt(reader, info.Size(), SnapshotStreamCallbacks{
		Code: func(Group) error { return nil },
	})
	runtime.GC()
	readerHeap := liveHeap()
	readLive := uint64(0)
	if readerHeap > readerBase {
		readLive = readerHeap - readerBase
	}
	require.NoError(t, err)
	require.Less(t, writerLive, uint64(16<<20))
	require.Less(t, readLive, uint64(16<<20))
}

func TestWriterRejectsEmptyAccountAndZeroSizeCode(t *testing.T) {
	position := append(make([]byte, 31), 1)
	zeroBasic, err := eip8297.EncodeBasicData(0, uint256.NewInt(0), 0)
	require.NoError(t, err)
	zeroBasic[0] = 1
	write := func(leaves []testLeaf) error {
		var output bytes.Buffer
		_, err := WriteSnapshot(&output, common.Hash{}, func(emit func([]byte, []byte) error) error {
			for _, leaf := range leaves {
				if err := emit(leaf.Key, leaf.Value); err != nil {
					return err
				}
			}
			return nil
		})
		return err
	}
	require.ErrorContains(t, write([]testLeaf{{eip8297.TreeKey(eip8297.AccountZone, position, 0), zeroBasic[:]}}), "zero nonce and balance", "an empty kind-0 account must be refused")
	codeBasic, err := eip8297.EncodeBasicData(1, uint256.NewInt(0), 0)
	require.NoError(t, err)
	require.Error(t, write([]testLeaf{
		{eip8297.TreeKey(eip8297.AccountZone, position, 0), codeBasic[:]},
		{eip8297.TreeKey(eip8297.AccountZone, position, 1), bytes.Repeat([]byte{1}, 32)},
	}), "kind-1 code of size zero must be refused")
}

func TestPreimageReaderRejectsUnsortedDuplicateAndTruncatedRecords(t *testing.T) {
	addressA := common.Address{1}
	records := []Preimage{{Address: addressA}}
	var encoded bytes.Buffer
	require.NoError(t, writePreimages(t, &encoded, preimageSliceIterator(records)))
	_, err := readPreimages(t, append(encoded.Bytes(), 1))
	require.Error(t, err, "a trailing byte must not be accepted as a preimage record")
	duplicate := append(bytes.Clone(encoded.Bytes()), encoded.Bytes()...)
	_, err = readPreimages(t, duplicate)
	require.Error(t, err, "duplicate addresses must be rejected")
	addressRecords := []Preimage{{Address: common.Address{1}}, {Address: common.Address{2}}}
	sort.Slice(addressRecords, func(i, j int) bool {
		left := keccak.Sum256(addressRecords[i].Address[:])
		right := keccak.Sum256(addressRecords[j].Address[:])
		return bytes.Compare(left[:], right[:]) < 0
	})
	encoded.Reset()
	require.NoError(t, writePreimages(t, &encoded, preimageSliceIterator(addressRecords)))
	unsortedAddresses := bytes.Clone(encoded.Bytes())
	firstAddress := bytes.Clone(unsortedAddresses[:20])
	copy(unsortedAddresses[:20], unsortedAddresses[24:44])
	copy(unsortedAddresses[24:44], firstAddress)
	_, err = readPreimages(t, unsortedAddresses)
	require.Error(t, err, "unsorted addresses must be rejected")
	slots := [][32]byte{{1}, {2}}
	sort.Slice(slots, func(i, j int) bool {
		left := keccak.Sum256(slots[i][:])
		right := keccak.Sum256(slots[j][:])
		return bytes.Compare(left[:], right[:]) < 0
	})
	records = []Preimage{{Address: addressA, Slots: slots}}
	encoded.Reset()
	require.NoError(t, writePreimages(t, &encoded, preimageSliceIterator(records)))
	unsortedSlots := bytes.Clone(encoded.Bytes())
	firstSlot := bytes.Clone(unsortedSlots[24:56])
	copy(unsortedSlots[24:56], unsortedSlots[56:88])
	copy(unsortedSlots[56:88], firstSlot)
	_, err = readPreimages(t, unsortedSlots)
	require.Error(t, err, "unsorted slots must be rejected")
	duplicateSlot := bytes.Clone(encoded.Bytes())
	copy(duplicateSlot[56:88], duplicateSlot[24:56])
	_, err = readPreimages(t, duplicateSlot)
	require.Error(t, err, "duplicate slots must be rejected")
}

func TestPreimageJoinMergesTreeKeysAcrossAddressOrders(t *testing.T) {
	const accountCount = 1000
	addresses := make([]common.Address, accountCount)
	leaves := make([]testLeaf, 0, accountCount*3)
	records := make([]Preimage, accountCount)
	for i := range addresses {
		binary.BigEndian.PutUint64(addresses[i][12:], uint64(i+1))
		address32 := eip8297.RightAlign32(addresses[i][:])
		position := eip8297.HashBytes(address32[:])
		basic, err := eip8297.EncodeBasicData(uint64(i+1), newBalance(uint64(i+1)), 0)
		require.NoError(t, err)
		headerSlot := [32]byte{}
		headerSlot[31] = 1
		overflowSlot := [32]byte{0x80}
		leaves = append(leaves,
			testLeaf{eip8297.TreeKey(eip8297.AccountZone, position[:], eip8297.BasicDataLeafKey), basic[:]},
			testLeaf{eip8297.TreeKeyStorage(addresses[i][:], headerSlot[:]), paddedValue(1)},
			testLeaf{eip8297.TreeKeyStorage(addresses[i][:], overflowSlot[:]), paddedValue(2)},
		)
		records[i] = Preimage{Address: addresses[i], Slots: [][32]byte{headerSlot, overflowSlot}}
	}
	sort.Slice(leaves, func(i, j int) bool { return bytes.Compare(leaves[i].Key, leaves[j].Key) < 0 })
	sort.Slice(records, func(i, j int) bool {
		left := keccak.Sum256(records[i].Address[:])
		right := keccak.Sum256(records[j].Address[:])
		return bytes.Compare(left[:], right[:]) < 0
	})
	for i := range records {
		sort.Slice(records[i].Slots, func(left, right int) bool {
			a := keccak.Sum256(records[i].Slots[left][:])
			b := keccak.Sum256(records[i].Slots[right][:])
			return bytes.Compare(a[:], b[:]) < 0
		})
	}
	var snapshot, preimages bytes.Buffer
	_, err := WriteSnapshot(&snapshot, common.Hash{}, func(emit func([]byte, []byte) error) error {
		for _, leaf := range leaves {
			if err := emit(leaf.Key, leaf.Value); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, writePreimages(t, &preimages, preimageSliceIterator(records)))
	require.NoError(t, JoinAt(bytes.NewReader(snapshot.Bytes()), int64(snapshot.Len()), bytes.NewReader(preimages.Bytes()), int64(preimages.Len()), eip8297.HashBytes, nil, t.TempDir()))

	testJoinError := func(name string, mutate func([]Preimage) []Preimage, want string) {
		t.Run(name, func(t *testing.T) {
			mutated := mutate(append([]Preimage(nil), records...))
			var encoded bytes.Buffer
			require.NoError(t, writePreimages(t, &encoded, preimageSliceIterator(mutated)))
			err := JoinAt(bytes.NewReader(snapshot.Bytes()), int64(snapshot.Len()), bytes.NewReader(encoded.Bytes()), int64(encoded.Len()), eip8297.HashBytes, nil, t.TempDir())
			require.ErrorContains(t, err, want)
		})
	}
	testJoinError("missing address", func(input []Preimage) []Preimage { return input[:len(input)-1] }, "missing key")
	testJoinError("surplus address", func(input []Preimage) []Preimage {
		input = append(input, Preimage{Address: common.Address{0xff}})
		sort.Slice(input, func(i, j int) bool {
			a := keccak.Sum256(input[i].Address[:])
			b := keccak.Sum256(input[j].Address[:])
			return bytes.Compare(a[:], b[:]) < 0
		})
		return input
	}, "surplus key")
	testJoinError("missing header slot", func(input []Preimage) []Preimage {
		input[0].Slots = input[0].Slots[1:]
		return input
	}, "missing key")
	testJoinError("surplus header slot", func(input []Preimage) []Preimage {
		input[0].Slots = append(input[0].Slots, [32]byte{2})
		sort.Slice(input[0].Slots, func(i, j int) bool {
			a := keccak.Sum256(input[0].Slots[i][:])
			b := keccak.Sum256(input[0].Slots[j][:])
			return bytes.Compare(a[:], b[:]) < 0
		})
		return input
	}, "surplus key")
	testJoinError("missing overflow slot", func(input []Preimage) []Preimage {
		input[0].Slots = input[0].Slots[:1]
		return input
	}, "missing key")
	testJoinError("surplus overflow slot", func(input []Preimage) []Preimage {
		input[0].Slots = append(input[0].Slots, [32]byte{0x81})
		sort.Slice(input[0].Slots, func(i, j int) bool {
			a := keccak.Sum256(input[0].Slots[i][:])
			b := keccak.Sum256(input[0].Slots[j][:])
			return bytes.Compare(a[:], b[:]) < 0
		})
		return input
	}, "surplus key")
}

func TestCheckPreimageSetAtRejectsMissingAndSurplusKeys(t *testing.T) {
	address := common.Address{1}
	address32 := eip8297.RightAlign32(address[:])
	stem := eip8297.HashBytes(address32[:])
	headerKey := eip8297.TreeKey(eip8297.AccountZone, stem[:], eip8297.BasicDataLeafKey)
	headerSlot := [32]byte{}
	headerSlot[31] = 1
	overflowSlot := [32]byte{0x80}
	headerSlotKey := eip8297.TreeKeyStorage(address[:], headerSlot[:])
	overflowKey := eip8297.TreeKeyStorage(address[:], overflowSlot[:])
	expected := [][]byte{headerKey, headerSlotKey, overflowKey}
	record := Preimage{Address: address, Slots: [][32]byte{headerSlot, overflowSlot}}
	var encoded bytes.Buffer
	require.NoError(t, writePreimages(t, &encoded, preimageSliceIterator([]Preimage{record})))
	yieldExpected := func(yield func([]byte) error) error {
		for _, key := range expected {
			if err := yield(key); err != nil {
				return err
			}
		}
		return nil
	}
	require.NoError(t, CheckPreimageSetAt(bytes.NewReader(encoded.Bytes()), int64(encoded.Len()), yieldExpected, eip8297.HashBytes, t.TempDir()))
	require.Error(t, CheckPreimageSetAt(bytes.NewReader(encoded.Bytes()), int64(encoded.Len()), func(yield func([]byte) error) error {
		return yieldExpected(func(key []byte) error {
			if bytes.Equal(key, headerSlotKey) {
				return nil
			}
			return yield(key)
		})
	}, eip8297.HashBytes, t.TempDir()))
	require.Error(t, CheckPreimageSetAt(bytes.NewReader(encoded.Bytes()), int64(encoded.Len()), func(yield func([]byte) error) error {
		if err := yield(headerKey); err != nil {
			return err
		}
		return yield(headerSlotKey)
	}, eip8297.HashBytes, t.TempDir()), "a missing overflow preimage must be rejected")
	extraHeaderSlot := [32]byte{2}
	extraSlots := [][32]byte{headerSlot, overflowSlot, extraHeaderSlot}
	sort.Slice(extraSlots, func(i, j int) bool {
		left := keccak.Sum256(extraSlots[i][:])
		right := keccak.Sum256(extraSlots[j][:])
		return bytes.Compare(left[:], right[:]) < 0
	})
	encoded.Reset()
	require.NoError(t, writePreimages(t, &encoded, preimageSliceIterator([]Preimage{{Address: address, Slots: extraSlots}})))
	require.ErrorContains(t, CheckPreimageSetAt(bytes.NewReader(encoded.Bytes()), int64(encoded.Len()), yieldExpected, eip8297.HashBytes, t.TempDir()), "surplus key")
}

func TestCheckPreimageSetAtAcceptsReusedExpectedKey(t *testing.T) {
	address := common.Address{1}
	record := Preimage{Address: address, Slots: [][32]byte{{1}, {2}}}
	sort.Slice(record.Slots, func(i, j int) bool {
		left := keccak.Sum256(record.Slots[i][:])
		right := keccak.Sum256(record.Slots[j][:])
		return bytes.Compare(left[:], right[:]) < 0
	})
	var encoded bytes.Buffer
	require.NoError(t, writePreimages(t, &encoded, preimageSliceIterator([]Preimage{record})))
	address32 := eip8297.RightAlign32(address[:])
	stem := eip8297.HashBytes(address32[:])
	expected := [][]byte{
		eip8297.TreeKey(eip8297.AccountZone, stem[:], eip8297.BasicDataLeafKey),
		eip8297.TreeKeyStorage(address[:], record.Slots[0][:]),
		eip8297.TreeKeyStorage(address[:], record.Slots[1][:]),
	}
	sort.Slice(expected, func(i, j int) bool { return bytes.Compare(expected[i], expected[j]) < 0 })
	reused := make([]byte, 0, len(expected[0])+len(expected[1])+len(expected[2]))
	yieldExpected := func(yield func([]byte) error) error {
		for _, key := range expected {
			reused = append(reused[:0], key...)
			if err := yield(reused); err != nil {
				return err
			}
		}
		return nil
	}
	require.NoError(t, CheckPreimageSetAt(bytes.NewReader(encoded.Bytes()), int64(encoded.Len()), yieldExpected, eip8297.HashBytes, t.TempDir()))
}

func TestJoinAtLargeStorageStaysBounded(t *testing.T) {
	const slotCount = 4_300_000
	address := common.Address{1}
	basic, err := eip8297.EncodeBasicData(1, uint256.NewInt(1), 0)
	require.NoError(t, err)
	treeCollector := etl.NewCollector(t.Name()+"-tree", t.TempDir(), etl.NewSortableBuffer(1<<20), log.New())
	defer treeCollector.Close()
	preimageCollector := etl.NewCollector(t.Name()+"-preimages", t.TempDir(), etl.NewSortableBuffer(1<<20), log.New())
	defer preimageCollector.Close()
	address32 := eip8297.RightAlign32(address[:])
	stem := eip8297.HashBytes(address32[:])
	require.NoError(t, treeCollector.Collect(eip8297.TreeKey(eip8297.AccountZone, stem[:], eip8297.BasicDataLeafKey), basic[:]))
	value := paddedValue(1)
	for i := range slotCount {
		var slot [32]byte
		slot[0] = 1
		binary.BigEndian.PutUint64(slot[1:9], uint64(i))
		slot[31] = 0x80
		slotDigest := keccak.Sum256(slot[:])
		require.NoError(t, treeCollector.Collect(eip8297.TreeKeyStorage(address[:], slot[:]), value))
		require.NoError(t, preimageCollector.Collect(slotDigest[:], slot[:]))
	}
	snapshotFile, err := os.CreateTemp(t.TempDir(), "large-snapshot-")
	require.NoError(t, err)
	_, err = WriteSnapshot(snapshotFile, common.Hash{}, func(emit func([]byte, []byte) error) error {
		return treeCollector.Load(nil, "", func(key, value []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
			return emit(key, value)
		}, etl.TransformArgs{})
	})
	require.NoError(t, err)
	require.NoError(t, snapshotFile.Close())
	defer func() { _ = dir.RemoveFile(snapshotFile.Name()) }()
	snapshot, err := os.Open(snapshotFile.Name())
	require.NoError(t, err)
	defer snapshot.Close()
	snapshotInfo, err := snapshot.Stat()
	require.NoError(t, err)
	preimageFile, err := os.CreateTemp(t.TempDir(), "large-preimages-")
	require.NoError(t, err)
	err = WritePreimagesStreamWithScratch(preimageFile, func(yield func(common.Address, func(func([32]byte) error) error) error) error {
		return yield(address, func(slotYield func([32]byte) error) error {
			return preimageCollector.Load(nil, "", func(key, value []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
				var slot [32]byte
				copy(slot[:], value)
				return slotYield(slot)
			}, etl.TransformArgs{})
		})
	}, t.TempDir())
	require.NoError(t, err)
	require.NoError(t, preimageFile.Close())
	defer func() { _ = dir.RemoveFile(preimageFile.Name()) }()
	preimages, err := os.Open(preimageFile.Name())
	require.NoError(t, err)
	defer preimages.Close()
	preimageInfo, err := preimages.Stat()
	require.NoError(t, err)
	require.GreaterOrEqual(t, snapshotInfo.Size(), int64(128<<20))
	require.GreaterOrEqual(t, preimageInfo.Size(), int64(128<<20))
	readGroups := 0
	runtime.GC()
	readerBase := liveHeap()
	var readerPeak uint64
	_, err = allocatedBytes(func() error {
		_, err := ReadSnapshotStreamAt(snapshot, snapshotInfo.Size(), SnapshotStreamCallbacks{
			Storage: func(_ common.Hash, groups func(func(Group) error) error) error {
				return groups(func(group Group) error {
					readGroups += len(group.Entries)
					if readGroups&((1<<16)-1) == 0 {
						runtime.GC()
						if current := liveHeap(); current > readerPeak {
							readerPeak = current
						}
					}
					return nil
				})
			},
		})
		return err
	})
	require.NoError(t, err)
	require.Equal(t, slotCount, readGroups)
	if readerPeak < readerBase {
		readerPeak = readerBase
	}
	require.Less(t, readerPeak-readerBase, uint64(1<<20))
	readSlots := 0
	runtime.GC()
	preimageBase := liveHeap()
	var preimagePeak uint64
	err = ReadPreimagesStream(preimages, preimageInfo.Size(), func(_ common.Address, slots func(func([32]byte) error) error) error {
		return slots(func([32]byte) error {
			readSlots++
			if readSlots&((1<<16)-1) == 0 {
				runtime.GC()
				if current := liveHeap(); current > preimagePeak {
					preimagePeak = current
				}
			}
			return nil
		})
	})
	require.NoError(t, err)
	require.Equal(t, slotCount, readSlots)
	if preimagePeak < preimageBase {
		preimagePeak = preimageBase
	}
	require.Less(t, preimagePeak-preimageBase, uint64(1<<20))
	seen := 0
	runtime.GC()
	joinBase := liveHeap()
	var joinPeak uint64
	_, err = allocatedBytes(func() error {
		return joinAtWithBuffer(snapshot, snapshotInfo.Size(), preimages, preimageInfo.Size(), eip8297.HashBytes, func(common.Address, [32]byte) error {
			seen++
			if seen&((1<<16)-1) == 0 {
				runtime.GC()
				if current := liveHeap(); current > joinPeak {
					joinPeak = current
				}
			}
			return nil
		}, 1<<20, t.TempDir())
	})
	require.NoError(t, err)
	require.Equal(t, slotCount, seen)
	require.NotZero(t, joinPeak)
	if joinPeak < joinBase {
		joinPeak = joinBase
	}
	require.Less(t, joinPeak-joinBase, uint64(32<<20))
}

func TestPreimageReaderAllocationsStayBounded(t *testing.T) {
	const recordCount = 10_000
	records := make([]Preimage, recordCount)
	for i := range records {
		records[i].Address[19] = byte(i)
		records[i].Address[18] = byte(i >> 8)
	}
	sort.Slice(records, func(i, j int) bool {
		left := keccak.Sum256(records[i].Address[:])
		right := keccak.Sum256(records[j].Address[:])
		return bytes.Compare(left[:], right[:]) < 0
	})
	var encoded bytes.Buffer
	require.NoError(t, writePreimages(t, &encoded, preimageSliceIterator(records)))
	allocations := testing.AllocsPerRun(3, func() {
		err := ReadPreimagesStream(bytes.NewReader(encoded.Bytes()), int64(encoded.Len()), func(common.Address, func(func([32]byte) error) error) error {
			return nil
		})
		require.NoError(t, err)
	})
	require.Less(t, allocations, float64(recordCount)*2, "the reader must reuse its cursor buffer")
}

func TestReadPreimagesShortRecordsReportTheSameError(t *testing.T) {
	for _, size := range []int{21, 23} {
		err := ReadPreimagesStream(bytes.NewReader(make([]byte, size)), int64(size), func(common.Address, func(func([32]byte) error) error) error {
			return nil
		})
		require.EqualError(t, err, "pbt artifact: invalid preimages: truncated record")
	}
}

func TestWritePreimagesStreamWithScratchReusesScratchAcrossAccounts(t *testing.T) {
	previousCreate := preimageScratchFileCreate
	previousFlush := preimageScratchFileFlush
	createCount := 0
	flushCount := 0
	preimageScratchFileCreate = func(dir, pattern string) (*os.File, error) {
		createCount++
		return previousCreate(dir, pattern)
	}
	preimageScratchFileFlush = func(writer *bufio.Writer) error {
		flushCount++
		return previousFlush(writer)
	}
	t.Cleanup(func() {
		preimageScratchFileCreate = previousCreate
		preimageScratchFileFlush = previousFlush
	})
	records := make([]common.Address, 10_000)
	for i := range records {
		binary.BigEndian.PutUint64(records[i][12:], uint64(i))
	}
	sort.Slice(records, func(i, j int) bool {
		left := keccak.Sum256(records[i][:])
		right := keccak.Sum256(records[j][:])
		return bytes.Compare(left[:], right[:]) < 0
	})
	largeSlots := make([][32]byte, 32_769)
	for i := range largeSlots {
		binary.BigEndian.PutUint64(largeSlots[i][24:], uint64(i))
	}
	sort.Slice(largeSlots, func(i, j int) bool {
		left := keccak.Sum256(largeSlots[i][:])
		right := keccak.Sum256(largeSlots[j][:])
		return bytes.Compare(left[:], right[:]) < 0
	})
	scratchDir := t.TempDir()
	var output bytes.Buffer
	largeYieldCount := 0
	err := WritePreimagesStreamWithScratch(&output, func(yield func(common.Address, func(func([32]byte) error) error) error) error {
		for index, address := range records {
			if err := yield(address, func(slotYield func([32]byte) error) error {
				if index == 0 {
					for _, slot := range largeSlots {
						largeYieldCount++
						if err := slotYield(slot); err != nil {
							return err
						}
					}
					return nil
				}
				var slot [32]byte
				binary.BigEndian.PutUint64(slot[24:], uint64(index))
				return slotYield(slot)
			}); err != nil {
				return err
			}
		}
		return nil
	}, scratchDir)
	require.NoError(t, err)
	require.Greater(t, len(output.Bytes()), len(records)*24)
	require.Equal(t, 1, createCount)
	require.Equal(t, len(largeSlots), largeYieldCount)
	require.Equal(t, 2, flushCount)
	entries, err := os.ReadDir(scratchDir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestWritePreimagesStreamWithScratchPreservesConsecutiveSpills(t *testing.T) {
	const largeSlotCount = preimageSlotSpillThreshold/32 + 1
	largeSlots := make([][32]byte, largeSlotCount)
	for i := range largeSlots {
		binary.BigEndian.PutUint64(largeSlots[i][24:], uint64(i))
	}
	sort.Slice(largeSlots, func(i, j int) bool {
		left := keccak.Sum256(largeSlots[i][:])
		right := keccak.Sum256(largeSlots[j][:])
		return bytes.Compare(left[:], right[:]) < 0
	})

	tests := []struct {
		name   string
		counts []int
	}{
		{name: "two large", counts: []int{largeSlotCount, largeSlotCount}},
		{name: "large small large", counts: []int{largeSlotCount, 1, largeSlotCount}},
		{name: "three large", counts: []int{largeSlotCount, largeSlotCount, largeSlotCount}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			records := make([]Preimage, len(tt.counts))
			for i, count := range tt.counts {
				records[i].Address[19] = byte(i + 1)
				records[i].Slots = largeSlots[:count]
			}
			sort.Slice(records, func(i, j int) bool {
				left := keccak.Sum256(records[i].Address[:])
				right := keccak.Sum256(records[j].Address[:])
				return bytes.Compare(left[:], right[:]) < 0
			})
			var encoded bytes.Buffer
			require.NoError(t, WritePreimagesStreamWithScratch(&encoded, func(yield func(common.Address, func(func([32]byte) error) error) error) error {
				return preimageSliceIterator(records)(func(record Preimage) error {
					return yield(record.Address, func(slotYield func([32]byte) error) error {
						for _, slot := range record.Slots {
							if err := slotYield(slot); err != nil {
								return err
							}
						}
						return nil
					})
				})
			}, t.TempDir()))

			index := 0
			require.NoError(t, ReadPreimagesStream(bytes.NewReader(encoded.Bytes()), int64(encoded.Len()), func(address common.Address, slots func(func([32]byte) error) error) error {
				record := records[index]
				index++
				require.Equal(t, record.Address, address)
				slotIndex := 0
				require.NoError(t, slots(func(slot [32]byte) error {
					require.Equal(t, record.Slots[slotIndex], slot)
					slotIndex++
					return nil
				}))
				require.Equal(t, len(record.Slots), slotIndex)
				return nil
			}))
			require.Equal(t, len(records), index)
		})
	}
}

func TestWritePreimagesStreamUnderThresholdDoesNotTouchScratchDirectory(t *testing.T) {
	scratchDir := t.TempDir()
	sentinel := filepath.Join(scratchDir, "sentinel")
	require.NoError(t, os.WriteFile(sentinel, []byte("sentinel"), 0o644))
	require.NoError(t, os.Chmod(scratchDir, 0o500))
	t.Cleanup(func() { require.NoError(t, os.Chmod(scratchDir, 0o755)) })
	before, err := os.Stat(scratchDir)
	require.NoError(t, err)
	var output bytes.Buffer
	address := common.Address{1}
	require.NoError(t, WritePreimagesStreamWithScratch(&output, func(yield func(common.Address, func(func([32]byte) error) error) error) error {
		return yield(address, func(slotYield func([32]byte) error) error {
			var slot [32]byte
			return slotYield(slot)
		})
	}, scratchDir))
	after, err := os.Stat(scratchDir)
	require.NoError(t, err)
	require.Equal(t, before.ModTime(), after.ModTime())
}

type testLeaf struct {
	Key   []byte
	Value []byte
}

func goldenLeaves(t *testing.T) []testLeaf {
	t.Helper()
	account := func(position []byte, sub byte) []byte { return eip8297.TreeKey(eip8297.AccountZone, position, sub) }
	code := func(position []byte, sub byte) []byte { return eip8297.TreeKey(eip8297.CodeZone, position, sub) }
	storage := func(address, stem []byte, sub byte) []byte {
		return eip8297.TreeKey(eip8297.StorageZone, append(bytes.Clone(address), stem...), sub)
	}
	position := func(value byte) []byte { return append(make([]byte, 31), value) }
	basic := func(nonce, balance, codeSize uint64) []byte {
		value, err := eip8297.EncodeBasicData(nonce, newBalance(balance), codeSize)
		require.NoError(t, err)
		return value[:]
	}
	delegation := make([]byte, 32)
	copy(delegation, append([]byte{0xef, 0x01, 0x00}, bytes.Repeat([]byte{0xbb}, 20)...))
	return []testLeaf{
		{account(position(1), 0), basic(1, 2, 0)},
		{account(position(1), 64), paddedValue(0x11)},
		{account(position(1), 67), paddedValue(0x2233)},
		{account(position(2), 0), basic(2, 0, 31)},
		{account(position(2), 1), bytes.Repeat([]byte{0xaa}, 32)},
		{account(position(2), 65), paddedValue(0x44)},
		{account(position(3), 0), basic(3, 4, eip8297.DelegationCodeLength)},
		{account(position(3), 2), delegation},
		{code(bytes.Repeat([]byte{0xcc}, 32), 0), leftPaddedValue(0x55)},
		{code(bytes.Repeat([]byte{0xcc}, 32), 7), leftPaddedBytes(0x66, 0x77)},
		{storage(append(make([]byte, 31), 1), bytes.Repeat([]byte{0xdd}, 32), 64), paddedValue(0x88)},
		{storage(append(make([]byte, 31), 1), bytes.Repeat([]byte{0xee}, 32), 1), paddedValue(0x99)},
		{storage(append(make([]byte, 31), 1), bytes.Repeat([]byte{0xee}, 32), 2), paddedValue(0xaa)},
	}
}

func paddedValue(value uint64) []byte {
	result := make([]byte, 32)
	for i := uint(0); i < 8 && value != 0; i++ {
		result[31-i] = byte(value)
		value >>= 8
	}
	return result
}

func leftPaddedValue(value byte) []byte {
	result := make([]byte, 32)
	result[31] = value
	return result
}

func leftPaddedBytes(values ...byte) []byte {
	result := make([]byte, 32)
	copy(result[len(result)-len(values):], values)
	return result
}

func newBalance(value uint64) *uint256.Int {
	return uint256.NewInt(value)
}

func readGolden(t *testing.T) goldenArtifact {
	t.Helper()
	data, err := os.ReadFile("testdata/golden.json")
	require.NoError(t, err)
	var golden goldenArtifact
	require.NoError(t, json.Unmarshal(data, &golden))
	return golden
}

func minimalHeaderArtifact(kind byte, nonzero bool) []byte {
	data := []byte{kind}
	data = append(data, make([]byte, 32)...)
	if nonzero {
		data = append(data, 1, 1, 0)
	} else {
		data = append(data, 0, 0)
	}
	if kind == 1 {
		data = append(data, bytes.Repeat([]byte{1}, 32)...)
		data = append(data, 1, 1)
	} else if kind == 2 {
		data = append(data, make([]byte, 20)...)
	}
	data = append(data, 0, 7)
	data = append(data, make([]byte, 32)...)
	return data
}
