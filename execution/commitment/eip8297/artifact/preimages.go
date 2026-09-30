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
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

	keccak "github.com/erigontech/fastkeccak"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

var ErrPreimages = errors.New("pbt artifact: invalid preimages")

type PreimageIterator func(func(Preimage) error) error
type PreimageStreamIterator func(func(common.Address, func(func([32]byte) error) error) error) error

func WritePreimages(dst io.Writer, records any) error {
	var iterate PreimageIterator
	switch value := records.(type) {
	case []Preimage:
		iterate = func(yield func(Preimage) error) error {
			for _, record := range value {
				if err := yield(record); err != nil {
					return err
				}
			}
			return nil
		}
	case PreimageIterator:
		iterate = value
	case func(func(Preimage) error) error:
		iterate = PreimageIterator(value)
	default:
		return ErrPreimages
	}
	if iterate == nil {
		return ErrPreimages
	}
	return WritePreimagesStream(dst, func(yield func(common.Address, func(func([32]byte) error) error) error) error {
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
	})
}

func WritePreimagesStream(dst io.Writer, iterate PreimageStreamIterator) error {
	if iterate == nil {
		return ErrPreimages
	}
	var previous common.Hash
	index := 0
	return iterate(func(address common.Address, slots func(func([32]byte) error) error) error {
		digest := common.Hash(keccak.Sum256(address[:]))
		if index != 0 && bytes.Compare(digest[:], previous[:]) <= 0 {
			return ErrUnsorted
		}
		previous = digest
		index++
		if slots == nil {
			return ErrPreimages
		}
		slotFile, err := os.CreateTemp("", "pbt-preimage-record-")
		if err != nil {
			return err
		}
		slotName := slotFile.Name()
		cleanup := func() {
			_ = slotFile.Close()
			_ = dir.RemoveFile(slotName)
		}
		defer cleanup()
		var count uint32
		var previousSlot common.Hash
		err = slots(func(slot [32]byte) error {
			if count == ^uint32(0) {
				return ErrPreimages
			}
			slotDigest := common.Hash(keccak.Sum256(slot[:]))
			if count != 0 && bytes.Compare(slotDigest[:], previousSlot[:]) <= 0 {
				return ErrUnsorted
			}
			previousSlot = slotDigest
			if _, err := slotFile.Write(slot[:]); err != nil {
				return err
			}
			count++
			return nil
		})
		if err != nil {
			return err
		}
		if err := slotFile.Sync(); err != nil {
			return err
		}
		var encodedCount [4]byte
		binary.BigEndian.PutUint32(encodedCount[:], count)
		if _, err := dst.Write(address[:]); err != nil {
			return err
		}
		if _, err := dst.Write(encodedCount[:]); err != nil {
			return err
		}
		if _, err := slotFile.Seek(0, io.SeekStart); err != nil {
			return err
		}
		_, err = io.Copy(dst, slotFile)
		return err
	})
}

func ReadPreimagesAt(src io.ReaderAt, size int64, yield func(Preimage) error) error {
	return ReadPreimagesStream(src, size, func(address common.Address, slots func(func([32]byte) error) error) error {
		record := Preimage{Address: address}
		if err := slots(func(slot [32]byte) error {
			record.Slots = append(record.Slots, slot)
			return nil
		}); err != nil {
			return err
		}
		if yield == nil {
			return nil
		}
		return yield(record)
	})
}

func ReadPreimagesStream(src io.ReaderAt, size int64, yield func(common.Address, func(func([32]byte) error) error) error) error {
	if src == nil || size < 0 {
		return ErrPreimages
	}
	c := artifactCursor{src: src, limit: size}
	var previous common.Hash
	index := 0
	for c.offset < c.limit {
		address, err := c.bytes(20)
		if err != nil {
			return fmt.Errorf("%w: truncated record", ErrPreimages)
		}
		countBytes, err := c.bytes(4)
		if err != nil {
			return fmt.Errorf("%w: truncated record", ErrPreimages)
		}
		count := binary.BigEndian.Uint32(countBytes)
		if uint64(count) > uint64(c.remaining()/32) {
			return fmt.Errorf("%w: truncated slots", ErrPreimages)
		}
		var recordAddress common.Address
		copy(recordAddress[:], address)
		digest := common.Hash(keccak.Sum256(recordAddress[:]))
		if index != 0 && bytes.Compare(digest[:], previous[:]) <= 0 {
			return ErrUnsorted
		}
		previous = digest
		index++
		if yield != nil {
			if err := yield(recordAddress, func(slotYield func([32]byte) error) error {
				var previousSlot common.Hash
				for i := range count {
					slotBytes, err := c.bytes(32)
					if err != nil {
						return fmt.Errorf("%w: truncated slots", ErrPreimages)
					}
					var slot [32]byte
					copy(slot[:], slotBytes)
					slotDigest := common.Hash(keccak.Sum256(slot[:]))
					if i != 0 && bytes.Compare(slotDigest[:], previousSlot[:]) <= 0 {
						return ErrUnsorted
					}
					previousSlot = slotDigest
					if err := slotYield(slot); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				return err
			}
		} else {
			if _, err := c.bytes(int(count) * 32); err != nil {
				return fmt.Errorf("%w: truncated slots", ErrPreimages)
			}
		}
	}
	return nil
}

func JoinAt(snapshot io.ReaderAt, snapshotSize int64, preimages io.ReaderAt, preimageSize int64, hashFn eip8297.HashFn, yield func(common.Address, [32]byte) error) error {
	if hashFn == nil {
		hashFn = eip8297.HashBytes
	}
	expected, err := os.CreateTemp("", "pbt-join-expected-")
	if err != nil {
		return err
	}
	expectedName := expected.Name()
	defer func() { _ = expected.Close(); _ = dir.RemoveFile(expectedName) }()
	expectedWriter := bufio.NewWriterSize(expected, 1<<20)
	writeExpected := func(key []byte) error { return writeJoinItem(expectedWriter, joinItem{key: bytes.Clone(key)}) }
	_, err = ReadSnapshotStreamAt(snapshot, snapshotSize, SnapshotStreamCallbacks{
		Header: func(header Header) error {
			if err := writeExpected(eip8297.TreeKey(eip8297.AccountZone, header.AddressHash[:], eip8297.BasicDataLeafKey)); err != nil {
				return err
			}
			for _, slot := range header.Slots {
				if err := writeExpected(eip8297.TreeKey(eip8297.AccountZone, header.AddressHash[:], eip8297.HeaderStorageOffset+slot.Index)); err != nil {
					return err
				}
			}
			return nil
		},
		Storage: func(address common.Hash, groups func(func(Group) error) error) error {
			return groups(func(group Group) error {
				position := append(bytes.Clone(address[:]), group.StemHash[:]...)
				for _, entry := range group.Entries {
					if err := writeExpected(eip8297.TreeKey(eip8297.StorageZone, position, entry.Index)); err != nil {
						return err
					}
				}
				return nil
			})
		},
	})
	if err != nil {
		return err
	}
	if err := expectedWriter.Flush(); err != nil {
		return err
	}
	if err := expected.Sync(); err != nil {
		return err
	}
	if _, err := expected.Seek(0, io.SeekStart); err != nil {
		return err
	}
	runs, err := makeJoinRuns(preimages, preimageSize, hashFn)
	if err != nil {
		return err
	}
	defer removeJoinRuns(runs)
	actual, err := newJoinMerge(runs)
	if err != nil {
		return err
	}
	expectedReader := bufio.NewReaderSize(expected, 1<<20)
	for {
		want, wantOK, err := readJoinItem(expectedReader)
		if err != nil {
			return err
		}
		got, gotOK, err := actual.next()
		if err != nil {
			return err
		}
		if !wantOK && !gotOK {
			return nil
		}
		if !wantOK {
			return fmt.Errorf("%w: surplus key %x", ErrPreimages, got.key)
		}
		if !gotOK {
			return fmt.Errorf("%w: missing key %x", ErrPreimages, want.key)
		}
		comparison := bytes.Compare(want.key, got.key)
		if comparison < 0 {
			return fmt.Errorf("%w: missing key %x", ErrPreimages, want.key)
		}
		if comparison > 0 {
			return fmt.Errorf("%w: surplus key %x", ErrPreimages, got.key)
		}
		if got.hasSlot && yield != nil {
			if err := yield(got.address, got.slot); err != nil {
				return err
			}
		}
	}
}

func CheckPreimageSetAt(preimages io.ReaderAt, preimageSize int64, expected func(func([]byte) error) error, hashFn eip8297.HashFn) error {
	if expected == nil {
		return ErrPreimages
	}
	if hashFn == nil {
		hashFn = eip8297.HashBytes
	}
	expectedFile, err := os.CreateTemp("", "pbt-preimage-expected-")
	if err != nil {
		return err
	}
	expectedName := expectedFile.Name()
	defer func() { _ = expectedFile.Close(); _ = dir.RemoveFile(expectedName) }()
	expectedWriter := bufio.NewWriterSize(expectedFile, 64<<10)
	var previous []byte
	if err := expected(func(key []byte) error {
		if len(key) == 0 || (previous != nil && bytes.Compare(previous, key) >= 0) {
			return ErrUnsorted
		}
		previous = bytes.Clone(key)
		return writeJoinItem(expectedWriter, joinItem{key: bytes.Clone(key)})
	}); err != nil {
		return err
	}
	if err := expectedWriter.Flush(); err != nil {
		return err
	}
	if err := expectedFile.Sync(); err != nil {
		return err
	}
	if _, err := expectedFile.Seek(0, io.SeekStart); err != nil {
		return err
	}
	runs, err := makeJoinRuns(preimages, preimageSize, hashFn)
	if err != nil {
		return err
	}
	defer removeJoinRuns(runs)
	actual, err := newJoinMerge(runs)
	if err != nil {
		return err
	}
	expectedReader := bufio.NewReaderSize(expectedFile, 64<<10)
	for {
		want, wantOK, err := readJoinItem(expectedReader)
		if err != nil {
			return err
		}
		got, gotOK, err := actual.next()
		if err != nil {
			return err
		}
		if !wantOK && !gotOK {
			return nil
		}
		if !wantOK {
			return fmt.Errorf("%w: surplus key %x", ErrPreimages, got.key)
		}
		if !gotOK {
			return fmt.Errorf("%w: missing key %x", ErrPreimages, want.key)
		}
		if comparison := bytes.Compare(want.key, got.key); comparison != 0 {
			if comparison < 0 {
				return fmt.Errorf("%w: missing key %x", ErrPreimages, want.key)
			}
			return fmt.Errorf("%w: surplus key %x", ErrPreimages, got.key)
		}
	}
}

const joinRunItems = 4096

type joinItem struct {
	key     []byte
	address common.Address
	slot    [32]byte
	hasSlot bool
}

type joinRun struct {
	file   *os.File
	reader *bufio.Reader
	item   joinItem
	valid  bool
}

type joinMerge []*joinRun

func (m joinMerge) Len() int           { return len(m) }
func (m joinMerge) Less(i, j int) bool { return bytes.Compare(m[i].item.key, m[j].item.key) < 0 }
func (m joinMerge) Swap(i, j int)      { m[i], m[j] = m[j], m[i] }
func (m *joinMerge) Push(value any)    { *m = append(*m, value.(*joinRun)) }
func (m *joinMerge) Pop() any {
	old := *m
	value := old[len(old)-1]
	*m = old[:len(old)-1]
	return value
}

type joinMergeReader struct{ heap joinMerge }

func newJoinMerge(paths []string) (*joinMergeReader, error) {
	result := &joinMergeReader{}
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		run := &joinRun{file: file, reader: bufio.NewReaderSize(file, 64<<10)}
		item, ok, err := readJoinItem(run.reader)
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		if !ok {
			_ = file.Close()
			continue
		}
		run.item, run.valid = item, true
		heap.Push(&result.heap, run)
	}
	return result, nil
}

func (m *joinMergeReader) next() (joinItem, bool, error) {
	if len(m.heap) == 0 {
		return joinItem{}, false, nil
	}
	run := heap.Pop(&m.heap).(*joinRun)
	item := run.item
	next, ok, err := readJoinItem(run.reader)
	if err != nil {
		return joinItem{}, false, err
	}
	if ok {
		run.item, run.valid = next, true
		heap.Push(&m.heap, run)
	} else if err := run.file.Close(); err != nil {
		return joinItem{}, false, err
	}
	return item, true, nil
}

func writeJoinItem(writer *bufio.Writer, item joinItem) error {
	if len(item.key) > 255 {
		return ErrPreimages
	}
	if err := writer.WriteByte(byte(len(item.key))); err != nil {
		return err
	}
	if _, err := writer.Write(item.key); err != nil {
		return err
	}
	flags := byte(0)
	if item.hasSlot {
		flags = 1
	}
	if err := writer.WriteByte(flags); err != nil {
		return err
	}
	if item.hasSlot {
		if _, err := writer.Write(item.address[:]); err != nil {
			return err
		}
		_, err := writer.Write(item.slot[:])
		return err
	}
	return nil
}

func readJoinItem(reader *bufio.Reader) (joinItem, bool, error) {
	keyLength, err := reader.ReadByte()
	if errors.Is(err, io.EOF) {
		return joinItem{}, false, nil
	}
	if err != nil {
		return joinItem{}, false, err
	}
	key := make([]byte, int(keyLength))
	if _, err := io.ReadFull(reader, key); err != nil {
		return joinItem{}, false, err
	}
	flags, err := reader.ReadByte()
	if err != nil {
		return joinItem{}, false, err
	}
	item := joinItem{key: key}
	if flags > 1 {
		return joinItem{}, false, ErrPreimages
	}
	if flags == 1 {
		if _, err := io.ReadFull(reader, item.address[:]); err != nil {
			return joinItem{}, false, err
		}
		if _, err := io.ReadFull(reader, item.slot[:]); err != nil {
			return joinItem{}, false, err
		}
		item.hasSlot = true
	}
	return item, true, nil
}

func makeJoinRuns(src io.ReaderAt, size int64, hashFn eip8297.HashFn) ([]string, error) {
	paths := make([]string, 0)
	items := make([]joinItem, 0, joinRunItems)
	flush := func() error {
		if len(items) == 0 {
			return nil
		}
		sort.Slice(items, func(i, j int) bool { return bytes.Compare(items[i].key, items[j].key) < 0 })
		file, err := os.CreateTemp("", "pbt-join-run-")
		if err != nil {
			return err
		}
		writer := bufio.NewWriterSize(file, 1<<20)
		for _, item := range items {
			if err := writeJoinItem(writer, item); err != nil {
				_ = file.Close()
				_ = dir.RemoveFile(file.Name())
				return err
			}
		}
		if err := writer.Flush(); err != nil {
			_ = file.Close()
			_ = dir.RemoveFile(file.Name())
			return err
		}
		if err := file.Close(); err != nil {
			_ = dir.RemoveFile(file.Name())
			return err
		}
		paths = append(paths, file.Name())
		items = items[:0]
		return nil
	}
	emit := func(address common.Address, slot *[32]byte) error {
		address32 := eip8297.RightAlign32(address[:])
		stem := hashFn(address32[:])
		var item joinItem
		if slot == nil {
			item.key = eip8297.TreeKey(eip8297.AccountZone, stem[:], eip8297.BasicDataLeafKey)
		} else {
			item.key = treeKeyWithHash(hashFn, address[:], slot[:])
			item.address = address
			item.slot = *slot
			item.hasSlot = true
		}
		items = append(items, item)
		if len(items) == joinRunItems {
			return flush()
		}
		return nil
	}
	err := ReadPreimagesStream(src, size, func(address common.Address, slots func(func([32]byte) error) error) error {
		if err := emit(address, nil); err != nil {
			return err
		}
		return slots(func(slot [32]byte) error { return emit(address, &slot) })
	})
	if err != nil {
		removeJoinRuns(paths)
		return nil, err
	}
	if err := flush(); err != nil {
		removeJoinRuns(paths)
		return nil, err
	}
	return paths, nil
}

func removeJoinRuns(paths []string) {
	for _, path := range paths {
		_ = dir.RemoveFile(path)
	}
}

func treeKeyWithHash(hashFn eip8297.HashFn, address, slot []byte) []byte {
	address32 := eip8297.RightAlign32(address)
	slot32 := eip8297.RightAlign32(slot)
	stem := hashFn(address32[:])
	if eip8297.SlotInHeader(&slot32) {
		return eip8297.TreeKey(eip8297.AccountZone, stem[:], eip8297.HeaderStorageOffset+slot32[31])
	}
	groupInput := make([]byte, 0, 64)
	groupInput = append(groupInput, address32[:]...)
	groupInput = append(groupInput, 0)
	groupInput = append(groupInput, slot32[:31]...)
	group := hashFn(groupInput)
	position := make([]byte, 0, 64)
	position = append(position, stem[:]...)
	position = append(position, group[:]...)
	return eip8297.TreeKey(eip8297.StorageZone, position, slot32[31])
}
