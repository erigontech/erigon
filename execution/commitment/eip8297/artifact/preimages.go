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
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/c2h5oh/datasize"
	keccak "github.com/erigontech/fastkeccak"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

var ErrPreimages = errors.New("pbt artifact: invalid preimages")

const preimageSlotSpillThreshold = 1 << 20

var (
	preimageScratchFileCreate = os.CreateTemp
	preimageScratchFileFlush  = func(writer *bufio.Writer) error {
		return writer.Flush()
	}
)

type PreimageStreamIterator func(func(common.Address, func(func([32]byte) error) error) error) error

func WritePreimagesStream(dst io.Writer, iterate PreimageStreamIterator) error {
	return WritePreimagesStreamWithScratch(dst, iterate, os.TempDir())
}

func WritePreimagesStreamWithScratch(dst io.Writer, iterate PreimageStreamIterator, scratchDir string) error {
	if iterate == nil || dst == nil {
		return ErrPreimages
	}
	if scratchDir == "" {
		scratchDir = os.TempDir()
	}
	var slotFile *os.File
	var slotWriter *bufio.Writer
	var slotName string
	defer func() {
		if slotFile != nil {
			if slotWriter != nil {
				_ = preimageScratchFileFlush(slotWriter)
			}
			_ = slotFile.Close()
			_ = dir.RemoveFile(slotName)
		}
	}()
	var previous common.Hash
	index := 0
	nextProgress := time.Now().Add(30 * time.Second)
	destination := bufio.NewWriterSize(dst, 1<<20)
	writeErr := iterate(func(address common.Address, slots func(func([32]byte) error) error) error {
		digest := common.Hash(keccak.Sum256(address[:]))
		if index != 0 && bytes.Compare(digest[:], previous[:]) <= 0 {
			return ErrUnsorted
		}
		previous = digest
		index++
		if index&4095 == 0 {
			if now := time.Now(); !now.Before(nextProgress) {
				nextProgress = now.Add(30 * time.Second)
				log.Root().Info("PBT preimage writer progress", "phase", "preimage writer", "accounts", index, "key_prefix", hex.EncodeToString(address[:8]))
			}
		}
		if slots == nil {
			return ErrPreimages
		}
		var slotBytes bytes.Buffer
		var count uint32
		var previousSlot common.Hash
		spilled := false
		err := slots(func(slot [32]byte) error {
			if count == ^uint32(0) {
				return ErrPreimages
			}
			slotDigest := common.Hash(keccak.Sum256(slot[:]))
			if count != 0 && bytes.Compare(slotDigest[:], previousSlot[:]) <= 0 {
				return ErrUnsorted
			}
			previousSlot = slotDigest
			if !spilled && slotBytes.Len()+len(slot) > preimageSlotSpillThreshold {
				var createErr error
				if slotFile == nil {
					slotFile, createErr = preimageScratchFileCreate(scratchDir, "pbt-preimage-record-")
					if createErr != nil {
						return createErr
					}
					slotName = slotFile.Name()
					slotWriter = bufio.NewWriterSize(slotFile, 1<<20)
				}
				if _, createErr = slotWriter.Write(slotBytes.Bytes()); createErr != nil {
					return createErr
				}
				slotBytes.Reset()
				spilled = true
			}
			if spilled {
				_, err := slotWriter.Write(slot[:])
				count++
				return err
			}
			_, err := slotBytes.Write(slot[:])
			count++
			return err
		})
		if err != nil {
			return err
		}
		var encodedCount [4]byte
		binary.BigEndian.PutUint32(encodedCount[:], count)
		if _, err := destination.Write(address[:]); err != nil {
			return err
		}
		if _, err := destination.Write(encodedCount[:]); err != nil {
			return err
		}
		if !spilled {
			_, err = destination.Write(slotBytes.Bytes())
			return err
		}
		if err := preimageScratchFileFlush(slotWriter); err != nil {
			return err
		}
		if _, err := slotFile.Seek(0, io.SeekStart); err != nil {
			return err
		}
		if _, err := io.Copy(destination, slotFile); err != nil {
			return err
		}
		if err := slotFile.Truncate(0); err != nil {
			return err
		}
		_, err = slotFile.Seek(0, io.SeekStart)
		return err
	})
	if writeErr != nil {
		return writeErr
	}
	return destination.Flush()
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

func JoinAt(snapshot io.ReaderAt, snapshotSize int64, preimages io.ReaderAt, preimageSize int64, hashFn eip8297.HashFn, yield func(common.Address, [32]byte) error, scratchDir string) error {
	return joinAtWithBuffer(snapshot, snapshotSize, preimages, preimageSize, hashFn, yield, etl.BufferOptimalSize, scratchDir)
}

func joinAtWithBuffer(snapshot io.ReaderAt, snapshotSize int64, preimages io.ReaderAt, preimageSize int64, hashFn eip8297.HashFn, yield func(common.Address, [32]byte) error, bufferSize datasize.ByteSize, tmpDir string) error {
	if hashFn == nil {
		hashFn = eip8297.HashBytes
	}
	expected, err := os.CreateTemp(tmpDir, "pbt-join-expected-")
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
	collector := etl.NewCollector("pbt-join", tmpDir, etl.NewSortableBuffer(bufferSize), log.Root()).SortAndFlushInBackground(true)
	defer collector.Close()
	if err := collectJoinItems(preimages, preimageSize, hashFn, collector); err != nil {
		return err
	}
	expectedReader := bufio.NewReaderSize(expected, 1<<20)
	joined := uint64(0)
	nextProgress := time.Now().Add(30 * time.Second)
	if err := collector.Load(nil, "", func(key, value []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
		joined++
		if now := time.Now(); !now.Before(nextProgress) {
			nextProgress = now.Add(30 * time.Second)
			log.Root().Info("PBT preimage join progress", "phase", "preimage join", "records", joined, "key_prefix", hex.EncodeToString(key[:min(len(key), 8)]))
		}
		want, wantOK, err := readJoinItem(expectedReader)
		if err != nil {
			return err
		}
		got, err := decodeJoinItem(key, value)
		if err != nil {
			return err
		}
		if !wantOK {
			return fmt.Errorf("%w: surplus key (%s)", ErrPreimages, joinItemLabel(got))
		}
		comparison := bytes.Compare(want.key, got.key)
		if comparison < 0 {
			return fmt.Errorf("%w: missing key (%s)", ErrPreimages, joinItemLabel(want))
		}
		if comparison > 0 {
			return fmt.Errorf("%w: surplus key (%s)", ErrPreimages, joinItemLabel(got))
		}
		if got.hasSlot && yield != nil {
			return yield(got.address, got.slot)
		}
		return nil
	}, etl.TransformArgs{}); err != nil {
		return err
	}
	want, ok, err := readJoinItem(expectedReader)
	if err != nil {
		return err
	}
	if ok {
		return fmt.Errorf("%w: missing key (%s)", ErrPreimages, joinItemLabel(want))
	}
	return nil
}

func joinItemLabel(item joinItem) string {
	if item.hasSlot {
		return fmt.Sprintf("address %x slot %x tree key %x", item.address, item.slot, item.key)
	}
	return fmt.Sprintf("tree key %x", item.key)
}

func CheckPreimageSetAt(preimages io.ReaderAt, preimageSize int64, expected func(func([]byte) error) error, hashFn eip8297.HashFn, scratchDir string) error {
	return checkPreimageSetAtWithBuffer(preimages, preimageSize, expected, hashFn, etl.BufferOptimalSize, scratchDir)
}

func checkPreimageSetAtWithBuffer(preimages io.ReaderAt, preimageSize int64, expected func(func([]byte) error) error, hashFn eip8297.HashFn, bufferSize datasize.ByteSize, tmpDir string) error {
	if expected == nil {
		return ErrPreimages
	}
	if hashFn == nil {
		hashFn = eip8297.HashBytes
	}
	expectedFile, err := os.CreateTemp(tmpDir, "pbt-preimage-expected-")
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
	collector := etl.NewCollector("pbt-preimage-check", tmpDir, etl.NewSortableBuffer(bufferSize), log.Root()).SortAndFlushInBackground(true)
	defer collector.Close()
	if err := collectJoinItems(preimages, preimageSize, hashFn, collector); err != nil {
		return err
	}
	expectedReader := bufio.NewReaderSize(expectedFile, 64<<10)
	checked := uint64(0)
	nextProgress := time.Now().Add(30 * time.Second)
	if err := collector.Load(nil, "", func(key, value []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
		checked++
		if now := time.Now(); !now.Before(nextProgress) {
			nextProgress = now.Add(30 * time.Second)
			log.Root().Info("PBT preimage check progress", "phase", "preimage check", "records", checked, "key_prefix", hex.EncodeToString(key[:min(len(key), 8)]))
		}
		want, wantOK, err := readJoinItem(expectedReader)
		if err != nil {
			return err
		}
		got, err := decodeJoinItem(key, value)
		if err != nil {
			return err
		}
		if !wantOK {
			return fmt.Errorf("%w: surplus key (%s)", ErrPreimages, joinItemLabel(got))
		}
		if comparison := bytes.Compare(want.key, got.key); comparison != 0 {
			if comparison < 0 {
				return fmt.Errorf("%w: missing key (%s)", ErrPreimages, joinItemLabel(want))
			}
			return fmt.Errorf("%w: surplus key (%s)", ErrPreimages, joinItemLabel(got))
		}
		return nil
	}, etl.TransformArgs{}); err != nil {
		return err
	}
	want, ok, err := readJoinItem(expectedReader)
	if err != nil {
		return err
	}
	if ok {
		return fmt.Errorf("%w: missing key (%s)", ErrPreimages, joinItemLabel(want))
	}
	return nil
}

type joinItem struct {
	key     []byte
	address common.Address
	slot    [32]byte
	hasSlot bool
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

func collectJoinItems(src io.ReaderAt, size int64, hashFn eip8297.HashFn, collector *etl.Collector) error {
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
		return collector.Collect(item.key, encodeJoinValue(item))
	}
	return ReadPreimagesStream(src, size, func(address common.Address, slots func(func([32]byte) error) error) error {
		if err := emit(address, nil); err != nil {
			return err
		}
		return slots(func(slot [32]byte) error { return emit(address, &slot) })
	})
}

func encodeJoinValue(item joinItem) []byte {
	if !item.hasSlot {
		return []byte{0}
	}
	value := make([]byte, 1+len(item.address)+32)
	value[0] = 1
	copy(value[1:], item.address[:])
	copy(value[1+len(item.address):], item.slot[:])
	return value
}

func decodeJoinItem(key, value []byte) (joinItem, error) {
	item := joinItem{key: bytes.Clone(key)}
	if len(value) == 1 && value[0] == 0 {
		return item, nil
	}
	if len(value) != 1+len(item.address)+32 || value[0] != 1 {
		return joinItem{}, ErrPreimages
	}
	copy(item.address[:], value[1:1+len(item.address)])
	copy(item.slot[:], value[1+len(item.address):])
	item.hasSlot = true
	return item, nil
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
