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

type PreimageRecordWriter struct {
	dst        io.Writer
	scratchDir string
	slotFile   *os.File
	slotWriter *bufio.Writer
	slotName   string
	address    common.Address
	slotBytes  bytes.Buffer
	count      uint32
	spilled    bool
}

func NewPreimageRecordWriter(dst io.Writer, scratchDir string) (*PreimageRecordWriter, error) {
	if dst == nil {
		return nil, ErrPreimages
	}
	return &PreimageRecordWriter{dst: dst, scratchDir: scratchDir}, nil
}

func (w *PreimageRecordWriter) Begin(address common.Address) error {
	if w == nil || w.dst == nil || w.count != 0 || w.slotBytes.Len() != 0 || w.spilled {
		return ErrPreimages
	}
	w.address = address
	return nil
}

func (w *PreimageRecordWriter) AddSlot(slot []byte) error {
	if w == nil || w.dst == nil || len(slot) != 32 || w.count == ^uint32(0) {
		return ErrPreimages
	}
	if !w.spilled && w.slotBytes.Len()+len(slot) <= preimageSlotSpillThreshold {
		_, err := w.slotBytes.Write(slot)
		if err == nil {
			w.count++
		}
		return err
	}
	if w.slotFile == nil {
		file, err := preimageScratchFileCreate(w.scratchDir, "pbt-preimage-record-")
		if err != nil {
			return err
		}
		w.slotFile = file
		w.slotName = file.Name()
		w.slotWriter = bufio.NewWriterSize(file, 1<<20)
	}
	if !w.spilled {
		if _, err := w.slotWriter.Write(w.slotBytes.Bytes()); err != nil {
			return err
		}
		w.slotBytes.Reset()
		w.spilled = true
	}
	if _, err := w.slotWriter.Write(slot); err != nil {
		return err
	}
	w.count++
	return nil
}

func (w *PreimageRecordWriter) End() (uint64, error) {
	if w == nil || w.dst == nil {
		return 0, ErrPreimages
	}
	var countBytes [4]byte
	binary.BigEndian.PutUint32(countBytes[:], w.count)
	if _, err := w.dst.Write(w.address[:]); err != nil {
		return 0, err
	}
	if _, err := w.dst.Write(countBytes[:]); err != nil {
		return 0, err
	}
	if !w.spilled {
		if _, err := w.dst.Write(w.slotBytes.Bytes()); err != nil {
			return 0, err
		}
	} else {
		if err := preimageScratchFileFlush(w.slotWriter); err != nil {
			return 0, err
		}
		if _, err := w.slotFile.Seek(0, io.SeekStart); err != nil {
			return 0, err
		}
		if _, err := io.Copy(w.dst, w.slotFile); err != nil {
			return 0, err
		}
		if err := w.slotFile.Truncate(0); err != nil {
			return 0, err
		}
		if _, err := w.slotFile.Seek(0, io.SeekStart); err != nil {
			return 0, err
		}
	}
	count := uint64(w.count)
	w.slotBytes.Reset()
	w.count = 0
	w.spilled = false
	return count, nil
}

func (w *PreimageRecordWriter) Close() error {
	if w == nil || w.slotFile == nil {
		return nil
	}
	if err := preimageScratchFileFlush(w.slotWriter); err != nil {
		return err
	}
	err := w.slotFile.Close()
	if removeErr := dir.RemoveFile(w.slotName); err == nil {
		err = removeErr
	}
	w.slotFile = nil
	w.slotWriter = nil
	w.slotName = ""
	return err
}

func WritePreimagesStreamWithScratch(dst io.Writer, iterate PreimageStreamIterator, scratchDir string) error {
	if iterate == nil || dst == nil {
		return ErrPreimages
	}
	destination := bufio.NewWriterSize(dst, 1<<20)
	records, err := NewPreimageRecordWriter(destination, scratchDir)
	if err != nil {
		return err
	}
	defer records.Close()
	var previous common.Hash
	index := 0
	nextProgress := time.Now().Add(30 * time.Second)
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
		var previousSlot common.Hash
		if err := records.Begin(address); err != nil {
			return err
		}
		err := slots(func(slot [32]byte) error {
			slotDigest := common.Hash(keccak.Sum256(slot[:]))
			if previousSlot != (common.Hash{}) && bytes.Compare(slotDigest[:], previousSlot[:]) <= 0 {
				return ErrUnsorted
			}
			previousSlot = slotDigest
			return records.AddSlot(slot[:])
		})
		if err != nil {
			return err
		}
		_, err = records.End()
		return err
	})
	if writeErr != nil {
		return writeErr
	}
	return destination.Flush()
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
	writeExpected := func(yield func([]byte) error) error {
		_, err := ReadSnapshotStreamAt(snapshot, snapshotSize, SnapshotStreamCallbacks{
			Header: func(header Header) error {
				if err := yield(eip8297.TreeKey(eip8297.AccountZone, header.AddressHash[:], eip8297.BasicDataLeafKey)); err != nil {
					return err
				}
				for _, slot := range header.Slots {
					if err := yield(eip8297.TreeKey(eip8297.AccountZone, header.AddressHash[:], eip8297.HeaderStorageOffset+slot.Index)); err != nil {
						return err
					}
				}
				return nil
			},
			Storage: func(address common.Hash, groups func(func(Group) error) error) error {
				return groups(func(group Group) error {
					position := append(bytes.Clone(address[:]), group.StemHash[:]...)
					for _, entry := range group.Entries {
						if err := yield(eip8297.TreeKey(eip8297.StorageZone, position, entry.Index)); err != nil {
							return err
						}
					}
					return nil
				})
			},
		})
		return err
	}
	return comparePreimageKeys(preimages, preimageSize, writeExpected, hashFn, bufferSize, tmpDir, "PBT preimage join progress", "preimage join", func(item joinItem) error {
		if item.hasSlot && yield != nil {
			return yield(item.address, item.slot)
		}
		return nil
	})
}

func joinItemLabel(item joinItem) string {
	if item.hasSlot {
		return fmt.Sprintf("address %x slot %x tree key %x", item.address, item.slot, item.key)
	}
	return fmt.Sprintf("tree key %x", item.key)
}

func CheckPreimageSetAt(preimages io.ReaderAt, preimageSize int64, expected func(func([]byte) error) error, hashFn eip8297.HashFn, scratchDir string) error {
	return comparePreimageKeys(preimages, preimageSize, expected, hashFn, etl.BufferOptimalSize, scratchDir, "PBT preimage check progress", "preimage check", nil)
}

func comparePreimageKeys(preimages io.ReaderAt, preimageSize int64, expected func(func([]byte) error) error, hashFn eip8297.HashFn, bufferSize datasize.ByteSize, tmpDir, progressMessage, progressPhase string, yield func(joinItem) error) error {
	if expected == nil {
		return ErrPreimages
	}
	expectedFile, err := os.CreateTemp(tmpDir, "pbt-preimage-expected-")
	if err != nil {
		return err
	}
	expectedName := expectedFile.Name()
	defer func() { _ = expectedFile.Close(); _ = dir.RemoveFile(expectedName) }()
	expectedWriter := bufio.NewWriterSize(expectedFile, 1<<20)
	var previous []byte
	writeExpected := func(key []byte) error {
		if len(key) == 0 || (previous != nil && bytes.Compare(previous, key) >= 0) {
			return ErrUnsorted
		}
		previous = bytes.Clone(key)
		return writeJoinItem(expectedWriter, joinItem{key: bytes.Clone(key)})
	}
	if err := expected(writeExpected); err != nil {
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
	expectedReader := bufio.NewReaderSize(expectedFile, 1<<20)
	checked := uint64(0)
	nextProgress := time.Now().Add(30 * time.Second)
	if err := collector.Load(nil, "", func(key, value []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
		checked++
		if now := time.Now(); !now.Before(nextProgress) {
			nextProgress = now.Add(30 * time.Second)
			log.Root().Info(progressMessage, "phase", progressPhase, "records", checked, "key_prefix", hex.EncodeToString(key[:min(len(key), 8)]))
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
		if yield != nil {
			if err := yield(got); err != nil {
				return err
			}
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
	return joinItem{key: key}, true, nil
}

func collectJoinItems(src io.ReaderAt, size int64, hashFn eip8297.HashFn, collector *etl.Collector) error {
	cache := eip8297.DigestCache{Sum: hashFn}
	emit := func(address common.Address, slot *[32]byte) error {
		var item joinItem
		if slot == nil {
			item.key = cache.AccountKey(address[:], eip8297.BasicDataLeafKey)
		} else {
			item.key = cache.StorageKey(address[:], slot[:])
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
