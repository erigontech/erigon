package blob_storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/spf13/afero"
)

const (
	// subdivisionSlot = 10_000
	mutexSize = 64
)

type DataColumnStorage interface {
	WriteColumnSidecars(ctx context.Context, blockRoot common.Hash, columnIndex int64, columnData *cltypes.DataColumnSidecar) error
	RemoveColumnSidecar(ctx context.Context, slot uint64, blockRoot common.Hash, columnIndex int64) error
	RemoveAllColumnSidecars(ctx context.Context, slot uint64, blockRoot common.Hash) error
	ReadColumnSidecarByColumnIndex(ctx context.Context, slot uint64, blockRoot common.Hash, columnIndex int64) (*cltypes.DataColumnSidecar, error)
	ColumnSidecarExists(ctx context.Context, slot uint64, blockRoot common.Hash, columnIndex int64) (bool, error)
	WriteStream(w io.Writer, slot uint64, blockRoot common.Hash, idx uint64) error // Used for P2P networking
	GetSavedColumnIndex(ctx context.Context, blockRoot common.Hash) ([]uint64, error)
	Prune(keepSlotDistance uint64) error
}

type dataColumnStorageImpl struct {
	db                kv.RwDB
	fs                afero.Fs
	beaconChainConfig *clparams.BeaconChainConfig
	ethClock          eth_clock.EthereumClock
	slotsKept         uint64

	dbMutexes map[uint64]*sync.RWMutex
}

func NewDataColumnStore(db kv.RwDB, fs afero.Fs, slotsKept uint64, beaconChainConfig *clparams.BeaconChainConfig, ethClock eth_clock.EthereumClock) DataColumnStorage {
	impl := &dataColumnStorageImpl{
		db:                db,
		fs:                fs,
		beaconChainConfig: beaconChainConfig,
		ethClock:          ethClock,
		slotsKept:         slotsKept,
		dbMutexes:         make(map[uint64]*sync.RWMutex, mutexSize),
	}
	for i := uint64(0); i < mutexSize; i++ {
		impl.dbMutexes[i] = &sync.RWMutex{}
	}
	return impl
}

func dataColumnFilePath(slot uint64, blockRoot common.Hash, columnIndex uint64) (dir, filepath string) {
	subdir := slot / subdivisionSlot
	dir = strconv.FormatUint(subdir, 10)
	filepath = fmt.Sprintf("%s/%s_%d", dir, blockRoot.String(), columnIndex)
	return
}

func (s *dataColumnStorageImpl) WriteColumnSidecars(ctx context.Context, blockRoot common.Hash, columnIndex int64, columnData *cltypes.DataColumnSidecar) error {
	dir, filepath := dataColumnFilePath(columnData.SignedBlockHeader.Header.Slot, blockRoot, uint64(columnIndex))
	if err := s.fs.MkdirAll(dir, 0755); err != nil {
		return err
	}
	if _, err := s.fs.Stat(filepath); err == nil {
		// File already exists, no need to write again
		// TODO: Need to check content or database?
		return nil
	}
	fh, err := s.fs.Create(filepath)
	if err != nil {
		return err
	}
	// snappy of | length | ssz data |
	if err := ssz_snappy.EncodeAndWrite(fh, columnData); err != nil {
		fh.Close()
		s.fs.Remove(filepath)
		return err
	}
	if err := fh.Sync(); err != nil {
		fh.Close()
		s.fs.Remove(filepath)
		return err
	}

	mutex := s.acquireMutexBySlot(columnData.SignedBlockHeader.Header.Slot)
	mutex.Lock()
	defer mutex.Unlock()
	// increment the column count and append the column index
	// | column_count | column_index1 | column_index2 | ... |
	tx, err := s.db.BeginRw(ctx)
	if err != nil {
		fh.Close()
		s.fs.Remove(filepath)
		return err
	}
	defer tx.Rollback()
	bytes, err := tx.GetOne(kv.BlockRootToDataColumnCount, blockRoot[:])
	if err != nil {
		fh.Close()
		s.fs.Remove(filepath)
		return err
	}
	curCount := uint32(0)
	restBytes := []byte{}
	if bytes != nil {
		curCount = binary.LittleEndian.Uint32(bytes[0:4])
		restBytes = bytes[4:]
	}
	curCount++
	countBytes := make([]byte, 4)
	columnIndexBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(countBytes, curCount)
	binary.LittleEndian.PutUint32(columnIndexBytes, uint32(columnIndex))
	// | column_count | column_index1 | column_index2 | ... | new_column_index |
	newBytes := append(countBytes, restBytes...)
	newBytes = append(newBytes, columnIndexBytes...)
	if err := tx.Put(kv.BlockRootToDataColumnCount, blockRoot[:], newBytes); err != nil {
		fh.Close()
		s.fs.Remove(filepath)
		return err
	}
	if err := tx.Commit(); err != nil {
		fh.Close()
		s.fs.Remove(filepath)
		return err
	}
	fh.Close()
	log.Trace("wrote data column sidecar", "slot", columnData.SignedBlockHeader.Header.Slot, "block_root", blockRoot.String(), "column_index", columnIndex)
	return nil
}

func (s *dataColumnStorageImpl) ReadColumnSidecarByColumnIndex(ctx context.Context, slot uint64, blockRoot common.Hash, columnIndex int64) (*cltypes.DataColumnSidecar, error) {
	_, filepath := dataColumnFilePath(slot, blockRoot, uint64(columnIndex))
	fh, err := s.fs.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer fh.Close()
	data := &cltypes.DataColumnSidecar{}
	version := s.beaconChainConfig.GetCurrentStateVersion(slot / s.beaconChainConfig.SlotsPerEpoch)
	if err := ssz_snappy.DecodeAndReadNoForkDigest(fh, data, version); err != nil {
		return nil, err
	}
	return data, nil
}

func (s *dataColumnStorageImpl) ColumnSidecarExists(ctx context.Context, slot uint64, blockRoot common.Hash, columnIndex int64) (bool, error) {
	_, filepath := dataColumnFilePath(slot, blockRoot, uint64(columnIndex))
	if _, err := s.fs.Stat(filepath); os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

func (s *dataColumnStorageImpl) RemoveAllColumnSidecars(ctx context.Context, slot uint64, blockRoot common.Hash) error {
	mutex := s.acquireMutexBySlot(slot)
	mutex.Lock()
	defer mutex.Unlock()

	tx, err := s.db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	bytes, err := tx.GetOne(kv.BlockRootToDataColumnCount, blockRoot[:])
	if err != nil {
		return err
	}
	if bytes == nil {
		// No column sidecars, no need to remove
		return nil
	}
	count := binary.LittleEndian.Uint32(bytes[0:4])
	for i := uint32(0); i < count; i++ {
		columnIndex := binary.LittleEndian.Uint32(bytes[4+i*4 : 4+(i+1)*4])
		_, filepath := dataColumnFilePath(slot, blockRoot, uint64(columnIndex))
		if err := s.fs.Remove(filepath); err != nil {
			return err
		}
	}
	if err := tx.Delete(kv.BlockRootToDataColumnCount, blockRoot[:]); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *dataColumnStorageImpl) RemoveColumnSidecar(ctx context.Context, slot uint64, blockRoot common.Hash, columnIndex int64) error {
	mutex := s.acquireMutexBySlot(slot)
	mutex.Lock()
	defer mutex.Unlock()

	tx, err := s.db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	bytes, err := tx.GetOne(kv.BlockRootToDataColumnCount, blockRoot[:])
	if err != nil {
		return err
	}
	if bytes == nil {
		// empty
		return nil
	}
	// find the column index in the bytes and remove it
	count := binary.LittleEndian.Uint32(bytes[0:4])
	for i := uint32(0); i < count; i++ {
		index := binary.LittleEndian.Uint32(bytes[4+i*4 : 4+(i+1)*4])
		if index == uint32(columnIndex) {
			// remove the column index
			count--
			countBytes := make([]byte, 4)
			binary.LittleEndian.PutUint32(countBytes, count)
			newBytes := make([]byte, 4+count*4)
			copy(newBytes[:4], countBytes)
			copy(newBytes[4:], bytes[4:4+i*4])
			copy(newBytes[4+i*4:], bytes[4+(i+1)*4:])
			if err := tx.Put(kv.BlockRootToDataColumnCount, blockRoot[:], newBytes); err != nil { // truncate bytes
				return err
			}
			break
		}
	}
	if count == 0 {
		if err := tx.Delete(kv.BlockRootToDataColumnCount, blockRoot[:]); err != nil {
			return err
		}
	}

	_, filepath := dataColumnFilePath(slot, blockRoot, uint64(columnIndex))
	if err := s.fs.Remove(filepath); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *dataColumnStorageImpl) WriteStream(w io.Writer, slot uint64, blockRoot common.Hash, idx uint64) error {
	_, filepath := dataColumnFilePath(slot, blockRoot, idx)
	fh, err := s.fs.Open(filepath)
	if err != nil {
		return err
	}
	defer fh.Close()
	_, err = io.Copy(w, fh)
	return err
}

func (s *dataColumnStorageImpl) GetSavedColumnIndex(ctx context.Context, blockRoot common.Hash) ([]uint64, error) {
	// No need to lock the mutex here, as we are only reading from the database
	tx, err := s.db.BeginRw(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bytes, err := tx.GetOne(kv.BlockRootToDataColumnCount, blockRoot[:])
	if err != nil {
		return nil, err
	}
	if bytes == nil {
		return nil, nil
	}
	count := binary.LittleEndian.Uint32(bytes[0:4])
	columns := make([]uint64, count)
	for i := uint32(0); i < count; i++ {
		columns[i] = uint64(binary.LittleEndian.Uint32(bytes[4+i*4 : 4+(i+1)*4]))
	}
	return columns, nil
}

func (s *dataColumnStorageImpl) acquireMutexBySlot(slot uint64) *sync.RWMutex {
	index := slot % mutexSize
	return s.dbMutexes[index]
}

func (s *dataColumnStorageImpl) Prune(keepSlotDistance uint64) error {
	currentSlot := s.ethClock.GetCurrentSlot()
	currentSlot -= keepSlotDistance
	currentSlot = (currentSlot / subdivisionSlot) * subdivisionSlot
	var startPrune uint64
	minSlotsForBlobSidecarRequest := s.beaconChainConfig.MinSlotsForBlobsSidecarsRequest()
	if currentSlot >= minSlotsForBlobSidecarRequest {
		startPrune = currentSlot - minSlotsForBlobSidecarRequest
	}
	// delete all the folders that are older than slotsKept
	for i := startPrune; i < currentSlot; i += subdivisionSlot {
		s.fs.RemoveAll(strconv.FormatUint(i/subdivisionSlot, 10))
	}
	return nil
}
