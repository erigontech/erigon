package blob_storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"

	"github.com/erigontech/erigon-lib/common"
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
	GetSavedColumnIndex(ctx context.Context, slot uint64, blockRoot common.Hash) ([]uint64, error)
	Prune(keepSlotDistance uint64) error
}

type dataColumnStorageImpl struct {
	fs                afero.Fs
	beaconChainConfig *clparams.BeaconChainConfig
	ethClock          eth_clock.EthereumClock
	slotsKept         uint64

	dbMutexes map[uint64]*sync.RWMutex
}

func NewDataColumnStore(fs afero.Fs, slotsKept uint64, beaconChainConfig *clparams.BeaconChainConfig, ethClock eth_clock.EthereumClock) DataColumnStorage {
	impl := &dataColumnStorageImpl{
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

	for i := uint64(0); i < s.beaconChainConfig.NumberOfColumns; i++ {
		_, filepath := dataColumnFilePath(slot, blockRoot, i)
		s.fs.Remove(filepath)
	}
	return nil
}

func (s *dataColumnStorageImpl) RemoveColumnSidecar(ctx context.Context, slot uint64, blockRoot common.Hash, columnIndex int64) error {
	mutex := s.acquireMutexBySlot(slot)
	mutex.Lock()
	defer mutex.Unlock()

	_, filepath := dataColumnFilePath(slot, blockRoot, uint64(columnIndex))
	if err := s.fs.Remove(filepath); err != nil {
		return err
	}
	return nil
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

// GetSavedColumnIndex returns the list of saved column indices for the given slot and block root.
func (s *dataColumnStorageImpl) GetSavedColumnIndex(ctx context.Context, slot uint64, blockRoot common.Hash) ([]uint64, error) {
	var savedColumns []uint64
	for i := uint64(0); i < s.beaconChainConfig.NumberOfColumns; i++ {
		_, filepath := dataColumnFilePath(slot, blockRoot, i)
		if _, err := s.fs.Stat(filepath); os.IsNotExist(err) {
			continue // file does not exist
		} else if err != nil {
			return nil, err // some other error
		}
		savedColumns = append(savedColumns, i)
	}
	return savedColumns, nil
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
