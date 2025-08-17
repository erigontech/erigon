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
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/spf13/afero"
)

//go:generate mockgen -typed=true -destination=./mock_services/data_column_storage_mock.go -package=mock_services . DataColumnStorage
type DataColumnStorage interface {
	WriteColumnSidecars(ctx context.Context, blockRoot common.Hash, columnIndex int64, columnData *cltypes.DataColumnSidecar) error
	RemoveColumnSidecars(ctx context.Context, slot uint64, blockRoot common.Hash, columnIndices ...int64) error
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
	emitters          *beaconevents.EventEmitter

	lock sync.RWMutex
}

func NewDataColumnStore(fs afero.Fs, slotsKept uint64, beaconChainConfig *clparams.BeaconChainConfig, ethClock eth_clock.EthereumClock, emitters *beaconevents.EventEmitter) DataColumnStorage {
	impl := &dataColumnStorageImpl{
		fs:                fs,
		beaconChainConfig: beaconChainConfig,
		ethClock:          ethClock,
		slotsKept:         slotsKept,
		emitters:          emitters,
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
	s.lock.Lock()
	defer s.lock.Unlock()
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

	fh.Close()
	s.emitters.Operation().SendDataColumnSidecar(columnData)
	log.Trace("wrote data column sidecar", "slot", columnData.SignedBlockHeader.Header.Slot, "block_root", blockRoot.String(), "column_index", columnIndex)
	return nil
}

func (s *dataColumnStorageImpl) ReadColumnSidecarByColumnIndex(ctx context.Context, slot uint64, blockRoot common.Hash, columnIndex int64) (*cltypes.DataColumnSidecar, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
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
	s.lock.RLock()
	defer s.lock.RUnlock()
	_, filepath := dataColumnFilePath(slot, blockRoot, uint64(columnIndex))
	if _, err := s.fs.Stat(filepath); os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

func (s *dataColumnStorageImpl) RemoveAllColumnSidecars(ctx context.Context, slot uint64, blockRoot common.Hash) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	for i := uint64(0); i < s.beaconChainConfig.NumberOfColumns; i++ {
		_, filepath := dataColumnFilePath(slot, blockRoot, i)
		s.fs.Remove(filepath)
	}
	return nil
}

func (s *dataColumnStorageImpl) RemoveColumnSidecars(ctx context.Context, slot uint64, blockRoot common.Hash, columnIndices ...int64) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, index := range columnIndices {
		_, filepath := dataColumnFilePath(slot, blockRoot, uint64(index))
		if err := s.fs.Remove(filepath); err != nil {
			if os.IsNotExist(err) {
				continue // file does not exist, nothing to remove
			}
			return fmt.Errorf("failed to remove column sidecar: %v", err)
		}
		log.Trace("removed data column sidecar", "slot", slot, "block_root", blockRoot.String(), "column_index", index)
	}
	return nil
}

func (s *dataColumnStorageImpl) WriteStream(w io.Writer, slot uint64, blockRoot common.Hash, idx uint64) error {
	s.lock.RLock()
	defer s.lock.RUnlock()
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
	s.lock.RLock()
	defer s.lock.RUnlock()
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

func (s *dataColumnStorageImpl) Prune(keepSlotDistance uint64) error {
	s.lock.Lock()
	defer s.lock.Unlock()
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
