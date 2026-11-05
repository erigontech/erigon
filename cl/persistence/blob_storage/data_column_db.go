package blob_storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/spf13/afero"
)

// SSZ sizes of a pre-Gloas DataColumnSidecar. Each blob adds a 2048-byte cell, a 48-byte commitment and a 48-byte
// proof. The fixed part is 8 + 3*4 offsets + 208 (signed header) + 32 per inclusion proof hash: 356 with the 4-hash
// proof, 772 with the 17-hash one. Both are below 2144, so length % 2144 tells the two layouts apart.
const (
	dataColumnSidecarBlobSize        = 2144
	legacyDataColumnSidecarFixedSize = 772
)

//go:generate mockgen -typed=true -destination=./mock_services/data_column_storage_mock.go -package=mock_services . DataColumnStorage
type DataColumnStorage interface {
	WriteColumnSidecars(ctx context.Context, blockRoot common.Hash, columnIndex int64, columnData *cltypes.DataColumnSidecar) error
	RemoveColumnSidecars(ctx context.Context, slot uint64, blockRoot common.Hash, columnIndices ...int64) error
	ReadColumnSidecarByColumnIndex(ctx context.Context, slot uint64, blockRoot common.Hash, columnIndex int64) (*cltypes.DataColumnSidecar, error)
	ColumnSidecarExists(ctx context.Context, slot uint64, blockRoot common.Hash, columnIndex int64) (bool, error)
	WriteStream(w io.Writer, slot uint64, blockRoot common.Hash, idx uint64) error // Used for P2P networking
	GetSavedColumnIndex(ctx context.Context, slot uint64, blockRoot common.Hash) ([]uint64, error)
	PruneBelow(slot uint64) error
	EarliestStoredSlot() (uint64, error)
}

type dataColumnStorageImpl struct {
	bucketStore
	slotLocks
	beaconChainConfig *clparams.BeaconChainConfig
	emitters          *beaconevents.EventEmitter
	bitmaps           *columnBitmaps
}

func NewDataColumnStore(fs afero.Fs, beaconChainConfig *clparams.BeaconChainConfig, emitters *beaconevents.EventEmitter) DataColumnStorage {
	impl := &dataColumnStorageImpl{
		beaconChainConfig: beaconChainConfig,
		emitters:          emitters,
		bitmaps:           newColumnBitmaps(),
	}
	impl.bucketStore.init(fs)
	impl.slotLocks.initLocks()
	return impl
}

// forRoot stripes by block root, not slot: a root's files are named by its bucket and root
// alone, so callers passing different slots of one bucket must still serialize, or a scan
// could seed a bitmap around a write or remove of the same root.
func (s *dataColumnStorageImpl) forRoot(root common.Hash) *sync.RWMutex {
	return &s.locks[binary.LittleEndian.Uint64(root[:8])%rwLocksCount]
}

func (s *dataColumnStorageImpl) columnKey(slot uint64, root common.Hash) columnKey {
	return columnKey{root: root, bucket: slot / subdivisionSlot, width: s.beaconChainConfig.NumberOfColumns}
}

func (s *dataColumnStorageImpl) WriteColumnSidecars(ctx context.Context, blockRoot common.Hash, columnIndex int64, columnData *cltypes.DataColumnSidecar) error {
	// Get slot from sidecar - version-aware handling
	// For Fulu: slot is in SignedBlockHeader.Header.Slot
	// For GLOAS: slot is directly in Slot field
	var slot uint64
	switch {
	case columnData.Version() >= clparams.GloasVersion:
		slot = columnData.Slot
	case columnData.SignedBlockHeader != nil:
		slot = columnData.SignedBlockHeader.Header.Slot
	default:
		slot = columnData.Slot // fallback
	}

	// Ensure BlockRoot and Slot are set
	columnData.BlockRoot = blockRoot
	columnData.Slot = slot

	created, err := func() (bool, error) {
		lock := s.forRoot(blockRoot)
		lock.Lock()
		defer lock.Unlock()
		if !s.startWrite(slot) {
			return false, nil
		}
		defer s.finishWrite()
		created, err := s.writeAdmitted(slot, blockRoot, uint64(columnIndex), columnData)
		if err != nil {
			return false, err
		}
		s.bitmaps.set(s.columnKey(slot, blockRoot), uint64(columnIndex))
		return created, nil
	}()
	if err != nil {
		return err
	}
	if created {
		s.emitters.NotifyDataColumnStored(blockRoot)
		s.emitters.Operation().SendDataColumnSidecar(beaconevents.NewDataColumnSidecarData(columnData))
		log.Trace("wrote data column sidecar", "slot", slot, "block_root", blockRoot.String(), "column_index", columnIndex)
	}
	return nil
}

func (s *dataColumnStorageImpl) ReadColumnSidecarByColumnIndex(ctx context.Context, slot uint64, blockRoot common.Hash, columnIndex int64) (*cltypes.DataColumnSidecar, error) {
	data := &cltypes.DataColumnSidecar{}
	version := s.beaconChainConfig.GetCurrentStateVersion(slot / s.beaconChainConfig.SlotsPerEpoch)
	found, err := s.read(slot, blockRoot, uint64(columnIndex), data, version)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, os.ErrNotExist
	}
	// BlockRoot and Slot are not part of SSZ schema, set them from parameters
	data.BlockRoot = blockRoot
	data.Slot = slot
	return data, nil
}

func (s *dataColumnStorageImpl) ColumnSidecarExists(ctx context.Context, slot uint64, blockRoot common.Hash, columnIndex int64) (bool, error) {
	column := uint64(columnIndex)
	// The gossip and peer-DAS paths call this per column, so it reads the floor without
	// queueing behind a prune waiting on pruneMutex. A prune that raises the floor after this
	// check removes the bucket after the answer, as it would after a stat.
	if slot >= s.floor.Load() {
		if stored, known := s.bitmaps.has(s.columnKey(slot, blockRoot), column); known {
			return stored, nil
		}
	}
	return s.exists(slot, blockRoot, column)
}

func (s *dataColumnStorageImpl) RemoveColumnSidecars(ctx context.Context, slot uint64, blockRoot common.Hash, columnIndices ...int64) error {
	if len(columnIndices) == 0 {
		return nil
	}
	lock := s.forRoot(blockRoot)
	lock.Lock()
	defer lock.Unlock()
	columns := make([]uint64, len(columnIndices))
	for i, index := range columnIndices {
		columns[i] = uint64(index)
	}
	s.bitmaps.clear(s.columnKey(slot, blockRoot), columns)
	var firstErr error
	defer func() {
		if firstErr != nil {
			// A file that failed to go may still be there, so its cleared bit no longer
			// matches the disk; let the files answer for this root again.
			s.bitmaps.forget(blockRoot)
		}
	}()
	for _, index := range columnIndices {
		if err := s.remove(slot, blockRoot, uint64(index)); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("failed to remove column sidecar: %w", err)
			}
			continue
		}
		log.Trace("removed data column sidecar", "slot", slot, "block_root", blockRoot.String(), "column_index", index)
	}
	return firstErr
}

// WriteStream re-encodes a pre-Gloas sidecar stored with the 17-hash inclusion proof, so peers get the canonical layout,
// and copies every other file as stored.
func (s *dataColumnStorageImpl) WriteStream(w io.Writer, slot uint64, blockRoot common.Hash, idx uint64) error {
	_, file := s.path(slot, blockRoot, idx)
	fh, err := s.fs.Open(file)
	if err != nil {
		return err
	}
	defer fh.Close()
	version := s.beaconChainConfig.GetCurrentStateVersion(slot / s.beaconChainConfig.SlotsPerEpoch)
	if version < clparams.GloasVersion {
		length, err := ssz_snappy.ReadUvarint(fh)
		if err != nil {
			return err
		}
		if _, err := fh.Seek(0, io.SeekStart); err != nil {
			return err
		}
		if length%dataColumnSidecarBlobSize == legacyDataColumnSidecarFixedSize {
			sidecar := &cltypes.DataColumnSidecar{}
			if err := ssz_snappy.DecodeAndReadNoForkDigest(fh, sidecar, version); err != nil {
				return err
			}
			return ssz_snappy.EncodeAndWrite(w, sidecar)
		}
	}
	_, err = io.Copy(w, fh)
	return err
}

// GetSavedColumnIndex returns the list of saved column indices for the given slot and block root.
func (s *dataColumnStorageImpl) GetSavedColumnIndex(ctx context.Context, slot uint64, blockRoot common.Hash) ([]uint64, error) {
	lock := s.forRoot(blockRoot)
	lock.RLock()
	defer lock.RUnlock()
	key := s.columnKey(slot, blockRoot)
	if !s.startWrite(slot) {
		return s.scanSavedColumns(slot, key)
	}
	defer s.finishWrite()
	if saved, ok := s.bitmaps.columns(key); ok {
		return saved, nil
	}
	saved, err := s.scanSavedColumns(slot, key)
	if err != nil {
		return nil, err
	}
	s.bitmaps.seed(key, saved)
	return saved, nil
}

func (s *dataColumnStorageImpl) scanSavedColumns(slot uint64, key columnKey) ([]uint64, error) {
	blockRoot := key.root
	var savedColumns []uint64
	for i := uint64(0); i < key.width; i++ {
		exists, err := s.exists(slot, blockRoot, i)
		if err != nil {
			return nil, err
		}
		if !exists {
			continue
		}
		savedColumns = append(savedColumns, i)
	}
	return savedColumns, nil
}

func (s *dataColumnStorageImpl) PruneBelow(slot uint64) error {
	log.Debug("pruning data column sidecars", "cutoff_slot", slot)
	err := s.pruneBelow(slot)
	s.pruneMutex.RLock()
	floor := s.pruneFloor
	s.pruneMutex.RUnlock()
	s.bitmaps.dropBelow(floor / subdivisionSlot)
	return err
}
