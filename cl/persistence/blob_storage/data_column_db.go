package blob_storage

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/spf13/afero"
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
}

type dataColumnStorageImpl struct {
	bucketStore
	slotLocks
	beaconChainConfig *clparams.BeaconChainConfig
	emitters          *beaconevents.EventEmitter
}

func NewDataColumnStore(fs afero.Fs, beaconChainConfig *clparams.BeaconChainConfig, emitters *beaconevents.EventEmitter) DataColumnStorage {
	impl := &dataColumnStorageImpl{
		beaconChainConfig: beaconChainConfig,
		emitters:          emitters,
	}
	impl.bucketStore.init(fs)
	impl.slotLocks.initLocks()
	return impl
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

	lock := s.forSlot(slot)
	lock.Lock()
	defer lock.Unlock()
	if !s.startWrite(slot) {
		return nil
	}
	created, err := s.writeAdmitted(slot, blockRoot, uint64(columnIndex), columnData)
	s.finishWrite()
	if err != nil {
		return err
	}
	if created {
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
	return s.exists(slot, blockRoot, uint64(columnIndex))
}

func (s *dataColumnStorageImpl) RemoveColumnSidecars(ctx context.Context, slot uint64, blockRoot common.Hash, columnIndices ...int64) error {
	lock := s.forSlot(slot)
	lock.Lock()
	defer lock.Unlock()
	var firstErr error
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

func (s *dataColumnStorageImpl) WriteStream(w io.Writer, slot uint64, blockRoot common.Hash, idx uint64) error {
	return s.stream(w, slot, blockRoot, idx)
}

// GetSavedColumnIndex returns the list of saved column indices for the given slot and block root.
func (s *dataColumnStorageImpl) GetSavedColumnIndex(ctx context.Context, slot uint64, blockRoot common.Hash) ([]uint64, error) {
	lock := s.forSlot(slot)
	lock.RLock()
	defer lock.RUnlock()
	var savedColumns []uint64
	for i := uint64(0); i < s.beaconChainConfig.NumberOfColumns; i++ {
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
	return s.pruneBelow(slot)
}
