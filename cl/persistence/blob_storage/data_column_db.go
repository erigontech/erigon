package blob_storage

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
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
	Prune(keepSlotDistance uint64) error
}

type dataColumnStorageImpl struct {
	bucketStore
	slotLocks
	beaconChainConfig *clparams.BeaconChainConfig
	ethClock          eth_clock.EthereumClock
	slotsKept         uint64
	emitters          *beaconevents.EventEmitter
}

func NewDataColumnStore(fs afero.Fs, slotsKept uint64, beaconChainConfig *clparams.BeaconChainConfig, ethClock eth_clock.EthereumClock, emitters *beaconevents.EventEmitter) DataColumnStorage {
	impl := &dataColumnStorageImpl{
		beaconChainConfig: beaconChainConfig,
		ethClock:          ethClock,
		slotsKept:         slotsKept,
		emitters:          emitters,
	}
	impl.bucketStore.init(fs)
	impl.slotLocks.init()
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
	created, err := s.write(slot, blockRoot, uint64(columnIndex), columnData)
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
	for _, index := range columnIndices {
		if err := s.remove(slot, blockRoot, uint64(index)); err != nil {
			return fmt.Errorf("failed to remove column sidecar: %w", err)
		}
		log.Trace("removed data column sidecar", "slot", slot, "block_root", blockRoot.String(), "column_index", index)
	}
	return nil
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

func (s *dataColumnStorageImpl) Prune(keepSlotDistance uint64) error {
	currentSlot := s.ethClock.GetCurrentSlot()
	if currentSlot <= keepSlotDistance {
		return nil
	}
	currentSlot -= keepSlotDistance
	currentSlot = (currentSlot / subdivisionSlot) * subdivisionSlot
	log.Debug("pruning data column sidecars", "cutoff_slot", currentSlot)
	return s.pruneBelow(currentSlot)
}
