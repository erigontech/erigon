package blob_storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
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
	WriteColumnSidecarsBatch(ctx context.Context, blockRoot common.Hash, sidecars []*cltypes.DataColumnSidecar) error
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
		bitmaps:           newColumnBitmaps(maxColumnBitmaps),
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

func sidecarSlot(sidecar *cltypes.DataColumnSidecar) uint64 {
	if sidecar.Version() < clparams.GloasVersion && sidecar.SignedBlockHeader != nil {
		return sidecar.SignedBlockHeader.Header.Slot
	}
	return sidecar.Slot
}

func (s *dataColumnStorageImpl) WriteColumnSidecars(ctx context.Context, blockRoot common.Hash, columnIndex int64, columnData *cltypes.DataColumnSidecar) error {
	slot := sidecarSlot(columnData)
	columnData.BlockRoot = blockRoot
	columnData.Slot = slot
	return s.writeColumns(blockRoot, slot, []batchEntry{{idx: uint64(columnIndex), v: columnData}}, []*cltypes.DataColumnSidecar{columnData})
}

// WriteColumnSidecarsBatch stores the sidecars of one block: it validates them all first, skips
// columns already on disk, and renames the new ones into place only after all of them are fsynced.
func (s *dataColumnStorageImpl) WriteColumnSidecarsBatch(ctx context.Context, blockRoot common.Hash, sidecars []*cltypes.DataColumnSidecar) error {
	if len(sidecars) == 0 {
		return nil
	}
	if slices.Contains(sidecars, nil) {
		return errors.New("nil column sidecar")
	}
	slot := sidecarSlot(sidecars[0])
	entries := make([]batchEntry, len(sidecars))
	seen := make(map[uint64]struct{}, len(sidecars))
	for i, sidecar := range sidecars {
		if got := sidecarSlot(sidecar); got != slot {
			return fmt.Errorf("column sidecars span slots %d and %d", slot, got)
		}
		if sidecar.Index >= s.beaconChainConfig.NumberOfColumns {
			return fmt.Errorf("column index %d out of range", sidecar.Index)
		}
		if _, dup := seen[sidecar.Index]; dup {
			return fmt.Errorf("duplicate column index %d", sidecar.Index)
		}
		seen[sidecar.Index] = struct{}{}
		entries[i] = batchEntry{idx: sidecar.Index, v: sidecar}
	}
	for _, sidecar := range sidecars {
		sidecar.BlockRoot = blockRoot
		sidecar.Slot = slot
	}
	return s.writeColumns(blockRoot, slot, entries, sidecars)
}

func (s *dataColumnStorageImpl) writeColumns(blockRoot common.Hash, slot uint64, entries []batchEntry, sidecars []*cltypes.DataColumnSidecar) error {
	created, err := func() ([]int, error) {
		lock := s.forRoot(blockRoot)
		lock.Lock()
		defer lock.Unlock()
		if !s.startWrite(slot) {
			return nil, nil
		}
		defer s.finishWrite()
		existing, created, err := s.writeMissingAdmitted(slot, blockRoot, entries)
		key := s.columnKey(slot, blockRoot)
		for _, i := range slices.Concat(existing, created) {
			s.bitmaps.set(key, entries[i].idx)
		}
		return created, err
	}()
	if len(created) > 0 {
		s.emitters.NotifyDataColumnStored(blockRoot)
		for _, i := range created {
			s.emitters.Operation().SendDataColumnSidecar(beaconevents.NewDataColumnSidecarData(sidecars[i]))
			log.Trace("wrote data column sidecar", "slot", slot, "block_root", blockRoot.String(), "column_index", entries[i].idx)
		}
	}
	return err
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
	// Called per column from gossip and peer-DAS, so it must not queue behind a prune waiting
	// on pruneMutex. A prune that raises the floor after this load removes the bucket after
	// the answer, as it would after a stat.
	if slot >= s.pruneFloor.Load() {
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
	key := s.columnKey(slot, blockRoot)
	var firstErr error
	defer func() {
		if firstErr != nil {
			// A file that failed to go may still be there; let the files answer for this
			// root again.
			s.bitmaps.forget(blockRoot)
		}
	}()
	for _, index := range columnIndices {
		// The bit goes after the file, so ColumnSidecarExists, which takes no root lock,
		// never reports missing a file that a failed remove leaves in place. Until then it
		// answers as a stat taken just before the unlink would.
		if err := s.remove(slot, blockRoot, uint64(index)); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("failed to remove column sidecar: %w", err)
			}
			continue
		}
		s.bitmaps.clear(key, uint64(index))
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
	s.bitmaps.dropBelow(s.pruneFloor.Load() / subdivisionSlot)
	return err
}
