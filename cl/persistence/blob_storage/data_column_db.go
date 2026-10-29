package blob_storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/bits"
	"os"
	"slices"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
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
	db                kv.RwDB
	beaconChainConfig *clparams.BeaconChainConfig
	emitters          *beaconevents.EventEmitter
}

func NewDataColumnStore(db kv.RwDB, fs afero.Fs, beaconChainConfig *clparams.BeaconChainConfig, emitters *beaconevents.EventEmitter) DataColumnStorage {
	impl := &dataColumnStorageImpl{
		db:                db,
		beaconChainConfig: beaconChainConfig,
		emitters:          emitters,
	}
	impl.bucketStore.init(fs)
	impl.dropRows = deleteRootRows(db, kv.BlockRootToDataColumnBitmap)
	impl.slotLocks.initLocks()
	return impl
}

func (s *dataColumnStorageImpl) columnRow(blockRoot common.Hash) ([]byte, error) {
	var row []byte
	err := s.db.View(context.Background(), func(tx kv.Tx) error {
		v, err := tx.GetOne(kv.BlockRootToDataColumnBitmap, blockRoot[:])
		if uint64(len(v)) == (s.beaconChainConfig.NumberOfColumns+7)/8 {
			row = bytes.Clone(v)
		}
		return err
	})
	return row, err
}

func (s *dataColumnStorageImpl) markColumns(slot uint64, blockRoot common.Hash, stored bool, columns ...uint64) error {
	return s.db.Update(context.Background(), func(tx kv.RwTx) error {
		row, err := tx.GetOne(kv.BlockRootToDataColumnBitmap, blockRoot[:])
		if err != nil {
			return err
		}
		bitmap := make([]byte, (s.beaconChainConfig.NumberOfColumns+7)/8)
		if len(row) != len(bitmap) {
			row = nil
		}
		copy(bitmap, row)
		if row == nil && stored {
			for column := range s.beaconChainConfig.NumberOfColumns {
				exists, err := s.exists(slot, blockRoot, column)
				if err != nil {
					return err
				}
				if exists {
					bitmap[column/8] |= 1 << (column % 8)
				}
			}
		}
		for _, column := range columns {
			if column >= s.beaconChainConfig.NumberOfColumns {
				continue
			}
			if stored {
				bitmap[column/8] |= 1 << (column % 8)
			} else {
				bitmap[column/8] &^= 1 << (column % 8)
			}
		}
		if !slices.ContainsFunc(bitmap, func(b byte) bool { return b != 0 }) {
			return tx.Delete(kv.BlockRootToDataColumnBitmap, blockRoot[:])
		}
		return tx.Put(kv.BlockRootToDataColumnBitmap, blockRoot[:], bitmap)
	})
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
	if err == nil {
		err = s.syncBucket(slot)
	}
	if err == nil {
		err = s.markColumns(slot, blockRoot, true, uint64(columnIndex))
	}
	s.finishWrite()
	if created {
		s.emitters.Operation().SendDataColumnSidecar(beaconevents.NewDataColumnSidecarData(columnData))
		log.Trace("wrote data column sidecar", "slot", slot, "block_root", blockRoot.String(), "column_index", columnIndex)
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
	row, err := s.columnRow(blockRoot)
	if err != nil {
		return false, err
	}
	if row == nil || column >= s.beaconChainConfig.NumberOfColumns {
		return s.exists(slot, blockRoot, column)
	}
	return row[column/8]&(1<<(column%8)) != 0, nil
}

func (s *dataColumnStorageImpl) RemoveColumnSidecars(ctx context.Context, slot uint64, blockRoot common.Hash, columnIndices ...int64) error {
	if len(columnIndices) == 0 {
		return nil
	}
	lock := s.forSlot(slot)
	lock.Lock()
	defer lock.Unlock()
	columns := make([]uint64, len(columnIndices))
	for i, index := range columnIndices {
		columns[i] = uint64(index)
	}
	if err := s.markColumns(slot, blockRoot, false, columns...); err != nil {
		return err
	}
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
	lock := s.forSlot(slot)
	lock.RLock()
	defer lock.RUnlock()
	row, err := s.columnRow(blockRoot)
	if err != nil {
		return nil, err
	}
	var savedColumns []uint64
	if row != nil {
		for i, b := range row {
			for ; b != 0; b &= b - 1 {
				savedColumns = append(savedColumns, uint64(i*8+bits.TrailingZeros8(b)))
			}
		}
		return savedColumns, nil
	}
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
