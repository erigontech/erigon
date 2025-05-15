package blob_storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"sync"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/spf13/afero"
)

const (
	// subdivisionSlot = 10_000
	mutexSize = 32
)

type DataCloumnStorage interface {
	WriteColumnSidecars(ctx context.Context, blockRoot common.Hash, columnIndex int64, columnData *cltypes.DataColumnSidecar) error
	RemoveColumnSidecars(ctx context.Context, slot uint64, blockRoot common.Hash) error

	//WriteStream(w io.Writer, slot uint64, blockRoot common.Hash, idx uint64) error // Used for P2P networking
	//KzgCommitmentsCount(ctx context.Context, blockRoot common.Hash) (uint32, error)
	//Prune() error
}

type dataCloumnStorageImpl struct {
	db                kv.RwDB
	fs                afero.Fs
	beaconChainConfig *clparams.BeaconChainConfig
	ethClock          eth_clock.EthereumClock
	slotsKept         uint64

	mutexes map[uint64]*sync.RWMutex
}

func NewDataColumnStore(db kv.RwDB, fs afero.Fs, slotsKept uint64, beaconChainConfig *clparams.BeaconChainConfig, ethClock eth_clock.EthereumClock) DataCloumnStorage {
	impl := &dataCloumnStorageImpl{
		db:                db,
		fs:                fs,
		beaconChainConfig: beaconChainConfig,
		ethClock:          ethClock,
		slotsKept:         slotsKept,
		mutexes:           make(map[uint64]*sync.RWMutex, mutexSize),
	}
	for i := uint64(0); i < mutexSize; i++ {
		impl.mutexes[i] = &sync.RWMutex{}
	}
	return impl
}

func dataColumnFilePath(slot uint64, blockRoot common.Hash, columnIndex uint64) (dir, filepath string) {
	subdir := slot / subdivisionSlot
	dir = strconv.FormatUint(subdir, 10)
	filepath = fmt.Sprintf("%s/%s_%d", dir, blockRoot.String(), columnIndex)
	return
}

func (s *dataCloumnStorageImpl) WriteColumnSidecars(ctx context.Context, blockRoot common.Hash, columnIndex int64, columnData *cltypes.DataColumnSidecar) error {
	mutex := s.acquireDataMutexBySlot(columnData.SignedBlockHeader.Header.Slot)
	mutex.Lock()
	defer mutex.Unlock()

	dir, filepath := dataColumnFilePath(columnData.SignedBlockHeader.Header.Slot, blockRoot, uint64(columnIndex))
	if err := s.fs.MkdirAll(dir, 0755); err != nil {
		return err
	}
	if _, err := s.fs.Stat(filepath); err == nil {
		// File already exists, no need to write again
		// TODO: Need to check content?
		return nil
	}
	fh, err := s.fs.Create(filepath)
	if err != nil {
		return err
	}
	if err := ssz_snappy.EncodeAndWrite(fh, columnData); err != nil {
		fh.Close()
		s.fs.Remove(filepath)
		return err
	}
	defer fh.Close()
	if err := fh.Sync(); err != nil {
		return err
	}

	// increment the column count
	tx, err := s.db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	bytes, err := tx.GetOne(kv.BlockRootToDataColumns, blockRoot[:])
	if err != nil {
		return err
	}
	curCount := uint32(0)
	if bytes != nil {
		curCount = binary.LittleEndian.Uint32(bytes)
	}
	curCount++
	val := make([]byte, 4)
	binary.LittleEndian.PutUint32(val, curCount)
	if err := tx.Put(kv.BlockRootToDataColumns, blockRoot[:], val); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *dataCloumnStorageImpl) RemoveColumnSidecars(ctx context.Context, slot uint64, blockRoot common.Hash) error {
	mutex := s.acquireDataMutexBySlot(slot)
	mutex.Lock()
	defer mutex.Unlock()

	_, filepath := dataColumnFilePath(slot, blockRoot, uint64(columnIndex))
	if err := s.fs.Remove(filepath); err != nil {
		return err
	}
	return nil
}

func (s *dataCloumnStorageImpl) acquireDataMutexBySlot(slot uint64) *sync.RWMutex {
	index := slot % mutexSize
	return s.mutexes[index]
}
