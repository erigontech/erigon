package blob_storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/spf13/afero"
)

const subdivisionSlot = 10_000

type BlobStorage interface {
	WriteBlobSidecars(ctx context.Context, blockRoot libcommon.Hash, blobSidecars []*cltypes.BlobSidecar) error
	ReadBlobSidecars(ctx context.Context, slot uint64, blockRoot libcommon.Hash) ([]*cltypes.BlobSidecar, bool, error)
	Prune() error
}

type BlobStore struct {
	db                kv.RwDB
	fs                afero.Fs
	beaconChainConfig *clparams.BeaconChainConfig
	genesisConfig     *clparams.GenesisConfig
	slotsKept         uint64
}

func NewBlobStore(db kv.RwDB, fs afero.Fs, slotsKept uint64, beaconChainConfig *clparams.BeaconChainConfig, genesisConfig *clparams.GenesisConfig) *BlobStore {
	return &BlobStore{fs: fs, db: db, slotsKept: slotsKept, beaconChainConfig: beaconChainConfig, genesisConfig: genesisConfig}
}

func blobSidecarFilePath(slot, index uint64, blockRoot libcommon.Hash) (folderpath, filepath string) {
	subdir := slot / subdivisionSlot
	folderpath = strconv.FormatUint(subdir, 10)
	filepath = fmt.Sprintf("%s/%s_%d", folderpath, blockRoot.String(), index)
	return
}

/*
file system layout: <slot/subdivisionSlot>/<blockRoot>_<index>
indicies:
- <blockRoot> -> kzg_commitments_length // block
*/

// WriteBlobSidecars writes the sidecars on the database. it assumes that all blobSidecars are for the same blockRoot and we have all of them.
func (bs *BlobStore) WriteBlobSidecars(ctx context.Context, blockRoot libcommon.Hash, blobSidecars []*cltypes.BlobSidecar) error {

	for _, blobSidecar := range blobSidecars {
		folderPath, filePath := blobSidecarFilePath(
			blobSidecar.SignedBlockHeader.Header.Slot,
			blobSidecar.Index, blockRoot)
		// mkdir the whole folder and subfolders
		bs.fs.MkdirAll(folderPath, 0755)
		// create the file
		file, err := bs.fs.Create(filePath)
		if err != nil {
			return err
		}
		defer file.Close()

		if err := ssz_snappy.EncodeAndWrite(file, blobSidecar); err != nil {
			return err
		}
		if err := file.Sync(); err != nil {
			return err
		}
	}
	val := make([]byte, 4)
	binary.LittleEndian.PutUint32(val, uint32(len(blobSidecars)))
	tx, err := bs.db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Wait for the blob to be written on disk and then write the index on mdbx
	if err := tx.Put(kv.BlockRootToKzgCommitments, blockRoot[:], val); err != nil {
		return err
	}
	return tx.Commit()
}

// ReadBlobSidecars reads the sidecars from the database. it assumes that all blobSidecars are for the same blockRoot and we have all of them.
func (bs *BlobStore) ReadBlobSidecars(ctx context.Context, slot uint64, blockRoot libcommon.Hash) ([]*cltypes.BlobSidecar, bool, error) {
	tx, err := bs.db.BeginRo(ctx)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback()

	val, err := tx.GetOne(kv.BlockRootToKzgCommitments, blockRoot[:])
	if err != nil {
		return nil, false, err
	}
	if len(val) == 0 {
		return nil, false, nil
	}
	kzgCommitmentsLength := binary.LittleEndian.Uint32(val)

	var blobSidecars []*cltypes.BlobSidecar
	for i := uint32(0); i < kzgCommitmentsLength; i++ {
		_, filePath := blobSidecarFilePath(slot, uint64(i), blockRoot)
		file, err := bs.fs.Open(filePath)
		if err != nil {
			if errors.Is(err, afero.ErrFileNotFound) {
				return nil, false, nil
			}
			return nil, false, err
		}
		defer file.Close()

		blobSidecar := &cltypes.BlobSidecar{}
		if err := ssz_snappy.DecodeAndReadNoForkDigest(file, blobSidecar, clparams.DenebVersion); err != nil {
			return nil, false, err
		}
		blobSidecars = append(blobSidecars, blobSidecar)
	}
	return blobSidecars, true, nil
}

// Do a bit of pruning
func (bs *BlobStore) Prune() error {
	if bs.slotsKept == math.MaxUint64 {
		return nil
	}

	currentSlot := utils.GetCurrentSlot(bs.genesisConfig.GenesisTime, bs.beaconChainConfig.SecondsPerSlot)
	currentSlot -= bs.slotsKept
	currentSlot = (currentSlot / subdivisionSlot) * subdivisionSlot
	var startPrune uint64
	if currentSlot >= 1_000_000 {
		startPrune = currentSlot - 1_000_000
	}
	// delete all the folders that are older than slotsKept
	for i := startPrune; i < currentSlot; i += subdivisionSlot {
		bs.fs.RemoveAll(strconv.FormatUint(uint64(i), 10))
	}
	return nil
}
