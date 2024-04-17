package blob_storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"sync"
	"sync/atomic"

	gokzg4844 "github.com/crate-crypto/go-kzg-4844"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/crypto/kzg"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/ledgerwatch/erigon/cl/utils/eth_clock"
	"github.com/spf13/afero"
)

const subdivisionSlot = 10_000

type BlobStorage interface {
	WriteBlobSidecars(ctx context.Context, blockRoot libcommon.Hash, blobSidecars []*cltypes.BlobSidecar) error
	RemoveBlobSidecars(ctx context.Context, slot uint64, blockRoot libcommon.Hash) error
	ReadBlobSidecars(ctx context.Context, slot uint64, blockRoot libcommon.Hash) (out []*cltypes.BlobSidecar, found bool, err error)
	WriteStream(w io.Writer, slot uint64, blockRoot libcommon.Hash, idx uint64) error // Used for P2P networking
	KzgCommitmentsCount(ctx context.Context, blockRoot libcommon.Hash) (uint32, error)
	Prune() error
}

type BlobStore struct {
	db                kv.RwDB
	fs                afero.Fs
	beaconChainConfig *clparams.BeaconChainConfig
	ethClock          eth_clock.EthereumClock
	slotsKept         uint64
}

func NewBlobStore(db kv.RwDB, fs afero.Fs, slotsKept uint64, beaconChainConfig *clparams.BeaconChainConfig, ethClock eth_clock.EthereumClock) BlobStorage {
	return &BlobStore{fs: fs, db: db, slotsKept: slotsKept, beaconChainConfig: beaconChainConfig, ethClock: ethClock}
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

	currentSlot := bs.ethClock.GetCurrentSlot()
	currentSlot -= bs.slotsKept
	currentSlot = (currentSlot / subdivisionSlot) * subdivisionSlot
	var startPrune uint64
	if currentSlot >= 1_000_000 {
		startPrune = currentSlot - 1_000_000
	}
	// delete all the folders that are older than slotsKept
	for i := startPrune; i < currentSlot; i += subdivisionSlot {
		bs.fs.RemoveAll(strconv.FormatUint(i, 10))
	}
	return nil
}

func (bs *BlobStore) WriteStream(w io.Writer, slot uint64, blockRoot libcommon.Hash, idx uint64) error {
	_, filePath := blobSidecarFilePath(slot, idx, blockRoot)
	file, err := bs.fs.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.Copy(w, file)
	return err
}

func (bs *BlobStore) KzgCommitmentsCount(ctx context.Context, blockRoot libcommon.Hash) (uint32, error) {
	tx, err := bs.db.BeginRo(context.Background())
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	val, err := tx.GetOne(kv.BlockRootToKzgCommitments, blockRoot[:])
	if err != nil {
		return 0, err
	}
	if len(val) != 4 {
		return 0, nil
	}
	return binary.LittleEndian.Uint32(val), nil
}

func (bs *BlobStore) RemoveBlobSidecars(ctx context.Context, slot uint64, blockRoot libcommon.Hash) error {
	tx, err := bs.db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	val, err := tx.GetOne(kv.BlockRootToKzgCommitments, blockRoot[:])
	if err != nil {
		return err
	}
	if len(val) == 0 {
		return nil
	}
	kzgCommitmentsLength := binary.LittleEndian.Uint32(val)
	for i := uint32(0); i < kzgCommitmentsLength; i++ {
		_, filePath := blobSidecarFilePath(slot, uint64(i), blockRoot)
		if err := bs.fs.Remove(filePath); err != nil {
			return err
		}
		tx.Delete(kv.BlockRootToKzgCommitments, blockRoot[:])
	}
	return tx.Commit()
}

type sidecarsPayload struct {
	blockRoot libcommon.Hash
	sidecars  []*cltypes.BlobSidecar
}

type verifyHeaderSignatureFn func(header *cltypes.SignedBeaconBlockHeader) error

// VerifyAgainstIdentifiersAndInsertIntoTheBlobStore does all due verification for blobs before database insertion. it also returns the latest correctly return blob.
func VerifyAgainstIdentifiersAndInsertIntoTheBlobStore(ctx context.Context, storage BlobStorage, identifiers *solid.ListSSZ[*cltypes.BlobIdentifier], sidecars []*cltypes.BlobSidecar, verifySignatureFn verifyHeaderSignatureFn) (uint64, uint64, error) {
	kzgCtx := kzg.Ctx()
	inserted := atomic.Uint64{}
	if identifiers.Len() == 0 || len(sidecars) == 0 {
		return 0, 0, nil
	}
	if len(sidecars) > identifiers.Len() {
		return 0, 0, fmt.Errorf("sidecars length is greater than identifiers length")
	}
	prevBlockRoot := identifiers.Get(0).BlockRoot
	totalProcessed := 0

	storableSidecars := []*sidecarsPayload{}
	currentSidecarsPayload := &sidecarsPayload{blockRoot: identifiers.Get(0).BlockRoot}
	lastProcessed := sidecars[0].SignedBlockHeader.Header.Slot
	// Some will be stored, truncate when validation goes to shit
	for i, sidecar := range sidecars {
		identifier := identifiers.Get(i)
		// check if the root of the block matches the identifier
		sidecarBlockRoot, err := sidecar.SignedBlockHeader.Header.HashSSZ()
		if err != nil {
			return 0, 0, err
		}
		if sidecarBlockRoot != identifier.BlockRoot {
			break
		}
		// check if the index of the sidecar matches the identifier
		if sidecars[i].Index != identifier.Index {
			break
		}

		if !cltypes.VerifyCommitmentInclusionProof(sidecar.KzgCommitment, sidecar.CommitmentInclusionProof, sidecar.Index, clparams.DenebVersion, sidecar.SignedBlockHeader.Header.BodyRoot) {
			return 0, 0, fmt.Errorf("could not verify blob's inclusion proof")
		}
		if verifySignatureFn != nil {
			// verify the signature of the sidecar head, we leave this step up to the caller to define
			if verifySignatureFn(sidecar.SignedBlockHeader); err != nil {
				return 0, 0, err
			}
		}
		// if the sidecar is valid, add it to the current payload of sidecars being built.
		if identifier.BlockRoot != prevBlockRoot {
			storableSidecars = append(storableSidecars, currentSidecarsPayload)
			if len(currentSidecarsPayload.sidecars) != 0 {
				lastProcessed = currentSidecarsPayload.sidecars[len(currentSidecarsPayload.sidecars)-1].SignedBlockHeader.Header.Slot
			}
			currentSidecarsPayload = &sidecarsPayload{blockRoot: identifier.BlockRoot}
		}
		currentSidecarsPayload.sidecars = append(currentSidecarsPayload.sidecars, sidecar)
		totalProcessed++
		prevBlockRoot = identifier.BlockRoot
	}
	if totalProcessed == identifiers.Len() {
		storableSidecars = append(storableSidecars, currentSidecarsPayload)
		lastProcessed = sidecars[len(sidecars)-1].SignedBlockHeader.Header.Slot
	}

	var errAtomic atomic.Value
	var wg sync.WaitGroup
	for _, sds := range storableSidecars {
		wg.Add(1)
		go func(sds *sidecarsPayload) {
			defer wg.Done()
			blobs := make([]gokzg4844.Blob, len(sds.sidecars))
			for i, sidecar := range sds.sidecars {
				blobs[i] = gokzg4844.Blob(sidecar.Blob)
			}
			kzgCommitments := make([]gokzg4844.KZGCommitment, len(sds.sidecars))
			for i, sidecar := range sds.sidecars {
				kzgCommitments[i] = gokzg4844.KZGCommitment(sidecar.KzgCommitment)
			}
			kzgProofs := make([]gokzg4844.KZGProof, len(sds.sidecars))
			for i, sidecar := range sds.sidecars {
				kzgProofs[i] = gokzg4844.KZGProof(sidecar.KzgProof)
			}
			if err := kzgCtx.VerifyBlobKZGProofBatch(blobs, kzgCommitments, kzgProofs); err != nil {
				errAtomic.Store(fmt.Errorf("sidecar is wrong"))
				return
			}
			if err := storage.WriteBlobSidecars(ctx, sds.blockRoot, sds.sidecars); err != nil {
				errAtomic.Store(err)
			} else {
				inserted.Add(uint64(len(sds.sidecars)))
			}

		}(sds)
	}
	wg.Wait()
	if err := errAtomic.Load(); err != nil {
		return 0, 0, err.(error)
	}
	return lastProcessed, inserted.Load(), nil
}
