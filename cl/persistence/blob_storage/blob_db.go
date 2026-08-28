// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package blob_storage

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/spf13/afero"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/db/kv"
)

//go:generate mockgen -typed=true -destination=./mock_services/blob_storage_mock.go -package=mock_services . BlobStorage
type BlobStorage interface {
	WriteBlobSidecars(ctx context.Context, blockRoot common.Hash, blobSidecars []*cltypes.BlobSidecar) error
	RemoveBlobSidecars(ctx context.Context, slot uint64, blockRoot common.Hash) error
	ReadBlobSidecars(ctx context.Context, slot uint64, blockRoot common.Hash) (out []*cltypes.BlobSidecar, found bool, err error)
	BlobSidecarExists(ctx context.Context, slot uint64, blockRoot common.Hash, idx uint64) (bool, error)
	WriteStream(w io.Writer, slot uint64, blockRoot common.Hash, idx uint64) error // Used for P2P networking
	KzgCommitmentsCount(ctx context.Context, blockRoot common.Hash) (uint32, error)
	PruneBelow(slot uint64) error
}

type BlobStore struct {
	bucketStore
	slotLocks
	db kv.RwDB
}

func NewBlobStore(db kv.RwDB, fs afero.Fs) BlobStorage {
	bs := &BlobStore{db: db}
	bs.bucketStore.init(fs)
	bs.slotLocks.initLocks()
	return bs
}

/*
file system layout: <slot/subdivisionSlot>/<blockRoot>_<index>
indicies:
- <blockRoot> -> kzg_commitments_length // block
*/

// WriteBlobSidecars writes the sidecars on the database. it assumes that all blobSidecars are for the same blockRoot and we have all of them.
func (bs *BlobStore) WriteBlobSidecars(ctx context.Context, blockRoot common.Hash, blobSidecars []*cltypes.BlobSidecar) error {
	// An empty batch writes no file, so it has no slot to lock on; it still records a
	// zero count row, which is what tells "this block has no blobs" from "unknown".
	if len(blobSidecars) > 0 {
		var slot uint64
		for index, sidecar := range blobSidecars {
			if sidecar == nil || sidecar.SignedBlockHeader == nil || sidecar.SignedBlockHeader.Header == nil {
				return errors.New("blob sidecar is missing its signed block header")
			}
			if index == 0 {
				slot = sidecar.SignedBlockHeader.Header.Slot
				continue
			}
			if sidecar.SignedBlockHeader.Header.Slot != slot {
				return errors.New("blob sidecars span multiple slots")
			}
		}
		lock := bs.forSlot(slot)
		lock.Lock()
		if !bs.startWrite(slot) {
			lock.Unlock()
			return nil
		}
		defer bs.finishWrite()
		err := func() error {
			defer lock.Unlock()
			for _, blobSidecar := range blobSidecars {
				if _, err := bs.writeAdmitted(blobSidecar.SignedBlockHeader.Header.Slot, blockRoot, blobSidecar.Index, blobSidecar); err != nil {
					return err
				}
			}
			return nil
		}()
		if err != nil {
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
func (bs *BlobStore) ReadBlobSidecars(ctx context.Context, slot uint64, blockRoot common.Hash) ([]*cltypes.BlobSidecar, bool, error) {
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

	lock := bs.forSlot(slot)
	lock.RLock()
	defer lock.RUnlock()

	var blobSidecars []*cltypes.BlobSidecar
	for i := range kzgCommitmentsLength {
		blobSidecar := &cltypes.BlobSidecar{}
		found, err := bs.read(slot, blockRoot, uint64(i), blobSidecar, clparams.DenebVersion)
		if err != nil {
			return nil, false, err
		}
		if !found {
			return nil, false, nil
		}
		blobSidecars = append(blobSidecars, blobSidecar)
	}
	return blobSidecars, true, nil
}

func (bs *BlobStore) PruneBelow(slot uint64) error {
	return bs.pruneBelow(slot)
}

func (bs *BlobStore) BlobSidecarExists(ctx context.Context, slot uint64, blockRoot common.Hash, idx uint64) (bool, error) {
	return bs.exists(slot, blockRoot, idx)
}

func (bs *BlobStore) WriteStream(w io.Writer, slot uint64, blockRoot common.Hash, idx uint64) error {
	return bs.stream(w, slot, blockRoot, idx)
}

func (bs *BlobStore) KzgCommitmentsCount(ctx context.Context, blockRoot common.Hash) (uint32, error) {
	tx, err := bs.db.BeginRo(ctx)
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

func (bs *BlobStore) RemoveBlobSidecars(ctx context.Context, slot uint64, blockRoot common.Hash) error {
	lock := bs.forSlot(slot)
	lock.Lock()
	defer lock.Unlock()

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
	var firstErr error
	for i := range kzgCommitmentsLength {
		if err := bs.remove(slot, blockRoot, uint64(i)); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	// Drop the row even after a partial failure: a file missing under a surviving row reads as
	// unavailable forever, but a dropped row reads as unknown and lets the block be re-fetched.
	if err := tx.Delete(kv.BlockRootToKzgCommitments, blockRoot[:]); err != nil {
		if firstErr == nil {
			firstErr = err
		}
		return firstErr
	}
	if err := tx.Commit(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

type sidecarsPayload struct {
	blockRoot common.Hash
	sidecars  []*cltypes.BlobSidecar
}

type verifyHeaderSignatureFn func(header *cltypes.SignedBeaconBlockHeader) error

// VerifyBlobSidecars validates sidecar proofs and optionally their signed headers.
func VerifyBlobSidecars(sidecars []*cltypes.BlobSidecar, version clparams.StateVersion, verifySignatureFn func(*cltypes.SignedBeaconBlockHeader) error) error {
	if len(sidecars) == 0 {
		return nil
	}
	blobs := make([]*goethkzg.Blob, len(sidecars))
	commitments := make([]goethkzg.KZGCommitment, len(sidecars))
	proofs := make([]goethkzg.KZGProof, len(sidecars))
	for i, sidecar := range sidecars {
		if sidecar == nil || sidecar.SignedBlockHeader == nil || sidecar.SignedBlockHeader.Header == nil {
			return errors.New("blob response contains incomplete sidecar")
		}
		if version < clparams.GloasVersion && !cltypes.VerifyCommitmentInclusionProof(sidecar.KzgCommitment, sidecar.CommitmentInclusionProof, sidecar.Index, clparams.DenebVersion, sidecar.SignedBlockHeader.Header.BodyRoot) {
			return errors.New("could not verify blob's inclusion proof")
		}
		if verifySignatureFn != nil {
			if err := verifySignatureFn(sidecar.SignedBlockHeader); err != nil {
				return err
			}
		}
		blobs[i] = (*goethkzg.Blob)(&sidecar.Blob)
		commitments[i] = goethkzg.KZGCommitment(sidecar.KzgCommitment)
		proofs[i] = goethkzg.KZGProof(sidecar.KzgProof)
	}
	if err := kzg.Ctx().VerifyBlobKZGProofBatch(blobs, commitments, proofs); err != nil {
		return errors.New("sidecar is wrong")
	}
	return nil
}

// VerifyAgainstIdentifiersAndInsertIntoTheBlobStore does all due verification for blobs before database insertion. it also returns the latest correctly return blob.
func VerifyAgainstIdentifiersAndInsertIntoTheBlobStore(ctx context.Context, storage BlobStorage, identifiers *solid.ListSSZ[*cltypes.BlobIdentifier], sidecars []*cltypes.BlobSidecar, verifySignatureFn verifyHeaderSignatureFn) (uint64, uint64, error) {
	kzgCtx := kzg.Ctx()
	inserted := atomic.Uint64{}
	if identifiers.Len() == 0 || len(sidecars) == 0 {
		return 0, 0, nil
	}
	if len(sidecars) > identifiers.Len() {
		return 0, 0, errors.New("sidecars length is greater than identifiers length")
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
			return 0, 0, errors.New("could not verify blob's inclusion proof")
		}
		if verifySignatureFn != nil {
			// verify the signature of the sidecar head, we leave this step up to the caller to define
			if err := verifySignatureFn(sidecar.SignedBlockHeader); err != nil {
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
		wg.Go(func() {
			blobs := make([]*goethkzg.Blob, len(sds.sidecars))
			for i, sidecar := range sds.sidecars {
				blobs[i] = (*goethkzg.Blob)(&sidecar.Blob)
			}
			kzgCommitments := make([]goethkzg.KZGCommitment, len(sds.sidecars))
			for i, sidecar := range sds.sidecars {
				kzgCommitments[i] = goethkzg.KZGCommitment(sidecar.KzgCommitment)
			}
			kzgProofs := make([]goethkzg.KZGProof, len(sds.sidecars))
			for i, sidecar := range sds.sidecars {
				kzgProofs[i] = goethkzg.KZGProof(sidecar.KzgProof)
			}
			if err := kzgCtx.VerifyBlobKZGProofBatch(blobs, kzgCommitments, kzgProofs); err != nil {
				errAtomic.Store(errors.New("sidecar is wrong"))
				return
			}
			if err := storage.WriteBlobSidecars(ctx, sds.blockRoot, sds.sidecars); err != nil {
				errAtomic.Store(err)
			} else {
				inserted.Add(uint64(len(sds.sidecars)))
			}

		})
	}
	wg.Wait()
	if err := errAtomic.Load(); err != nil {
		return 0, 0, err.(error)
	}
	return lastProcessed, inserted.Load(), nil
}
