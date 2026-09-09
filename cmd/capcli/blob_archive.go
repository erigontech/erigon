// Copyright 2026 The Erigon Authors
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

package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	goethkzg "github.com/crate-crypto/go-eth-kzg"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

// blobLenBytes is the fixed size of a blob: 4096 field elements of 32 bytes.
const blobLenBytes = 131072

// versionedHashFor derives the archive lookup key from a KZG commitment. The leading byte is
// the blob-commitment version, per EIP-4844.
func versionedHashFor(commitment common.Bytes48) common.Hash {
	sum := sha256.Sum256(commitment[:])
	sum[0] = 0x01
	return common.Hash(sum)
}

// verifyPayloadAgainstCommitment recomputes the commitment from an archive payload and
// rejects it unless it matches what the chain recorded, then derives the KZG proof. Archives
// are not required to store a usable proof, so it is always recomputed locally.
func verifyPayloadAgainstCommitment(payload []byte, want common.Bytes48) (goethkzg.KZGCommitment, goethkzg.KZGProof, error) {
	var (
		zeroC goethkzg.KZGCommitment
		zeroP goethkzg.KZGProof
	)
	if len(payload) != blobLenBytes {
		return zeroC, zeroP, fmt.Errorf("unexpected blob length %d, want %d", len(payload), blobLenBytes)
	}
	var blob goethkzg.Blob
	copy(blob[:], payload)

	ctx := kzg.Ctx()
	commitment, err := ctx.BlobToKZGCommitment(&blob, 0)
	if err != nil {
		return zeroC, zeroP, fmt.Errorf("blob to commitment: %w", err)
	}
	if common.Bytes48(commitment) != want {
		return zeroC, zeroP, fmt.Errorf("commitment mismatch: payload gives %x, block says %x", commitment, want)
	}
	proof, err := ctx.ComputeBlobKZGProof(&blob, commitment, 0)
	if err != nil {
		return zeroC, zeroP, fmt.Errorf("compute proof: %w", err)
	}
	return commitment, proof, nil
}

// archiveSource fetches full blocks from beacon endpoints and blob payloads from a Blobscan
// style archive. Payloads live behind expiring signed URLs, so each one is resolved fresh.
type archiveSource struct {
	beaconEndpoints []string
	blobscanBase    string
	client          *http.Client
	maxAttempts     int
	pause           time.Duration
}

func newArchiveSource(beaconEndpoints []string, blobscanBase string, maxAttempts int, timeout time.Duration) *archiveSource {
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	return &archiveSource{
		beaconEndpoints: beaconEndpoints,
		blobscanBase:    blobscanBase,
		client:          &http.Client{Timeout: timeout},
		maxAttempts:     maxAttempts,
	}
}

// getRetry distinguishes three outcomes that must never be conflated: the resource is absent
// (found=false, no error), the request kept failing (error), or it succeeded. Throttling and
// server errors are retried with a growing delay; a 404 is an answer and is returned at once.
func (s *archiveSource) getRetry(ctx context.Context, url, accept string) ([]byte, bool, error) {
	var lastErr error
	for attempt := 1; attempt <= s.maxAttempts; attempt++ {
		if attempt > 1 {
			delay := time.Duration(attempt-1) * 500 * time.Millisecond
			select {
			case <-ctx.Done():
				return nil, false, ctx.Err()
			case <-time.After(delay):
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, false, err
		}
		req.Header.Set("Accept", accept)
		resp, err := s.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()

		switch {
		case resp.StatusCode == http.StatusNotFound:
			return nil, false, nil
		case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
			lastErr = fmt.Errorf("status %d", resp.StatusCode)
			log.Warn("Archive throttled or erroring, backing off",
				"status", resp.StatusCode, "attempt", attempt, "of", s.maxAttempts)
			continue
		case resp.StatusCode != http.StatusOK:
			return nil, false, fmt.Errorf("status %d", resp.StatusCode)
		}
		if readErr != nil {
			lastErr = readErr
			continue
		}
		return body, true, nil
	}
	return nil, false, fmt.Errorf("gave up after %d attempts: %w", s.maxAttempts, lastErr)
}

// fullBlock returns the canonical block for a slot, including its execution payload. The
// payload matters: KzgCommitmentMerkleProof hashes the non-storage body schema, so a block
// read without it yields a branch that verifies against nothing.
func (s *archiveSource) fullBlock(ctx context.Context, slot uint64, beaconCfg *clparams.BeaconChainConfig) (*cltypes.SignedBeaconBlock, error) {
	var lastErr error
	for _, endpoint := range s.beaconEndpoints {
		raw, found, err := s.getRetry(ctx, fmt.Sprintf("%s/eth/v2/beacon/blocks/%d", endpoint, slot), "application/octet-stream")
		if err != nil {
			lastErr = err
			log.Warn("Beacon endpoint failed", "endpoint", endpoint, "slot", slot, "err", err)
			continue
		}
		if !found {
			continue
		}
		if len(raw) < 108 {
			lastErr = fmt.Errorf("block response too short: %d bytes", len(raw))
			continue
		}
		gotSlot := binary.LittleEndian.Uint64(raw[100:108])
		if gotSlot != slot {
			lastErr = fmt.Errorf("endpoint answered slot %d for %d", gotSlot, slot)
			continue
		}
		v := beaconCfg.GetCurrentStateVersion(slot / beaconCfg.SlotsPerEpoch)
		block := cltypes.NewSignedBeaconBlock(beaconCfg, v)
		if err := block.DecodeSSZ(raw, int(v)); err != nil {
			lastErr = fmt.Errorf("decode block: %w", err)
			continue
		}
		return block, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, nil
}

// blobPayload resolves a versioned hash to its bytes. The archive returns storage references
// rather than the payload, and the signed URLs it hands out are short lived.
func (s *archiveSource) blobPayload(ctx context.Context, versionedHash common.Hash) ([]byte, bool, error) {
	body, found, err := s.getRetry(ctx,
		fmt.Sprintf("%s/blobs/%s", s.blobscanBase, versionedHash.Hex()), "application/json")
	if err != nil || !found {
		return nil, found, err
	}
	var meta struct {
		Size                  int    `json:"size"`
		Commitment            string `json:"commitment"`
		DataStorageReferences []struct {
			Storage string `json:"storage"`
			URL     string `json:"url"`
		} `json:"dataStorageReferences"`
	}
	if err := json.Unmarshal(body, &meta); err != nil {
		return nil, false, fmt.Errorf("decode blob metadata: %w", err)
	}
	if meta.Size != blobLenBytes {
		return nil, false, fmt.Errorf("archive reports size %d, want %d", meta.Size, blobLenBytes)
	}
	if len(meta.DataStorageReferences) == 0 {
		return nil, false, errors.New("archive holds metadata but no payload reference")
	}
	var lastErr error
	for _, ref := range meta.DataStorageReferences {
		payload, ok, err := s.getRetry(ctx, ref.URL, "application/octet-stream")
		if err != nil {
			lastErr = err
			log.Warn("Payload store failed", "storage", ref.Storage, "err", err)
			continue
		}
		if !ok {
			continue
		}
		return payload, true, nil
	}
	if lastErr != nil {
		return nil, false, lastErr
	}
	return nil, false, nil
}

// buildVerifiedSidecars assembles the sidecars for a block from archive payloads, verifying
// each one the same way the store does before it is offered for insertion: the payload must
// reproduce the block's commitment, and the inclusion proof must resolve to the body root.
// Every sidecar for the block must be present, because writing a subset would rewrite the
// count row to the subset's size and orphan the rest.
func buildVerifiedSidecars(block *cltypes.SignedBeaconBlock, payloads map[uint64][]byte) ([]*cltypes.BlobSidecar, error) {
	commitments := block.Block.Body.BlobKzgCommitments
	if commitments == nil || commitments.Len() == 0 {
		return nil, errors.New("block carries no commitments")
	}
	want := commitments.Len()
	if len(payloads) != want {
		return nil, fmt.Errorf("have %d payloads for %d commitments", len(payloads), want)
	}
	header := block.SignedBeaconBlockHeader()
	bodyRoot := header.Header.BodyRoot

	blobs := make([]*goethkzg.Blob, 0, want)
	comms := make([]goethkzg.KZGCommitment, 0, want)
	proofs := make([]goethkzg.KZGProof, 0, want)
	out := make([]*cltypes.BlobSidecar, 0, want)

	for i := range want {
		idx := uint64(i)
		payload, ok := payloads[idx]
		if !ok {
			return nil, fmt.Errorf("missing payload for index %d", idx)
		}
		onChain := common.Bytes48(*commitments.Get(i))
		commitment, proof, err := verifyPayloadAgainstCommitment(payload, onChain)
		if err != nil {
			return nil, fmt.Errorf("index %d: %w", idx, err)
		}
		branch, err := block.Block.Body.KzgCommitmentMerkleProof(i)
		if err != nil {
			return nil, fmt.Errorf("index %d: inclusion proof: %w", idx, err)
		}
		if len(branch) != cltypes.CommitmentBranchSize {
			return nil, fmt.Errorf("index %d: branch length %d, want %d", idx, len(branch), cltypes.CommitmentBranchSize)
		}
		inclusion := solid.NewHashVector(cltypes.CommitmentBranchSize)
		for j, h := range branch {
			inclusion.Set(j, h)
		}
		if !cltypes.VerifyCommitmentInclusionProof(onChain, inclusion, idx, clparams.DenebVersion, bodyRoot) {
			return nil, fmt.Errorf("index %d: inclusion proof does not resolve to the body root", idx)
		}

		var blob goethkzg.Blob
		copy(blob[:], payload)
		sidecar := &cltypes.BlobSidecar{
			Index:                    idx,
			Blob:                     cltypes.Blob(blob),
			KzgCommitment:            common.Bytes48(commitment),
			KzgProof:                 common.Bytes48(proof),
			SignedBlockHeader:        header,
			CommitmentInclusionProof: inclusion,
		}
		out = append(out, sidecar)
		blobs = append(blobs, &blob)
		comms = append(comms, commitment)
		proofs = append(proofs, proof)
	}

	if err := kzg.Ctx().VerifyBlobKZGProofBatch(blobs, comms, proofs); err != nil {
		return nil, fmt.Errorf("kzg batch verification failed: %w", err)
	}
	return out, nil
}

// fillSlotFromArchive rebuilds one slot's sidecars from a blob archive. The block used to
// derive every proof must be the one the local index calls canonical: without that gate a
// reorged or foreign block could be used to build sidecars that verify against themselves
// but do not belong to this chain.
func (c *BlobFetchToStore) fillSlotFromArchive(ctx context.Context, tx kv.Tx, store blob_storage.BlobStorage,
	src *archiveSource, beaconCfg *clparams.BeaconChainConfig, slot uint64, tally *blobFetchTally) error {
	localRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, slot)
	if err != nil {
		return err
	}
	if localRoot == (common.Hash{}) {
		log.Warn("Slot has no canonical root", "slot", slot)
		tally.unserved++
		return nil
	}

	block, err := src.fullBlock(ctx, slot, beaconCfg)
	if err != nil {
		return fmt.Errorf("slot %d: %w", slot, err)
	}
	if block == nil {
		log.Warn("No endpoint served the block", "slot", slot)
		tally.unserved++
		return nil
	}
	remoteRoot, err := block.Block.HashSSZ()
	if err != nil {
		return err
	}
	if common.Hash(remoteRoot) != localRoot {
		log.Error("Endpoint block is not the local canonical block, refusing",
			"slot", slot, "local", localRoot, "remote", common.Hash(remoteRoot))
		tally.rootDiff++
		return nil
	}

	commitments := block.Block.Body.BlobKzgCommitments
	want := 0
	if commitments != nil {
		want = commitments.Len()
	}
	if want == 0 {
		tally.noBlobs++
		return nil
	}

	stored, err := store.KzgCommitmentsCount(ctx, localRoot)
	if err != nil {
		return err
	}
	if int(stored) == want {
		tally.alreadyOk++
		return nil
	}
	if stored > 0 && !c.Overwrite {
		log.Warn("Slot already holds sidecars, skipping", "slot", slot, "stored", stored, "want", want)
		tally.rejected++
		return nil
	}

	// Collect the whole set before touching the store: a partial write would rewrite the
	// count row to the subset size and orphan the files it does not cover.
	payloads := make(map[uint64][]byte, want)
	for i := range want {
		vh := versionedHashFor(common.Bytes48(*commitments.Get(i)))
		payload, found, err := src.blobPayload(ctx, vh)
		if err != nil {
			return fmt.Errorf("slot %d index %d (%s): %w", slot, i, vh.Hex(), err)
		}
		if !found {
			log.Warn("Archive does not hold this blob", "slot", slot, "index", i, "versionedHash", vh)
			tally.unserved++
			return nil
		}
		payloads[uint64(i)] = payload
	}

	sidecars, err := buildVerifiedSidecars(block, payloads)
	if err != nil {
		log.Error("Assembled sidecars failed verification, refusing", "slot", slot, "err", err)
		tally.rejected++
		return nil
	}

	if !c.Commit {
		log.Info("Would fill slot from archive", "slot", slot, "blockRoot", localRoot, "sidecars", len(sidecars))
		tally.wouldFill++
		return nil
	}

	if err := store.WriteBlobSidecars(ctx, localRoot, sidecars); err != nil {
		return fmt.Errorf("slot %d: write: %w", slot, err)
	}
	readBack, found, err := store.ReadBlobSidecars(ctx, slot, localRoot)
	if err != nil {
		return err
	}
	if !found || len(readBack) != want {
		log.Error("Read-back after insert does not match", "slot", slot, "found", found, "got", len(readBack), "want", want)
		tally.rejected++
		return nil
	}
	count, err := store.KzgCommitmentsCount(ctx, localRoot)
	if err != nil {
		return err
	}
	if int(count) != want {
		log.Error("Count row after insert does not match", "slot", slot, "count", count, "want", want)
		tally.rejected++
		return nil
	}
	log.Info("Filled slot from archive", "slot", slot, "blockRoot", localRoot, "sidecars", want)
	tally.filled++
	return nil
}
