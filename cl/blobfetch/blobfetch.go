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

// Package blobfetch fetches the blobs of a block from a beacon API endpoint and stores them only after
// they are verified against the block.
package blobfetch

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	bls12381 "github.com/consensys/gnark-crypto/ecc/bls12-381"
	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/hashicorp/go-retryablehttp"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

// MaxSidecarJSONSize bounds one sidecar in a JSON response: the hex blob is 256 KiB.
const MaxSidecarJSONSize = 512 << 10

// RequestTimeout bounds one attempt at an endpoint request, the body read included. A full response, 21 blobs of hex JSON, is about 6 MB. Variables so
// tests can shorten them.
var (
	RequestTimeout = time.Minute
	RetryWait      = time.Second
)

// ErrNoSidecars reports an endpoint that answers without the sidecars of a block that has blobs.
var ErrNoSidecars = errors.New("the endpoint has no sidecars for the block")

type StatusError int

func (e StatusError) Error() string {
	return fmt.Sprintf("blob retrieval failed, bad status code %d", int(e))
}

// NotServed reports an error that means the endpoint does not have the block's blobs, as opposed to a failure.
func NotServed(err error) bool {
	status, ok := errors.AsType[StatusError](err)
	return errors.Is(err, ErrNoSidecars) || ok && status == http.StatusBadRequest
}

// BlobCount returns the number of blob commitments of block.
func BlobCount(block *cltypes.SignedBeaconBlock) int {
	if block == nil || block.Version() < clparams.DenebVersion {
		return 0
	}
	if commitments := block.Block.Body.GetBlobKzgCommitments(); commitments != nil {
		return commitments.Len()
	}
	return 0
}

type blockBodyReader interface {
	ReadBeaconBlockBodyBySlot(ctx context.Context, tx kv.Tx, slot uint64) (*cltypes.SignedBeaconBlock, error)
}

// ExpectedBlobs returns how many blobs the canonical block root at its slot has on the network. A Gloas block's
// blobs come with its payload, so it has none when the canonical child skipped that payload. unresolved reports a
// Gloas block whose canonical child is not indexed yet; its commitment count is returned then.
func ExpectedBlobs(ctx context.Context, tx kv.Tx, blocks blockBodyReader, root common.Hash, block *cltypes.SignedBeaconBlock) (n int, unresolved bool, err error) {
	n = BlobCount(block)
	if n == 0 || block.Version() < clparams.GloasVersion {
		return n, false, nil
	}
	var childSlot uint64
	var childRoot common.Hash
	if err := beacon_indicies.RangeBlockRoots(tx, block.Block.Slot+1, math.MaxUint64, func(slot uint64, root common.Hash) bool {
		childSlot, childRoot = slot, root
		return false
	}); err != nil {
		return 0, false, err
	}
	if childRoot == (common.Hash{}) {
		return n, true, nil
	}
	child, err := blocks.ReadBeaconBlockBodyBySlot(ctx, tx, childSlot)
	if err != nil {
		return 0, false, err
	}
	if child == nil || child.Block.ParentRoot != root {
		return n, true, nil
	}
	if gotRoot, err := child.Block.HashSSZ(); err != nil {
		return 0, false, err
	} else if gotRoot != childRoot {
		return n, true, nil
	}
	if forkchoice.ParentPayloadStatusFromBids(block, child.Block) == cltypes.PayloadStatusFull {
		return n, false, nil
	}
	return 0, false, nil
}

// RetrieveBlobSidecars fetches the blob sidecars of root from a beacon API base URL, which may carry a query such as
// an API key. A response with more than maxSidecars entries is refused before they are decoded: an empty entry is a
// few bytes on the wire but a whole blob in memory.
func RetrieveBlobSidecars(ctx context.Context, endpoint string, root common.Hash, maxSidecars int) ([]*cltypes.BlobSidecar, error) {
	data, err := getBeaconData(ctx, endpoint, fmt.Sprintf("eth/v1/beacon/blob_sidecars/0x%x", root), maxSidecars)
	if err != nil {
		return nil, err
	}
	sidecars := make([]*cltypes.BlobSidecar, len(data))
	for i, raw := range data {
		sidecars[i] = &cltypes.BlobSidecar{}
		if err := sidecars[i].UnmarshalJSON(raw); err != nil {
			return nil, fmt.Errorf("blob retrieval decode failed %w", err)
		}
	}
	return sidecars, nil
}

// Retrieve fetches the sidecars of the block at root: blob_sidecars before Gloas, and from Gloas on the blobs, built
// into sidecars with an empty proof that Store recomputes.
func Retrieve(ctx context.Context, endpoint string, root common.Hash, block *cltypes.SignedBeaconBlock) ([]*cltypes.BlobSidecar, error) {
	if block.Version() < clparams.GloasVersion {
		return RetrieveBlobSidecars(ctx, endpoint, root, BlobCount(block))
	}
	commitments := block.Block.Body.GetBlobKzgCommitments()
	data, err := getBeaconData(ctx, endpoint, fmt.Sprintf("eth/v1/beacon/blobs/0x%x", root), BlobCount(block))
	if err != nil {
		return nil, err
	}
	header := block.SignedBeaconBlockHeader()
	sidecars := make([]*cltypes.BlobSidecar, len(data))
	for i, raw := range data {
		var blob cltypes.Blob
		if err := blob.UnmarshalJSON(raw); err != nil {
			return nil, fmt.Errorf("blob retrieval decode failed %w", err)
		}
		sidecars[i] = cltypes.NewBlobSidecar(uint64(i), &blob, common.Bytes48(*commitments.Get(i)), common.Bytes48(goethkzg.PointAtInfinity), header,
			solid.NewHashVector(cltypes.CommitmentBranchSize))
	}
	return sidecars, nil
}

var transport = http.DefaultTransport.(*http.Transport).Clone()

func retryBackoff(lo, hi time.Duration, attempt int, resp *http.Response) time.Duration {
	if resp != nil && (resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable) {
		if seconds, err := strconv.ParseUint(resp.Header.Get("Retry-After"), 10, 64); err == nil {
			return time.Duration(min(seconds, uint64(hi/time.Second))) * time.Second
		}
	}
	return min(retryablehttp.DefaultBackoff(lo, hi, attempt, resp), hi)
}

func getBeaconData(ctx context.Context, endpoint, path string, maxEntries int) ([]json.RawMessage, error) {
	base, err := url.Parse(endpoint)
	if err != nil {
		return nil, RedactURLError(err)
	}
	log.Debug("[Blob Retrieval] Requesting", "endpoint", RedactURL(base), "path", path)

	req, err := retryablehttp.NewRequestWithContext(ctx, http.MethodGet, base.JoinPath(path).String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")

	client := &retryablehttp.Client{
		HTTPClient:   &http.Client{Timeout: RequestTimeout, Transport: transport},
		RetryWaitMin: RetryWait,
		RetryWaitMax: 30 * time.Second,
		RetryMax:     2,
		CheckRetry:   retryablehttp.DefaultRetryPolicy,
		Backoff:      retryBackoff,
		ErrorHandler: retryablehttp.PassthroughErrorHandler,
	}
	var raw []byte
	req.SetResponseHandler(func(r *http.Response) (err error) {
		if r.StatusCode == http.StatusOK {
			raw, err = io.ReadAll(io.LimitReader(r.Body, int64(maxEntries+1)*MaxSidecarJSONSize))
		}
		return err
	})
	r, err := client.Do(req)
	if r != nil {
		defer r.Body.Close()
	}
	if err != nil {
		return nil, RedactURLError(err)
	}

	if r.StatusCode == http.StatusNotFound {
		// No blobs for this block (pre-Deneb or no blob transactions)
		return nil, nil
	}
	if r.StatusCode != http.StatusOK {
		return nil, StatusError(r.StatusCode)
	}

	var response struct {
		Data []json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return nil, fmt.Errorf("blob retrieval decode failed %w", err)
	}
	if len(response.Data) > maxEntries {
		return nil, fmt.Errorf("blob response has %d entries, the block has %d commitments", len(response.Data), maxEntries)
	}
	return response.Data, nil
}

// FetchAndStore fetches the sidecars of the block at the canonical root from endpoint, verifies them and stores them.
// With a non-nil cpu, the missing proofs of the block are computed in parallel, one budget slot each; with nil they
// are computed one after another on the caller's goroutine.
func FetchAndStore(ctx context.Context, store blob_storage.BlobStorage, endpoint string, root common.Hash, block *cltypes.SignedBeaconBlock, cpu *semaphore.Weighted) error {
	commitments := block.Block.Body.GetBlobKzgCommitments()
	sidecars, err := Retrieve(ctx, endpoint, root, block)
	if err != nil {
		return err
	}
	if len(sidecars) == 0 {
		return ErrNoSidecars
	}
	slices.SortFunc(sidecars, func(a, b *cltypes.BlobSidecar) int { return cmp.Compare(a.Index, b.Index) })
	// Identifiers carry the canonical root: the block read from storage keeps no transactions,
	// so its own hash is not that root.
	ids := solid.NewStaticListSSZ[*cltypes.BlobIdentifier](commitments.Len(), 40)
	for i := range commitments.Len() {
		ids.Append(&cltypes.BlobIdentifier{BlockRoot: root, Index: uint64(i)})
	}
	return store_(ctx, store, ids, sidecars, block, cpu)
}

// Store verifies a remote blob response against the requested identifiers, stores what
// matches, and requires the store to serve every requested identity back afterwards. Given the
// insert's early-stop contract, its error alone cannot tell a full insert from an empty one.
//
// Each response sidecar is bound to the block by commitment first. The insert skips the commitment
// inclusion proof from Gloas on, so without that comparison nothing ties a sidecar to the block it
// claims to belong to and a self-consistent blob, commitment and proof from elsewhere would verify.
//
// Completeness is judged by reading the store rather than by this invocation's insert count: blob
// storage commits independently of the caller's beacon transaction, so a re-run can legitimately
// insert nothing and still be complete. The read has to be ReadBlobSidecars, the same call
// consumers make, because sidecar files are published before the count row they depend on is
// committed — checking for the files alone would accept a store nothing can read.
func Store(
	ctx context.Context,
	blobDB blob_storage.BlobStorage,
	ids *solid.ListSSZ[*cltypes.BlobIdentifier],
	blobs []*cltypes.BlobSidecar,
	block *cltypes.SignedBeaconBlock,
) error {
	return store_(ctx, blobDB, ids, blobs, block, nil)
}

func store_(
	ctx context.Context,
	blobDB blob_storage.BlobStorage,
	ids *solid.ListSSZ[*cltypes.BlobIdentifier],
	blobs []*cltypes.BlobSidecar,
	block *cltypes.SignedBeaconBlock,
	cpu *semaphore.Weighted,
) error {
	commitments := block.Block.Body.GetBlobKzgCommitments()
	for _, sidecar := range blobs {
		if sidecar == nil {
			return errors.New("blob response contains a nil sidecar")
		}
		if commitments == nil || sidecar.Index >= uint64(commitments.Len()) {
			return fmt.Errorf("blob sidecar index %d is outside the block's commitments", sidecar.Index)
		}
		if common.Bytes48(*commitments.Get(int(sidecar.Index))) != sidecar.KzgCommitment {
			return fmt.Errorf("blob sidecar at index %d carries a commitment the block does not reference", sidecar.Index)
		}
	}
	if err := computeMissingProofs(ctx, blobs, cpu); err != nil {
		return err
	}
	_, inserted, err := blob_storage.VerifyAgainstIdentifiersAndInsertIntoTheBlobStore(ctx, blobDB, ids, blobs,
		block.Version(),
		func(header *cltypes.SignedBeaconBlockHeader) error {
			if header.Signature == block.Signature {
				return nil
			}
			return errors.New("mismatched block header in blob sidecar")
		})
	if err != nil {
		return err
	}
	wanted := map[common.Hash]uint64{}
	for i := 0; i < ids.Len(); i++ {
		wanted[ids.Get(i).BlockRoot]++
	}
	for root, count := range wanted {
		stored, found, err := blobDB.ReadBlobSidecars(ctx, block.Block.Slot, root)
		if err != nil {
			return err
		}
		if !found || uint64(len(stored)) < count {
			return fmt.Errorf("blob sidecars for root %x not readable after insert: got %d of %d (inserted %d, responses: %d)",
				root, len(stored), count, inserted, len(blobs))
		}
	}
	return nil
}

// proofMissing reports a proof an endpoint left as a placeholder: the point at infinity, or bytes that are not a G1
// point, such as all zeros. A computed proof is verified against the block's commitment like a served one.
func proofMissing(proof common.Bytes48) bool {
	if proof == common.Bytes48(goethkzg.PointAtInfinity) {
		return true
	}
	var point bls12381.G1Affine
	_, err := point.SetBytes(proof[:])
	return err != nil
}

// computeMissingProofs fills the placeholder KZG proofs of blobs. Each proof runs on one goroutine: spreading one proof over
// every CPU costs more CPU time in total, so parallelism comes only from the cpu budget, one slot per proof.
func computeMissingProofs(ctx context.Context, blobs []*cltypes.BlobSidecar, cpu *semaphore.Weighted) error {
	compute := func(sidecar *cltypes.BlobSidecar) error {
		proof, err := kzg.Ctx().ComputeBlobKZGProof((*goethkzg.Blob)(&sidecar.Blob), goethkzg.KZGCommitment(sidecar.KzgCommitment), 1)
		if err != nil {
			return fmt.Errorf("blob sidecar at index %d: compute its KZG proof: %w", sidecar.Index, err)
		}
		sidecar.KzgProof = common.Bytes48(proof)
		return nil
	}
	var g errgroup.Group
	for _, sidecar := range blobs {
		if !proofMissing(sidecar.KzgProof) {
			continue
		}
		if cpu == nil {
			if err := compute(sidecar); err != nil {
				return err
			}
			continue
		}
		if err := cpu.Acquire(ctx, 1); err != nil {
			_ = g.Wait()
			return err
		}
		g.Go(func() error {
			defer cpu.Release(1)
			return compute(sidecar)
		})
	}
	return g.Wait()
}

// RedactURL keeps only the scheme and host of u for logs and errors: beacon API providers put the API key in the
// user info, the path or the query.
func RedactURL(u *url.URL) string {
	return (&url.URL{Scheme: u.Scheme, Host: u.Host}).String()
}

// RedactURLError redacts the request URL that a *url.Error from net/http carries.
func RedactURLError(err error) error {
	ue, ok := errors.AsType[*url.Error](err)
	if !ok {
		return err
	}
	if ue.Op == "parse" {
		// The parse error quotes the malformed part of the URL, which can be the key.
		return errors.New("malformed endpoint URL")
	}
	ue.URL = RedactEndpoint(ue.URL)
	if strings.HasPrefix(ue.Err.Error(), "failed to parse Location header") {
		// net/http quotes the header, and a server can repeat the request's path and query in it.
		ue.Err = errors.New("malformed redirect Location header")
	}
	return err
}

// ValidEndpoint reports whether endpoint is an http or https URL with a host. Callers do not echo a rejected value:
// it can carry an API key.
func ValidEndpoint(endpoint string) bool {
	u, err := url.Parse(endpoint)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Hostname() != ""
}

// RedactEndpoint is RedactURL for an endpoint string.
func RedactEndpoint(endpoint string) string {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "<invalid endpoint>"
	}
	return RedactURL(u)
}
