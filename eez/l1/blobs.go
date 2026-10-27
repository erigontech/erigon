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

package l1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"

	goethkzg "github.com/crate-crypto/go-eth-kzg"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/common/hexutil"
)

// BlobSize is the size of an EIP-4844 blob.
const BlobSize = goethkzg.ScalarsPerBlob * goethkzg.SerializedScalarSize

// ErrBlobUnavailable reports a blob the source does not serve, which
// derivation cannot proceed without.
var ErrBlobUnavailable = errors.New("blob not served")

// BlobSource serves the blobs of L1 transactions.
type BlobSource interface {
	// Blobs returns the blobs with versioned hashes hashes, in order, that
	// the L1 block with header h carries.
	Blobs(ctx context.Context, h *Header, hashes []common.Hash) ([][]byte, error)
}

// VersionedHash returns the EIP-4844 versioned hash of blob, from its
// recomputed KZG commitment.
func VersionedHash(blob []byte) (common.Hash, error) {
	if len(blob) != BlobSize {
		return common.Hash{}, fmt.Errorf("%d-byte blob", len(blob))
	}
	commitment, err := kzg.Ctx().BlobToKZGCommitment((*goethkzg.Blob)(blob), 1)
	if err != nil {
		return common.Hash{}, err
	}
	return common.Hash(kzg.KZGToVersionedHash(commitment)), nil
}

// MatchBlobs returns the blobs of served whose content hashes to hashes, in
// the order of hashes. Nothing but the content identifies a blob.
func MatchBlobs(served [][]byte, hashes []common.Hash) ([][]byte, error) {
	byHash := make(map[common.Hash][]byte, len(served))
	for i, blob := range served {
		h, err := VersionedHash(blob)
		if err != nil {
			return nil, fmt.Errorf("%w: served blob %d: %w", ErrInconsistent, i, err)
		}
		byHash[h] = blob
	}
	out := make([][]byte, len(hashes))
	for i, h := range hashes {
		blob, ok := byHash[h]
		if !ok {
			return nil, fmt.Errorf("%w: %x", ErrBlobUnavailable, h)
		}
		out[i] = blob
	}
	return out, nil
}

// Beacon is the BlobSource of a beacon node's REST API.
type Beacon struct {
	url    string
	client *http.Client

	mu sync.Mutex
	// genesis and slot are the beacon chain's genesis time and seconds per
	// slot, zero until read.
	genesis, slot uint64
}

var _ BlobSource = (*Beacon)(nil)

// NewBeacon returns the BlobSource of the beacon node at url.
func NewBeacon(url string) *Beacon {
	return &Beacon{url: strings.TrimSuffix(url, "/"), client: http.DefaultClient}
}

// Blobs reads the blobs of h's slot, from /eth/v1/beacon/blobs or, on a node
// without it, /eth/v1/beacon/blob_sidecars.
func (b *Beacon) Blobs(ctx context.Context, h *Header, hashes []common.Hash) ([][]byte, error) {
	slot, err := b.slotOf(ctx, h.Time)
	if err != nil {
		return nil, err
	}
	var blobs struct {
		Data []hexutil.Bytes `json:"data"`
	}
	found, err := b.get(ctx, "/eth/v1/beacon/blobs/"+strconv.FormatUint(slot, 10), &blobs)
	if err != nil {
		return nil, err
	}
	served := make([][]byte, 0, len(blobs.Data))
	if found {
		for _, blob := range blobs.Data {
			served = append(served, blob)
		}
	} else {
		var sidecars struct {
			Data []struct {
				Blob hexutil.Bytes `json:"blob"`
			} `json:"data"`
		}
		found, err = b.get(ctx, "/eth/v1/beacon/blob_sidecars/"+strconv.FormatUint(slot, 10), &sidecars)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("%w: slot %d of L1 block %d", ErrBlobUnavailable, slot, h.Number)
		}
		for _, s := range sidecars.Data {
			served = append(served, s.Blob)
		}
	}
	out, err := MatchBlobs(served, hashes)
	if err != nil {
		return nil, fmt.Errorf("slot %d of L1 block %d: %w", slot, h.Number, err)
	}
	return out, nil
}

// slotOf returns the slot of an execution block with timestamp t.
func (b *Beacon) slotOf(ctx context.Context, t uint64) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.slot == 0 {
		var genesis struct {
			Data struct {
				GenesisTime string `json:"genesis_time"`
			} `json:"data"`
		}
		var spec struct {
			Data struct {
				SecondsPerSlot string `json:"SECONDS_PER_SLOT"`
			} `json:"data"`
		}
		if err := b.mustGet(ctx, "/eth/v1/beacon/genesis", &genesis); err != nil {
			return 0, err
		}
		if err := b.mustGet(ctx, "/eth/v1/config/spec", &spec); err != nil {
			return 0, err
		}
		g, err := strconv.ParseUint(genesis.Data.GenesisTime, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("beacon genesis time %q: %w", genesis.Data.GenesisTime, err)
		}
		s, err := strconv.ParseUint(spec.Data.SecondsPerSlot, 10, 64)
		if err != nil || s == 0 {
			return 0, fmt.Errorf("beacon SECONDS_PER_SLOT %q: %w", spec.Data.SecondsPerSlot, err)
		}
		b.genesis, b.slot = g, s
	}
	if t < b.genesis || (t-b.genesis)%b.slot != 0 {
		return 0, fmt.Errorf("%w: L1 timestamp %d is off the beacon slots from %d every %d s", ErrInconsistent, t, b.genesis, b.slot)
	}
	return (t - b.genesis) / b.slot, nil
}

func (b *Beacon) mustGet(ctx context.Context, path string, v any) error {
	found, err := b.get(ctx, path, v)
	if err == nil && !found {
		err = fmt.Errorf("beacon GET %s: not found", path)
	}
	return err
}

// get decodes the JSON at path into v and reports whether the node serves
// it: 404 and 405 are not found.
func (b *Beacon) get(ctx context.Context, path string, v any) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.url+path, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := b.client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed:
		return false, nil
	case resp.StatusCode != http.StatusOK:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return false, fmt.Errorf("beacon GET %s: %s: %s", path, resp.Status, body)
	}
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		return false, fmt.Errorf("beacon GET %s: %w", path, err)
	}
	return true, nil
}
