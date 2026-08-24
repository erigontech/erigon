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

package network

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

const remoteBlobFetchTimeout = 30 * time.Second

// remoteBlobSource fetches blob sidecars over the beacon API. Erigon serves sidecars only
// from what it already holds and never rebuilds them from custodied columns, so a node
// whose own reconstruction was missed cannot recover those blobs from the p2p network once
// they leave the serving window. Sidecars carry their own kzg proofs, so a source only has
// to be reachable, not trusted - the caller verifies before inserting.
type remoteBlobSource struct {
	endpoints []string
	client    *http.Client
	logger    log.Logger
}

func newRemoteBlobSource(endpoints []string, logger log.Logger) *remoteBlobSource {
	cleaned := make([]string, 0, len(endpoints))
	for _, e := range endpoints {
		if e = strings.TrimRight(strings.TrimSpace(e), "/"); e != "" {
			cleaned = append(cleaned, e)
		}
	}
	return &remoteBlobSource{
		endpoints: cleaned,
		client:    &http.Client{Timeout: remoteBlobFetchTimeout},
		logger:    logger,
	}
}

func (s *remoteBlobSource) enabled() bool { return s != nil && len(s.endpoints) > 0 }

// fetch returns the first non-empty sidecar set any endpoint serves for blockRoot. An
// endpoint that 404s, errors, or answers with an empty set is treated the same way: it
// cannot supply this block, so the next one is tried.
func (s *remoteBlobSource) fetch(ctx context.Context, blockRoot common.Hash) ([]*cltypes.BlobSidecar, error) {
	if !s.enabled() {
		return nil, nil
	}
	for _, endpoint := range s.endpoints {
		sidecars, err := s.fetchFrom(ctx, endpoint, blockRoot)
		if err != nil {
			s.logger.Debug("[BlobRepair] endpoint could not serve block", "endpoint", endpoint, "blockRoot", blockRoot, "err", err)
			continue
		}
		if len(sidecars) > 0 {
			return sidecars, nil
		}
	}
	return nil, nil
}

func (s *remoteBlobSource) fetchFrom(ctx context.Context, endpoint string, blockRoot common.Hash) ([]*cltypes.BlobSidecar, error) {
	url := fmt.Sprintf("%s/eth/v1/beacon/blob_sidecars/0x%x", endpoint, blockRoot)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bad status %d", resp.StatusCode)
	}

	var body struct {
		Data []*cltypes.BlobSidecar `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return body.Data, nil
}
