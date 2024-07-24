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

package sync

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/polygon/bor"
	"github.com/erigontech/erigon/polygon/heimdall"
)

var (
	ErrFailedToComputeHeadersRootHash = errors.New("failed to compute headers root hash")
	ErrBadHeadersRootHash             = errors.New("bad headers root hash")
)

type WaypointHeadersVerifier func(waypoint heimdall.Waypoint, headers []*types.Header) error

func VerifyCheckpointHeaders(waypoint heimdall.Waypoint, headers []*types.Header) error {
	rootHash, err := bor.ComputeHeadersRootHash(headers)
	if err != nil {
		return fmt.Errorf("VerifyCheckpointHeaders: %w: %w", ErrFailedToComputeHeadersRootHash, err)
	}
	if !bytes.Equal(rootHash, waypoint.RootHash().Bytes()) {
		return fmt.Errorf("VerifyCheckpointHeaders: %w", ErrBadHeadersRootHash)
	}
	return nil
}

func VerifyMilestoneHeaders(waypoint heimdall.Waypoint, headers []*types.Header) error {
	var hash common.Hash
	if len(headers) > 0 {
		hash = headers[len(headers)-1].Hash()
	}
	if hash != waypoint.RootHash() {
		return fmt.Errorf("VerifyMilestoneHeaders: %w", ErrBadHeadersRootHash)
	}
	return nil
}
