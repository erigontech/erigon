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

	"github.com/erigontech/erigon/erigon-lib/common"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/polygon/bor"
	"github.com/erigontech/erigon/polygon/heimdall"
)

var (
	ErrFailedToComputeHeadersRootHash = errors.New("failed to compute headers root hash")
	ErrBadHeadersRootHash             = errors.New("bad headers root hash")
	ErrIncorrectHeadersLength         = errors.New("incorrect headers length")
	ErrDisconnectedHeaders            = errors.New("disconnected headers")
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
	if uint64(len(headers)) != waypoint.Length() || len(headers) == 0 {
		return fmt.Errorf(
			"VerifyMilestoneHeaders: %w: headers=%d, waypoint=%d",
			ErrIncorrectHeadersLength, len(headers), waypoint.Length(),
		)
	}

	prevHeader := headers[0]
	for _, header := range headers[1:] {
		prevNum, prevHash := prevHeader.Number.Uint64(), prevHeader.Hash()
		num, hash, parentHash := header.Number.Uint64(), header.Hash(), header.ParentHash
		if num != prevNum+1 || parentHash != prevHash {
			return fmt.Errorf(
				"VerifyMilestoneHeaders: %w: prevNum=%d, prevHash=%s, num=%d, parentHash=%s, hash=%s",
				ErrDisconnectedHeaders, prevNum, prevHash, num, parentHash, hash,
			)
		}

		prevHeader = header
	}

	var hash common.Hash
	if len(headers) > 0 {
		hash = headers[len(headers)-1].Hash()
	}

	if hash != waypoint.RootHash() {
		return fmt.Errorf("VerifyMilestoneHeaders: %w", ErrBadHeadersRootHash)
	}

	return nil
}
