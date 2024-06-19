package sync

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/polygon/bor"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
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
