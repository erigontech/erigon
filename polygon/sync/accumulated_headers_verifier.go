package sync

import (
	"bytes"
	"fmt"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/polygon/bor"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
)

type AccumulatedHeadersVerifier func(hashAccumulator heimdall.Waypoint, headers []*types.Header) error

func VerifyAccumulatedHeaders(accumulator heimdall.Waypoint, headers []*types.Header) error {
	rootHash, err := bor.ComputeHeadersRootHash(headers)
	if err != nil {
		return fmt.Errorf("VerifyStatePointHeaders: failed to compute headers root hash %w", err)
	}
	if !bytes.Equal(rootHash, accumulator.RootHash().Bytes()) {
		return fmt.Errorf("VerifyStatePointHeaders: bad headers root hash")
	}
	return nil
}
