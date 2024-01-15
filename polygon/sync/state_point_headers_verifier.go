package sync

import (
	"bytes"
	"fmt"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/polygon/bor"
)

type StatePointHeadersVerifier func(statePoint *statePoint, headers []*types.Header) error

func VerifyStatePointHeaders(statePoint *statePoint, headers []*types.Header) error {
	rootHash, err := bor.ComputeHeadersRootHash(headers)
	if err != nil {
		return fmt.Errorf("VerifyStatePointHeaders: failed to compute headers root hash %w", err)
	}
	if !bytes.Equal(rootHash, statePoint.rootHash[:]) {
		return fmt.Errorf("VerifyStatePointHeaders: bad headers root hash")
	}
	return nil
}
