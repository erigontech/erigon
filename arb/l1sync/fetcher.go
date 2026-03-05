package l1sync

import (
	"context"
	"fmt"
	"math/big"

	"github.com/erigontech/nitro-erigon/arbnode"
)

// FetchBatchesInRange fetches all sequencer batches posted in the given L1 block range
func (s *L1SyncService) FetchBatchesInRange(ctx context.Context, fromL1Block, toL1Block uint64) ([]*arbnode.SequencerInboxBatch, error) {
	from := new(big.Int).SetUint64(fromL1Block)
	to := new(big.Int).SetUint64(toL1Block)

	batches, err := s.sequencerInbox.LookupBatchesInRange(ctx, from, to)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup batches in L1 range [%d, %d]: %w", fromL1Block, toL1Block, err)
	}
	return batches, nil
}

// FetchAndSerializeBatch serializes a single batch's data (header + payload from L1)
func (s *L1SyncService) FetchAndSerializeBatch(ctx context.Context, batch *arbnode.SequencerInboxBatch) ([]byte, error) {
	data, err := batch.Serialize(ctx, s.l1Client)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize batch %d: %w", batch.SequenceNumber, err)
	}
	return data, nil
}

// GetL1BatchCount returns the current batch count on L1 at the given block number
func (s *L1SyncService) GetL1BatchCount(ctx context.Context, l1BlockNumber uint64) (uint64, error) {
	blockNum := new(big.Int).SetUint64(l1BlockNumber)
	return s.sequencerInbox.GetBatchCount(ctx, blockNum)
}
