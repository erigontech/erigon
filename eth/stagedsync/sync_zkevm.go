package stagedsync

import (
	"fmt"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/zk/hermez_db"
)

// UnwindToBatch is used to unwind all stages to the highest block of the batch passed in
func (s *Sync) UnwindToBatch(unwindPoint uint64, tx kv.RwTx) error {

	// calculate block to unwind to (the node will be synced up to and including this block after the unwind completes)
	hdb := hermez_db.NewHermezDbReader(tx)
	unwindPointBlock, found, err := hdb.GetHighestBlockInBatch(unwindPoint)
	if err != nil {
		return err
	}

	if !found {
		return fmt.Errorf("no block found at batch %d", unwindPoint)
	}

	log.Info("UnwindToBatch", "batchNo", unwindPoint, "blockNo", unwindPointBlock)
	s.unwindPoint = &unwindPointBlock
	return nil
}
