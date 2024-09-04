package stagedsync

import (
	"fmt"

	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/log/v3"
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
