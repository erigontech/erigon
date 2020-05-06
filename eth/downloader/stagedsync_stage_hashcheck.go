package downloader

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/trie"
	"github.com/pkg/errors"
)

func (d *Downloader) spawnCheckFinalHashStage(syncHeadNumber uint64) error {
	hashProgress, err := GetStageProgress(d.stateDB, HashCheck)
	if err != nil {
		return err
	}

	if hashProgress == syncHeadNumber {
		// we already did hash check for this block
		// we don't do the obvious `if hashProgress > syncHeadNumber` to support reorgs more naturally
		return nil
	}

	syncHeadBlock := d.blockchain.GetBlockByNumber(syncHeadNumber)

	// make sure that we won't write the the real DB
	// should never be commited
	euphemeralMutation := d.stateDB.NewBatch()

	blockNr := syncHeadBlock.Header().Number.Uint64()

	tr := trie.New(syncHeadBlock.Root())
	// making resolve request for the trie root, so we only get a hash
	rr := tr.NewResolveRequest(nil, []byte{}, 0, tr.Root())

	log.Info("Validating root hash", "block", blockNr, "blockRoot", syncHeadBlock.Root().Hex())

	resolver := trie.NewResolver(blockNr)
	resolver.AddRequest(rr)
	err = resolver.ResolveStateful(euphemeralMutation, blockNr, false)
	if err != nil {
		return errors.Wrap(err, "checking root hash failed")
	}

	return SaveStageProgress(d.stateDB, HashCheck, blockNr)
}

func (d *Downloader) unwindHashCheckStage(unwindPoint uint64) error {
	// Currently it does not require unwinding because it does not create any Intemediate Hash records
	// and recomputes the state root from scratch
	lastProcessedBlockNumber, err := GetStageProgress(d.stateDB, HashCheck)
	if err != nil {
		return fmt.Errorf("unwind HashCheck: get stage progress: %v", err)
	}
	unwindPoint, err1 := GetStageUnwind(d.stateDB, HashCheck)
	if err1 != nil {
		return err1
	}
	if unwindPoint >= lastProcessedBlockNumber {
		err = SaveStageUnwind(d.stateDB, HashCheck, 0)
		if err != nil {
			return fmt.Errorf("unwind HashCheck: reset: %v", err)
		}
		return nil
	}
	mutation := d.stateDB.NewBatch()
	err = SaveStageUnwind(mutation, HashCheck, 0)
	if err != nil {
		return fmt.Errorf("unwind HashCheck: reset: %v", err)
	}
	_, err = mutation.Commit()
	if err != nil {
		return fmt.Errorf("unwind HashCheck: failed to write db commit: %v", err)
	}
	return nil
}
