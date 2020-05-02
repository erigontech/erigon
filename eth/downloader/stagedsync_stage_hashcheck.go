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

	resolver := trie.NewResolver(0, true, blockNr)
	resolver.AddRequest(rr)
	err = resolver.ResolveStateful(euphemeralMutation, blockNr, false)
	if err != nil {
		return errors.Wrap(err, "checking root hash failed")
	}

	return SaveStageProgress(d.stateDB, HashCheck, blockNr)
}

func (d *Downloader) unwindHashCheckStage(unwindPoint uint64) error {
	return fmt.Errorf("unwindHashCheckStage not implemented")
}
