package downloader

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/core/state"
)

func (d *Downloader) spawnCheckFinalHashStage() error {
	// TODO: fix this
	syncHeadNumber, err := GetStageProgress(d.stateDB, Execution)
	if err != nil {
		return err
	}
	fmt.Printf("getting head as %v\n", syncHeadNumber)
	syncHeadBlock := d.blockchain.GetBlockByNumber(syncHeadNumber)

	fmt.Printf("getting head block as %v\n", syncHeadBlock)

	// make sure that we won't write the the real DB
	// should never be commited
	euphemeralMutation := d.stateDB.NewBatch()

	tds := state.NewTrieDbState(syncHeadBlock.Header().Hash(), euphemeralMutation, syncHeadBlock.Header().Number.Uint64())

	_, err = tds.ResolveStateTrie(false, false)
	fmt.Printf("resolving state trie err=%v\n", err)
	if err != nil {
		return err
	}

	return nil
}
