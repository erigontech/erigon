package downloader

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/trie"
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

	blockNr := syncHeadBlock.Header().Number.Uint64()

	tr := trie.New(syncHeadBlock.Root())

	// FIXME: miner of the first block on Ethereum mainnet
	addr := common.HexToAddress("0x05a56e2d52c817161883f50c441c3228cfe54d9f")

	addrHash, _ := common.HashData(addr[:])

	r := trie.NewResolver(0, true, blockNr)
	need, rr := tr.NeedResolution(nil, addrHash[:])
	if !need {
		panic("should need :-D")
	}
	r.AddRequest(rr)
	err = r.ResolveStateful(euphemeralMutation, blockNr, true)
	if err != nil {
		return err
	}

	return nil
}
