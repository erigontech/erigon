package downloader

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/trie"
	"github.com/pkg/errors"
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

	resolver := trie.NewResolver(0, true, blockNr)

	rr := createResolveRequest(tr)

	resolver.AddRequest(rr)
	err = resolver.ResolveStateful(euphemeralMutation, blockNr, false)
	if err != nil {
		return errors.Wrap(err, "checking root hash failed")
	}

	return nil
}

func createResolveRequest(tr *trie.Trie) *trie.ResolveRequest {
	// FIXME: miner of the first block on Ethereum mainnet
	addr := common.HexToAddress("0x05a56e2d52c817161883f50c441c3228cfe54d9f")
	addrHash, err := common.HashData(addr[:])
	if err != nil {
		panic(err)
	}
	need, rr := tr.NeedResolution(nil, addrHash[:])
	if !need {
		panic("should need :-D")
	}
	//tr.NewResolveRequest(nil, []byte{0x00, 0x00}, 0, tr.Root())
	return rr
}
