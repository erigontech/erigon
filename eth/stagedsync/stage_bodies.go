package stagedsync

import (
	"context"
	"fmt"
	"math/big"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/debug"
	"github.com/ledgerwatch/turbo-geth/common/mclock"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/event"
	"github.com/ledgerwatch/turbo-geth/log"
)

func spawnBodyDownloadStage(s *StageState, u Unwinder, d DownloaderGlue, pid string) error {
	cont, err := d.SpawnBodyDownloadStage(pid, s, u)
	if err != nil {
		return err
	}
	if !cont {
		s.Done()
	}
	return nil

}

func unwindBodyDownloadStage(u *UnwindState, db ethdb.Database) error {
	mutation := db.NewBatch()
	if err := u.Done(db); err != nil {
		return fmt.Errorf("unwind Bodies: reset: %v", err)
	}
	if _, err := mutation.Commit(); err != nil {
		return fmt.Errorf("unwind Bodies: failed to write db commit: %v", err)
	}
	return nil
}

// InsertBodies is insertChain with execute=false and ommission of blockchain object
func InsertBodies(
	ctx context.Context,
	procInterrupt int32,
	chain types.Blocks,
	db ethdb.DbWithPendingMutations,
	currentBlock *types.Block,
	committedBlock *types.Block,
	trieDbState *state.TrieDbState,
	futureBlocks *lru.Cache,
	chainSideFeed *event.Feed,
	badBlocks *lru.Cache,
	verifySeals bool,
) (int, *types.Block, *types.Block, *state.TrieDbState, error) {
	// If the chain is terminating, don't even bother starting u
	if atomic.LoadInt32(&procInterrupt) == 1 {
		return 0, currentBlock, committedBlock, trieDbState, nil
	}

	// A queued approach to delivering events. This is generally
	// faster than direct delivery and requires much less mutex
	// acquiring.
	var (
		stats     = core.InsertStats{StartTime: mclock.Now()}
		currBlock = currentBlock
		commBlock = committedBlock
		trie      = trieDbState
	)
	// Start the parallel header verifier
	headers := make([]*types.Header, len(chain))
	seals := make([]bool, len(chain))

	for i, block := range chain {
		headers[i] = block.Header()
		seals[i] = verifySeals
	}

	externTd := big.NewInt(0)
	if len(chain) > 0 && chain[0].NumberU64() > 0 {
		d := rawdb.ReadTd(db, chain[0].ParentHash(), chain[0].NumberU64()-1)
		if d != nil {
			externTd = externTd.Set(d)
		}
	}

	localTd := rawdb.ReadTd(db, currBlock.Hash(), currBlock.NumberU64())

	var verifyFrom int
	for verifyFrom = 0; verifyFrom < len(chain) && localTd.Cmp(externTd) >= 0; verifyFrom++ {
		header := chain[verifyFrom].Header()
		externTd = externTd.Add(externTd, header.Difficulty)
	}

	var offset int
	var parent *types.Block
	var parentNumber = chain[0].NumberU64() - 1
	// Find correct insertion point for this chain
	preBlocks := []*types.Block{}
	parentHash := chain[0].ParentHash()
	parent = rawdb.ReadBlock(db, parentHash, parentNumber)
	if parent == nil {
		log.Error("chain segment could not be inserted, missing parent", "hash", parentHash)
		return 0, nil, nil, nil, fmt.Errorf("chain segment could not be inserted, missing parent %x", parentHash)
	}

	canonicalHash := rawdb.ReadCanonicalHash(db, parentNumber)
	for canonicalHash != parentHash {
		log.Warn("Chain segment's parent not on canonical hash, adding to pre-blocks", "block", parentNumber, "hash", parentHash)
		preBlocks = append(preBlocks, parent)
		parentNumber--
		parentHash = parent.ParentHash()
		parent = rawdb.ReadBlock(db, parentHash, parentNumber)
		if parent == nil {
			log.Error("chain segment could not be inserted, missing parent", "hash", parentHash)
			return 0, currBlock, commBlock, trie, fmt.Errorf("chain segment could not be inserted, missing parent %x", parentHash)
		}
		canonicalHash = rawdb.ReadCanonicalHash(db, parentNumber)
	}

	for left, right := 0, len(preBlocks)-1; left < right; left, right = left+1, right-1 {
		preBlocks[left], preBlocks[right] = preBlocks[right], preBlocks[left]
	}

	offset = len(preBlocks)
	if offset > 0 {
		chain = append(preBlocks, chain...)
	}

	var k int
	var committedK int
	// Iterate over the blocks and insert when the verifier permits
	for i, block := range chain {
		start := time.Now()
		if i >= offset {
			k = i - offset
		} else {
			k = 0
		}

		// If the chain is terminating, stop processing blocks
		if atomic.LoadInt32(&procInterrupt) == 1 {
			break
		}

		// If the header is a banned one, straight out abort
		if core.BadHashes[block.Hash()] {
			badBlocks.Add(block.Hash(), block)
			log.Error(fmt.Sprintf(`
########## BAD BLOCK #########

Number: %v
Hash: 0x%x

Error: %v
Callers: %v
##############################
`, block.Number(), block.Hash(), core.ErrBlacklistedHash, debug.Callers(20)))
			return k, currBlock, commBlock, trie, core.ErrBlacklistedHash
		}

		// Wait for the block's verification to complete
		var err error

		var usedGas uint64
		// proctime := time.Since(start)

		// Calculate the total difficulty of the block
		ptd := rawdb.ReadTd(db, block.ParentHash(), block.NumberU64()-1)
		if ptd == nil {
			return 0, currBlock, commBlock, trie, consensus.ErrUnknownAncestor
		}

		// Irrelevant of the canonical status, write the block itself to the database
		if !common.IsCanceled(ctx) {

			rawdb.WriteBody(ctx, db, block.Hash(), block.NumberU64(), block.Body())

			futureBlocks.Remove(block.Hash())

			chainSideFeed.Send(core.ChainSideEvent{Block: block})

			if err != nil {
				db.Rollback()
				trie = nil
				if commBlock != nil {
					currBlock = commBlock // Stuck
				}
				return k, currBlock, commBlock, trie, err
			}
		}
		log.Debug("Inserted new block", "number", block.Number(), "hash", block.Hash(),
			"uncles", len(block.Uncles()), "txs", len(block.Transactions()), "gas", block.GasUsed(),
			"elapsed", common.PrettyDuration(time.Since(start)),
			"root", block.Root())
		// bc.gcproc += proctime

		stats.Processed++
		stats.UsedGas += usedGas
		toCommit := stats.NeedToCommit(chain, db, i)
		stats.Report(chain, i, db, toCommit)
		if toCommit {
			var written uint64
			if written, err = db.Commit(); err != nil {
				log.Error("Could not commit chainDb", "error", err)
				db.Rollback()
				trie = nil
				if commBlock != nil {
					currBlock = commBlock
				}
				return k, currBlock, commBlock, trie, err
			}
			commBlock = currBlock
			committedK = k
			if trie != nil {
				trie.EvictTries(false)
			}
			size, err := db.(ethdb.HasStats).DiskSize(context.Background())
			if err != nil {
				return k, currBlock, commBlock, trie, err
			}
			log.Info("Database", "size", size, "written", written)
		}
	}

	return committedK + 1, currBlock, commBlock, trie, nil
}
