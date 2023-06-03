// nolint
package borfinality

import (
	"context"
	"errors"
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/borfinality/whitelist"
	"github.com/ledgerwatch/log/v3"
)

var (
	// errMissingBlocks is returned when we don't have the blocks locally, yet.
	errMissingBlocks = errors.New("missing blocks")

	// errRootHash is returned when we aren't able to calculate the root hash
	// locally for a range of blocks.
	errRootHash = errors.New("failed to get local root hash")

	// errHashMismatch is returned when the local hash doesn't match
	// with the hash of checkpoint/milestone. It is the root hash of blocks
	// in case of checkpoint and is end block hash in case of milestones.
	errHashMismatch = errors.New("hash mismatch")

	// errEndBlock is returned when we're unable to fetch a block locally.
	errEndBlock = errors.New("failed to get end block")

	// TODO: Uncomment once metrics is added
	// Metrics for collecting the rewindLength
	// rewindLengthMeter = metrics.NewRegisteredMeter("chain/autorewind/length", nil)
)

type borVerifier struct {
	verify func(ctx context.Context, config *config, start uint64, end uint64, hash string, isCheckpoint bool) (string, error)
}

func newBorVerifier() *borVerifier {
	return &borVerifier{borVerify}
}

func borVerify(ctx context.Context, config *config, start uint64, end uint64, hash string, isCheckpoint bool) (string, error) {
	roTx, err := config.chaindb.BeginRo(ctx)
	if err != nil {
		return hash, err
	}
	defer roTx.Rollback()

	str := "milestone"
	if isCheckpoint {
		str = "checkpoint"
	}

	service := whitelist.GetWhitelistingService()

	// check if we have the given blocks
	currentBlock := rawdb.ReadCurrentBlock(roTx)
	fmt.Println("CurrentBlock: ", currentBlock)
	if currentBlock == nil {
		log.Debug(fmt.Sprintf("Failed to fetch current block from blockchain while verifying incoming %s", str))
		return hash, errMissingBlocks
	}

	head := currentBlock.Number().Uint64()
	fmt.Println("CurrentBlock: head: ", head, "end: ", end)
	if head < end {
		log.Debug(fmt.Sprintf("Current head block behind incoming %s block", str), "head", head, "end block", end)
		return hash, errMissingBlocks
	}

	var localHash string
	logger := log.New()
	reqGen := requests.NewRequestGenerator(logger)

	// verify the hash
	if isCheckpoint {
		var err error

		// in case of checkpoint get the rootHash
		localHash, err = config.borAPI.GetRootHash(start, end)

		if err != nil {
			log.Debug("Failed to get root hash of given block range while whitelisting checkpoint", "start", start, "end", end, "err", err)
			return hash, errRootHash
		}
	} else {
		// in case of milestone(isCheckpoint==false) get the hash of endBlock
		block, err := requests.GetBlockByNumber(reqGen, end, false, config.logger)
		if err != nil {
			log.Debug("Failed to get end block hash while whitelisting milestone", "number", end, "err", err)
			return hash, errEndBlock
		}

		localHash = fmt.Sprintf("%v", block.Result.Hash)[2:]
	}

	//nolint
	if localHash != hash {

		if isCheckpoint {
			log.Warn("Root hash mismatch while whitelisting checkpoint", "expected", localHash, "got", hash)
		} else {
			log.Warn("End block hash mismatch while whitelisting milestone", "expected", localHash, "got", hash)
		}

		var (
			rewindTo uint64
			doExist  bool
		)

		if doExist, rewindTo, _ = service.GetWhitelistedMilestone(); doExist {

		} else if doExist, rewindTo, _ = service.GetWhitelistedCheckpoint(); doExist {

		} else {
			if start <= 0 {
				rewindTo = 0
			} else {
				rewindTo = start - 1
			}
		}

		if head-rewindTo > 255 {
			rewindTo = head - 255
		}

		rewindBack(config, head, rewindTo)

		return hash, errHashMismatch
	}

	// fetch the end block hash
	block, err := requests.GetBlockByNumber(reqGen, end, false, config.logger)
	if err != nil {
		log.Debug("Failed to get end block hash while whitelisting", "err", err)
		return hash, errEndBlock
	}

	hash = fmt.Sprintf("%v", block.Result.Hash)

	return hash, nil
}

// Stop the miner if the mining process is running and rewind back the chain
func rewindBack(config *config, head uint64, rewindTo uint64) {
	// TODO: Uncomment once minning is added

	// if eth.Miner().Mining() {
	// 	ch := make(chan struct{})
	// 	eth.Miner().Stop(ch)

	// 	<-ch
	// 	rewind(eth, head, rewindTo)

	// 	eth.Miner().Start(eth.etherbase)
	// } else {
	rewind(config, head, rewindTo)
	// }
}

func rewind(config *config, head uint64, rewindTo uint64) {
	log.Warn("Rewinding chain because it doesn't match the received milestone", "to", rewindTo)

	logger := log.New()
	reqGen := requests.NewRequestGenerator(logger)

	// fetch the end block hash
	block, err := requests.GetBlockByNumber(reqGen, head, false, config.logger)
	if err != nil {
		log.Debug("Failed to get end block hash while rewinding/unwinding", "err", err)
		return
	}

	tx, err := config.chaindb.BeginRw(context.Background())
	if err != nil {
		log.Error("Failed to start a database transaction", "err", err)
		return
	}
	defer tx.Rollback()

	// rewind the chain
	config.stagedSync.UnwindTo(rewindTo, block.Result.Hash)
	err = config.stagedSync.RunUnwind(config.chaindb, tx)
	if err != nil {
		log.Error("Failed to unwind chain", "err", err)
		return
	}

	// TODO: Uncomment once metrics is added
	// else {
	// 	rewindLengthMeter.Mark(int64(head - rewindTo))
	// }

}
