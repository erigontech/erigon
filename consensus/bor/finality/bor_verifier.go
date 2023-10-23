// nolint
package finality

import (
	"context"
	"errors"
	"fmt"
	"github.com/ledgerwatch/erigon-lib/metrics"

	"github.com/ledgerwatch/erigon/consensus/bor/finality/generics"
	"github.com/ledgerwatch/erigon/consensus/bor/finality/whitelist"
	"github.com/ledgerwatch/erigon/core/rawdb"
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

	//Metrics for collecting the rewindLength
	rewindLengthMeter = metrics.GetOrCreateCounter("chain_autorewind_length")
)

type borVerifier struct {
	verify func(ctx context.Context, config *config, start uint64, end uint64, hash string, isCheckpoint bool) (string, error)
}

func newBorVerifier() *borVerifier {
	return &borVerifier{borVerify}
}

func borVerify(ctx context.Context, config *config, start uint64, end uint64, hash string, isCheckpoint bool) (string, error) {
	roTx, err := config.chainDB.BeginRo(ctx)
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
	currentBlock := rawdb.ReadCurrentBlockNumber(roTx)
	if currentBlock == nil {
		log.Debug(fmt.Sprintf("Failed to fetch current block from blockchain while verifying incoming %s", str))
		return hash, errMissingBlocks
	}

	head := *currentBlock
	if head < end {
		log.Debug(fmt.Sprintf("Current head block behind incoming %s block", str), "head", head, "end block", end)
		return hash, errMissingBlocks
	}

	var localHash string

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
		block, err := config.blockReader.BlockByNumber(context.Background(), roTx, end)
		if err != nil {
			log.Debug("Failed to get end block hash while whitelisting milestone", "number", end, "err", err)
			return hash, errEndBlock
		}

		localHash = fmt.Sprintf("%v", block.Hash())[2:]
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

		if isCheckpoint {
			log.Warn("Rewinding chain due to checkpoint root hash mismatch", "number", rewindTo)
		} else {
			log.Warn("Rewinding chain due to milestone endblock hash mismatch", "number", rewindTo)
		}

		rewindBack(head, rewindTo)

		return hash, errHashMismatch
	}

	// fetch the end block hash
	block, err := config.blockReader.BlockByNumber(context.Background(), roTx, end)
	if err != nil {
		log.Debug("Failed to get end block hash while whitelisting", "err", err)
		return hash, errEndBlock
	}
	if block == nil {
		log.Debug("Current header behind the end block", "block", end)
		return hash, errEndBlock
	}

	hash = fmt.Sprintf("%v", block.Hash())

	return hash, nil
}

// Stop the miner if the mining process is running and rewind back the chain
func rewindBack(head uint64, rewindTo uint64) {
	rewindLengthMeter.Set(head - rewindTo)

	// Chain cannot be rewinded from this routine
	// hence we are using a shared variable
	generics.BorMilestoneRewind.Store(&rewindTo)
}
