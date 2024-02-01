package finality

import (
	"context"
	"errors"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/polygon/heimdall"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/polygon/bor/finality/whitelist"
)

var (
	// errCheckpoint is returned when we are unable to fetch the
	// latest checkpoint from the local heimdall.
	errCheckpoint = errors.New("failed to fetch latest checkpoint")

	// errMilestone is returned when we are unable to fetch the
	// latest milestone from the local heimdall.
	errMilestone = errors.New("failed to fetch latest milestone")

	ErrNotInRejectedList = errors.New("MilestoneID not in rejected list")
)

// fetchWhitelistCheckpoint fetches the latest checkpoint from it's local heimdall
// and verifies the data against bor data.
func fetchWhitelistCheckpoint(ctx context.Context, heimdallClient heimdall.HeimdallClient, verifier *borVerifier, config *config) (uint64, common.Hash, error) {
	var (
		blockNum  uint64
		blockHash common.Hash
	)

	// fetch the latest checkpoint from Heimdall
	checkpoint, err := heimdallClient.FetchCheckpoint(ctx, -1)
	if err != nil {
		config.logger.Debug("[bor.heimdall] Failed to fetch latest checkpoint for whitelisting", "err", err)
		return blockNum, blockHash, errCheckpoint
	}

	// Verify if the checkpoint fetched can be added to the local whitelist entry or not
	// If verified, it returns the hash of the end block of the checkpoint. If not,
	// it will return appropriate error.
	hash, err := verifier.verify(ctx, config, checkpoint.StartBlock().Uint64(), checkpoint.EndBlock().Uint64(), checkpoint.RootHash().String()[2:], true)

	if err != nil {
		if errors.Is(err, errMissingBlocks) {
			config.logger.Debug("[bor.heimdall] Got new checkpoint", "start", checkpoint.StartBlock().Uint64(), "end", checkpoint.EndBlock().Uint64(), "rootHash", checkpoint.RootHash().String())
			config.logger.Debug("[bor.heimdall] Failed to whitelist checkpoint", "err", err)
		} else {
			config.logger.Info("[bor.heimdall] Got new checkpoint", "start", checkpoint.StartBlock().Uint64(), "end", checkpoint.EndBlock().Uint64(), "rootHash", checkpoint.RootHash().String())
			config.logger.Warn("[bor.heimdall] Failed to whitelist checkpoint", "err", err)
		}

		return blockNum, blockHash, err
	}

	config.logger.Info("[bor.heimdall] Got new checkpoint", "start", checkpoint.StartBlock().Uint64(), "end", checkpoint.EndBlock().Uint64(), "rootHash", checkpoint.RootHash().String())

	blockNum = checkpoint.EndBlock().Uint64()
	blockHash = common.HexToHash(hash)

	return blockNum, blockHash, nil
}

// fetchWhitelistMilestone fetches the latest milestone from it's local heimdall
// and verifies the data against bor data.
func fetchWhitelistMilestone(ctx context.Context, heimdallClient heimdall.HeimdallClient, verifier *borVerifier, config *config) (uint64, common.Hash, error) {
	var (
		num  uint64
		hash common.Hash
	)

	// fetch latest milestone
	milestone, err := heimdallClient.FetchMilestone(ctx, -1)
	if errors.Is(err, heimdall.ErrServiceUnavailable) {
		config.logger.Debug("[bor.heimdall] Failed to fetch latest milestone for whitelisting", "err", err)
		return num, hash, err
	}

	if err != nil {
		config.logger.Warn("[bor.heimdall]  Failed to fetch latest milestone for whitelisting", "err", err)
		return num, hash, errMilestone
	}

	config.logger.Debug("[bor.heimdall] Got new milestone", "start", milestone.StartBlock().Uint64(), "end", milestone.EndBlock().Uint64())

	num = milestone.EndBlock().Uint64()
	hash = milestone.RootHash()

	// Verify if the milestone fetched can be added to the local whitelist entry or not
	// If verified, it returns the hash of the end block of the milestone. If not,
	// it will return appropriate error.
	_, err = verifier.verify(ctx, config, milestone.StartBlock().Uint64(), milestone.EndBlock().Uint64(), milestone.RootHash().String()[2:], false)
	if err != nil {
		whitelist.GetWhitelistingService().UnlockSprint(milestone.EndBlock().Uint64())
		return num, hash, err
	}

	return num, hash, nil
}

func fetchNoAckMilestone(ctx context.Context, heimdallClient heimdall.HeimdallClient, logger log.Logger) (string, error) {
	var (
		milestoneID string
	)

	milestoneID, err := heimdallClient.FetchLastNoAckMilestone(ctx)
	if errors.Is(err, heimdall.ErrServiceUnavailable) {
		logger.Debug("[bor.heimdall] Failed to fetch latest no-ack milestone", "err", err)
		return milestoneID, err
	}

	if err != nil {
		logger.Warn("[bor.heimdall] Failed to fetch latest no-ack milestone", "err", err)
		return milestoneID, errMilestone
	}

	return milestoneID, nil
}

func fetchNoAckMilestoneByID(ctx context.Context, heimdallClient heimdall.HeimdallClient, milestoneID string, logger log.Logger) error {
	err := heimdallClient.FetchNoAckMilestone(ctx, milestoneID)
	if errors.Is(err, heimdall.ErrServiceUnavailable) {
		logger.Debug("[bor.heimdall] Failed to fetch no-ack milestone by ID", "milestoneID", milestoneID, "err", err)
		return err
	}

	// fixme: handle different types of errors
	if errors.Is(err, ErrNotInRejectedList) {
		logger.Warn("[bor.heimdall] MilestoneID not in rejected list", "milestoneID", milestoneID, "err", err)
		return err
	}

	if err != nil {
		logger.Warn("[bor.heimdall] Failed to fetch no-ack milestone by ID ", "milestoneID", milestoneID, "err", err)
		return errMilestone
	}

	return nil
}
