package borfinality

import (
	"context"
	"errors"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/consensus/bor"
	"github.com/ledgerwatch/erigon/eth/borfinality/whitelist"
	"github.com/ledgerwatch/log/v3"
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

// handleWhitelistCheckpoint handles the checkpoint whitelist mechanism.
func handleWhitelistCheckpoint(ctx context.Context, bor *bor.Bor, config *config) error {
	// Create a new bor verifier, which will be used to verify checkpoints and milestones
	verifier := newBorVerifier()
	service := whitelist.GetWhitelistingService()

	blockNum, blockHash, err := fetchWhitelistCheckpoint(ctx, bor, verifier, config)

	// If the array is empty, we're bound to receive an error. Non-nill error and non-empty array
	// means that array has partial elements and it failed for some block. We'll add those partial
	// elements anyway.
	if err != nil {
		return err
	}

	service.ProcessCheckpoint(blockNum, blockHash)

	return nil
}

// handleMilestone handles the milestone mechanism.
func handleMilestone(ctx context.Context, bor *bor.Bor, config *config) error {
	// Create a new bor verifier, which will be used to verify checkpoints and milestones
	verifier := newBorVerifier()

	service := whitelist.GetWhitelistingService()

	num, hash, err := fetchWhitelistMilestone(ctx, bor, verifier, config)

	// If the current chain head is behind the received milestone, add it to the future milestone
	// list. Also, the hash mismatch (end block hash) error will lead to rewind so also
	// add that milestone to the future milestone list.
	if errors.Is(err, errMissingBlocks) || errors.Is(err, errHashMismatch) {
		service.ProcessFutureMilestone(num, hash)
	}

	if err != nil {
		return err
	}

	service.ProcessMilestone(num, hash)

	return nil
}

func handleNoAckMilestone(ctx context.Context, bor *bor.Bor, config *config) error {
	milestoneID, err := fetchNoAckMilestone(ctx, bor)

	service := whitelist.GetWhitelistingService()

	//If failed to fetch the no-ack milestone then it give the error.
	if err != nil {
		return err
	}

	service.RemoveMilestoneID(milestoneID)

	return nil
}

func handleNoAckMilestoneByID(ctx context.Context, bor *bor.Bor, config *config) error {
	service := whitelist.GetWhitelistingService()

	milestoneIDs := service.GetMilestoneIDsList()

	for _, milestoneID := range milestoneIDs {
		// todo: check if we can ignore the error
		err := fetchNoAckMilestoneByID(ctx, bor, milestoneID)
		if err == nil {
			service.RemoveMilestoneID(milestoneID)
		}
	}

	return nil
}

// fetchWhitelistCheckpoint fetches the latest checkpoint from it's local heimdall
// and verifies the data against bor data.
func fetchWhitelistCheckpoint(ctx context.Context, bor *bor.Bor, verifier *borVerifier, config *config) (uint64, common.Hash, error) {
	var (
		blockNum  uint64
		blockHash common.Hash
	)

	// fetch the latest checkpoint from Heimdall
	checkpoint, err := bor.HeimdallClient.FetchCheckpoint(ctx, -1)
	if err != nil {
		log.Debug("Failed to fetch latest checkpoint for whitelisting", "err", err)
		return blockNum, blockHash, errCheckpoint
	}

	// Verify if the checkpoint fetched can be added to the local whitelist entry or not
	// If verified, it returns the hash of the end block of the checkpoint. If not,
	// it will return appropriate error.
	hash, err := verifier.verify(ctx, config, checkpoint.StartBlock.Uint64(), checkpoint.EndBlock.Uint64(), checkpoint.RootHash.String()[2:], true)
	if err != nil {
		log.Warn("Failed to whitelist checkpoint", "err", err)
		return blockNum, blockHash, err
	}

	blockNum = checkpoint.EndBlock.Uint64()
	blockHash = common.HexToHash(hash)

	return blockNum, blockHash, nil
}

// fetchWhitelistMilestone fetches the latest milestone from it's local heimdall
// and verifies the data against bor data.
func fetchWhitelistMilestone(ctx context.Context, bor *bor.Bor, verifier *borVerifier, config *config) (uint64, common.Hash, error) {
	var (
		num  uint64
		hash common.Hash
	)

	// fetch latest milestone
	milestone, err := bor.HeimdallClient.FetchMilestone(ctx)
	if err != nil {
		log.Error("Failed to fetch latest milestone for whitelisting", "err", err)
		return num, hash, errMilestone
	}

	num = milestone.EndBlock.Uint64()
	hash = milestone.Hash

	// Verify if the milestone fetched can be added to the local whitelist entry or not
	// If verified, it returns the hash of the end block of the milestone. If not,
	// it will return appropriate error.
	_, err = verifier.verify(ctx, config, milestone.StartBlock.Uint64(), milestone.EndBlock.Uint64(), milestone.Hash.String()[2:], false)
	if err != nil {
		// h.downloader.UnlockSprint(milestone.EndBlock.Uint64())
		return num, hash, err
	}

	return num, hash, nil
}

func fetchNoAckMilestone(ctx context.Context, bor *bor.Bor) (string, error) {
	var (
		milestoneID string
	)

	// fetch latest milestone
	milestoneID, err := bor.HeimdallClient.FetchLastNoAckMilestone(ctx)
	if err != nil {
		log.Error("Failed to fetch latest no-ack milestone", "err", err)

		return milestoneID, errMilestone
	}

	return milestoneID, nil
}

func fetchNoAckMilestoneByID(ctx context.Context, bor *bor.Bor, milestoneID string) error {
	// fetch latest milestone
	err := bor.HeimdallClient.FetchNoAckMilestone(ctx, milestoneID)

	// fixme: handle different types of errors
	if errors.Is(err, ErrNotInRejectedList) {
		log.Warn("MilestoneID not in rejected list", "milestoneID", milestoneID, "err", err)
		return err
	}

	if err != nil {
		log.Error("Failed to fetch no-ack milestone by ID ", "milestoneID", milestoneID, "err", err)

		return errMilestone
	}

	return nil
}
