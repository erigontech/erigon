package jsonrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/bor"
	"github.com/ledgerwatch/erigon/consensus/bor/valset"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/borfinality/whitelist"
	"github.com/ledgerwatch/erigon/rpc"
)

type Snapshot struct {
	config *chain.BorConfig // Consensus engine parameters to fine tune behavior

	Number       uint64                    `json:"number"`       // Block number where the snapshot was created
	Hash         common.Hash               `json:"hash"`         // Block hash where the snapshot was created
	ValidatorSet *ValidatorSet             `json:"validatorSet"` // Validator set at this moment
	Recents      map[uint64]common.Address `json:"recents"`      // Set of recent signers for spam protections
}

// GetSnapshot retrieves the state snapshot at a given block.
func (api *BorImpl) GetSnapshot(number *rpc.BlockNumber) (*Snapshot, error) {
	// init chain db
	ctx := context.Background()
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Retrieve the requested block number (or current if none requested)
	var header *types.Header
	if number == nil || *number == rpc.LatestBlockNumber {
		header = rawdb.ReadCurrentHeader(tx)
	} else {
		header, _ = getHeaderByNumber(ctx, *number, api, tx)
	}
	// Ensure we have an actually valid block
	if header == nil {
		return nil, errUnknownBlock
	}

	// init consensus db
	borTx, err := api.bor.DB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer borTx.Rollback()
	return snapshot(ctx, api, tx, borTx, header)
}

// GetAuthor retrieves the author a block.
func (api *BorImpl) GetAuthor(number *rpc.BlockNumber) (*common.Address, error) {
	ctx := context.Background()
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Retrieve the requested block number (or current if none requested)
	var header *types.Header
	if number == nil || *number == rpc.LatestBlockNumber {
		header = rawdb.ReadCurrentHeader(tx)
	} else {
		header, _ = getHeaderByNumber(ctx, *number, api, tx)
	}
	// Ensure we have an actually valid block
	if header == nil {
		return nil, errUnknownBlock
	}
	author, err := author(api, tx, header)
	return &author, err
}

// GetSnapshotAtHash retrieves the state snapshot at a given block.
func (api *BorImpl) GetSnapshotAtHash(hash common.Hash) (*Snapshot, error) {
	// init chain db
	ctx := context.Background()
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Retreive the header
	header, _ := getHeaderByHash(ctx, api, tx, hash)

	// Ensure we have an actually valid block
	if header == nil {
		return nil, errUnknownBlock
	}

	// init consensus db
	borTx, err := api.bor.DB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer borTx.Rollback()
	return snapshot(ctx, api, tx, borTx, header)
}

// GetSigners retrieves the list of authorized signers at the specified block.
func (api *BorImpl) GetSigners(number *rpc.BlockNumber) ([]common.Address, error) {
	// init chain db
	ctx := context.Background()
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Retrieve the requested block number (or current if none requested)
	var header *types.Header
	if number == nil || *number == rpc.LatestBlockNumber {
		header = rawdb.ReadCurrentHeader(tx)
	} else {
		header, _ = getHeaderByNumber(ctx, *number, api, tx)
	}
	// Ensure we have an actually valid block
	if header == nil {
		return nil, errUnknownBlock
	}

	// init consensus db
	borTx, err := api.bor.DB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer borTx.Rollback()
	snap, err := snapshot(ctx, api, tx, borTx, header)
	return snap.signers(), err
}

// GetSignersAtHash retrieves the list of authorized signers at the specified block.
func (api *BorImpl) GetSignersAtHash(hash common.Hash) ([]common.Address, error) {
	// init chain db
	ctx := context.Background()
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Retreive the header
	header, _ := getHeaderByHash(ctx, api, tx, hash)

	// Ensure we have an actually valid block
	if header == nil {
		return nil, errUnknownBlock
	}

	// init consensus db
	borTx, err := api.bor.DB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer borTx.Rollback()

	snap, err := snapshot(ctx, api, tx, borTx, header)
	return snap.signers(), err
}

// GetCurrentProposer gets the current proposer
func (api *BorImpl) GetCurrentProposer() (common.Address, error) {
	snap, err := api.GetSnapshot(nil)
	if err != nil {
		return common.Address{}, err
	}
	return snap.ValidatorSet.GetProposer().Address, nil
}

// GetCurrentValidators gets the current validators
func (api *BorImpl) GetCurrentValidators() ([]*valset.Validator, error) {
	snap, err := api.GetSnapshot(nil)
	if err != nil {
		return make([]*valset.Validator, 0), err
	}
	return snap.ValidatorSet.Validators, nil
}

// GetVoteOnHash gets the vote on milestone hash
func (api *BorImpl) GetVoteOnHash(ctx context.Context, starBlockNr uint64, endBlockNr uint64, hash string, milestoneId string) (bool, error) {
	tx, err := api.db.BeginRo(context.Background())
	if err != nil {
		return false, err
	}

	service := whitelist.GetWhitelistingService()

	if service == nil {
		return false, errors.New("Only available in Bor engine")
	}

	//Confirmation of 16 blocks on the endblock
	tipConfirmationBlockNr := endBlockNr + uint64(16)

	//Check if tipConfirmation block exit
	_, err = api._blockReader.BlockByNumber(ctx, tx, tipConfirmationBlockNr)
	if err != nil {
		return false, errors.New("failed to get tip confirmation block")
	}

	//Check if end block exist
	localEndBlock, err := api._blockReader.BlockByNumber(ctx, tx, endBlockNr)
	if err != nil {
		return false, errors.New("failed to get end block")
	}

	localEndBlockHash := localEndBlock.Hash().String()

	isLocked := service.LockMutex(endBlockNr)

	if !isLocked {
		service.UnlockMutex(false, "", endBlockNr, common.Hash{})
		return false, errors.New("whitelisted number or locked sprint number is more than the received end block number")
	}

	if localEndBlockHash != hash {
		service.UnlockMutex(false, "", endBlockNr, common.Hash{})
		return false, fmt.Errorf("hash mismatch: localChainHash %s, milestoneHash %s", localEndBlockHash, hash)
	}

	bor, ok := api._engine.(*bor.Bor)

	if !ok {
		return false, errors.New("bor engine not available")
	}

	err = bor.HeimdallClient.FetchMilestoneID(ctx, milestoneId)

	if err != nil {
		service.UnlockMutex(false, "", endBlockNr, common.Hash{})
		return false, errors.New("milestone ID doesn't exist in Heimdall")
	}

	service.UnlockMutex(true, milestoneId, endBlockNr, localEndBlock.Hash())

	return true, nil
}

type BlockSigners struct {
	Signers []difficultiesKV
	Diff    int
	Author  common.Address
}

type difficultiesKV struct {
	Signer     common.Address
	Difficulty uint64
}

func (api *BorImpl) GetSnapshotProposerSequence(blockNrOrHash *rpc.BlockNumberOrHash) (BlockSigners, error) {
	// init chain db
	ctx := context.Background()
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return BlockSigners{}, err
	}
	defer tx.Rollback()

	// Retrieve the requested block number (or current if none requested)
	var header *types.Header
	if blockNrOrHash == nil {
		header = rawdb.ReadCurrentHeader(tx)
	} else {
		if blockNr, ok := blockNrOrHash.Number(); ok {
			if blockNr == rpc.LatestBlockNumber {
				header = rawdb.ReadCurrentHeader(tx)
			} else {
				header, err = getHeaderByNumber(ctx, blockNr, api, tx)
			}
		} else {
			if blockHash, ok := blockNrOrHash.Hash(); ok {
				header, err = getHeaderByHash(ctx, api, tx, blockHash)
			}
		}
	}

	// Ensure we have an actually valid block
	if header == nil || err != nil {
		return BlockSigners{}, errUnknownBlock
	}

	// init consensus db
	borTx, err := api.bor.DB.BeginRo(ctx)
	if err != nil {
		return BlockSigners{}, err
	}
	defer borTx.Rollback()

	parent, err := getHeaderByNumber(ctx, rpc.BlockNumber(int64(header.Number.Uint64()-1)), api, tx)
	if parent == nil || err != nil {
		return BlockSigners{}, errUnknownBlock
	}
	snap, err := snapshot(ctx, api, tx, borTx, parent)

	var difficulties = make(map[common.Address]uint64)

	if err != nil {
		return BlockSigners{}, err
	}

	proposer := snap.ValidatorSet.GetProposer().Address
	proposerIndex, _ := snap.ValidatorSet.GetByAddress(proposer)

	signers := snap.signers()
	for i := 0; i < len(signers); i++ {
		tempIndex := i
		if tempIndex < proposerIndex {
			tempIndex = tempIndex + len(signers)
		}

		difficulties[signers[i]] = uint64(len(signers) - (tempIndex - proposerIndex))
	}

	rankedDifficulties := rankMapDifficulties(difficulties)

	author, err := author(api, tx, header)
	if err != nil {
		return BlockSigners{}, err
	}

	diff := int(difficulties[author])
	blockSigners := BlockSigners{
		Signers: rankedDifficulties,
		Diff:    diff,
		Author:  author,
	}

	return blockSigners, nil
}

// GetRootHash returns the merkle root of the start to end block headers
func (api *BorImpl) GetRootHash(start, end uint64) (string, error) {
	ctx := context.Background()
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()

	return api.bor.GetRootHash(ctx, tx, start, end)
}

// Helper functions for Snapshot Type

// copy creates a deep copy of the snapshot, though not the individual votes.
func (s *Snapshot) copy() *Snapshot {
	cpy := &Snapshot{
		config:       s.config,
		Number:       s.Number,
		Hash:         s.Hash,
		ValidatorSet: s.ValidatorSet.Copy(),
		Recents:      make(map[uint64]common.Address),
	}
	for block, signer := range s.Recents {
		cpy.Recents[block] = signer
	}

	return cpy
}

// GetSignerSuccessionNumber returns the relative position of signer in terms of the in-turn proposer
func (s *Snapshot) GetSignerSuccessionNumber(signer common.Address) (int, error) {
	validators := s.ValidatorSet.Validators
	proposer := s.ValidatorSet.GetProposer().Address
	proposerIndex, _ := s.ValidatorSet.GetByAddress(proposer)
	if proposerIndex == -1 {
		return -1, &bor.UnauthorizedProposerError{Number: s.Number, Proposer: proposer.Bytes()}
	}
	signerIndex, _ := s.ValidatorSet.GetByAddress(signer)
	if signerIndex == -1 {
		return -1, &bor.UnauthorizedSignerError{Number: s.Number, Signer: signer.Bytes()}
	}

	tempIndex := signerIndex
	if proposerIndex != tempIndex {
		if tempIndex < proposerIndex {
			tempIndex = tempIndex + len(validators)
		}
	}
	return tempIndex - proposerIndex, nil
}

// signers retrieves the list of authorized signers in ascending order.
func (s *Snapshot) signers() []common.Address {
	sigs := make([]common.Address, 0, len(s.ValidatorSet.Validators))
	for _, sig := range s.ValidatorSet.Validators {
		sigs = append(sigs, sig.Address)
	}
	return sigs
}

// apply header changes on snapshot
func (s *Snapshot) apply(headers []*types.Header) (*Snapshot, error) {
	// Allow passing in no headers for cleaner code
	if len(headers) == 0 {
		return s, nil
	}
	// Sanity check that the headers can be applied
	for i := 0; i < len(headers)-1; i++ {
		if headers[i+1].Number.Uint64() != headers[i].Number.Uint64()+1 {
			return nil, errOutOfRangeChain
		}
	}
	if headers[0].Number.Uint64() != s.Number+1 {
		return nil, errOutOfRangeChain
	}
	// Iterate through the headers and create a new snapshot
	snap := s.copy()

	for _, header := range headers {
		// Remove any votes on checkpoint blocks
		number := header.Number.Uint64()

		// Delete the oldest signer from the recent list to allow it signing again
		currentSprint := s.config.CalculateSprint(number)
		if number >= currentSprint {
			delete(snap.Recents, number-currentSprint)
		}

		// Resolve the authorization key and check against signers
		signer, err := ecrecover(header, s.config)
		if err != nil {
			return nil, err
		}

		// check if signer is in validator set
		if !snap.ValidatorSet.HasAddress(signer.Bytes()) {
			return nil, &bor.UnauthorizedSignerError{Number: number, Signer: signer.Bytes()}
		}

		if _, err = snap.GetSignerSuccessionNumber(signer); err != nil {
			return nil, err
		}

		// add recents
		snap.Recents[number] = signer

		// change validator set and change proposer
		if number > 0 && (number+1)%currentSprint == 0 {
			if err := bor.ValidateHeaderExtraField(header.Extra); err != nil {
				return nil, err
			}
			validatorBytes := header.Extra[extraVanity : len(header.Extra)-extraSeal]

			// get validators from headers and use that for new validator set
			newVals, _ := valset.ParseValidators(validatorBytes)
			v := getUpdatedValidatorSet(snap.ValidatorSet.Copy(), newVals)
			v.IncrementProposerPriority(1)
			snap.ValidatorSet = v
		}
	}
	snap.Number += uint64(len(headers))
	snap.Hash = headers[len(headers)-1].Hash()

	return snap, nil
}

// snapshot retrieves the authorization snapshot at a given point in time.
func snapshot(ctx context.Context, api *BorImpl, db kv.Tx, borDb kv.Tx, header *types.Header) (*Snapshot, error) {
	// Search for a snapshot on disk or build it from checkpoint
	var (
		headers []*types.Header
		snap    *Snapshot
	)

	number := header.Number.Uint64()
	hash := header.Hash()

	for snap == nil {
		// If an on-disk checkpoint snapshot can be found, use that
		if number%checkpointInterval == 0 {
			if s, err := loadSnapshot(api, db, borDb, hash); err == nil {
				log.Info("Loaded snapshot from disk", "number", number, "hash", hash)
				snap = s
			}
			break
		}

		// No snapshot for this header, move backward and check parent snapshots
		if header == nil {
			header, _ = getHeaderByNumber(ctx, rpc.BlockNumber(number), api, db)
			if header == nil {
				return nil, consensus.ErrUnknownAncestor
			}
		}
		headers = append(headers, header)
		number, hash = number-1, header.ParentHash
		header = nil
	}

	if snap == nil {
		return nil, fmt.Errorf("unknown error while retrieving snapshot at block number %v", number)
	}

	// Previous snapshot found, apply any pending headers on top of it
	for i := 0; i < len(headers)/2; i++ {
		headers[i], headers[len(headers)-1-i] = headers[len(headers)-1-i], headers[i]
	}

	snap, err := snap.apply(headers)
	if err != nil {
		return nil, err
	}
	return snap, nil
}

// loadSnapshot loads an existing snapshot from the database.
func loadSnapshot(api *BorImpl, db kv.Tx, borDb kv.Tx, hash common.Hash) (*Snapshot, error) {
	blob, err := borDb.GetOne(kv.BorSeparate, append([]byte("bor-"), hash[:]...))
	if err != nil {
		return nil, err
	}
	snap := new(Snapshot)
	if err := json.Unmarshal(blob, snap); err != nil {
		return nil, err
	}
	config, _ := api.chainConfig(db)
	snap.config = config.Bor

	// update total voting power
	if err := snap.ValidatorSet.UpdateTotalVotingPower(); err != nil {
		return nil, err
	}

	return snap, nil
}
