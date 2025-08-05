// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package jsonrpc

import (
	"context"
	"errors"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/polygon/bor/valset"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// bor consensus snapshot (only used for RPC purposes)
type Snapshot struct {
	Number       uint64        `json:"number"`       // Block number where the snapshot was created
	Hash         common.Hash   `json:"hash"`         // Block hash where the snapshot was created
	ValidatorSet *ValidatorSet `json:"validatorSet"` // Validator set at this moment
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

// GetSnapshot retrieves the state snapshot at a given block.
func (api *BorImpl) GetSnapshot(number *rpc.BlockNumber) (*Snapshot, error) {
	// init chain db
	ctx := context.Background()
	tx, err := api.db.BeginTemporalRo(ctx)
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

	validatorSet, err := api.spanProducersReader.Producers(ctx, header.Number.Uint64())
	if err != nil {
		return nil, err
	}
	snap := &Snapshot{
		Number:       header.Number.Uint64(),
		Hash:         header.Hash(),
		ValidatorSet: validatorSet,
	}
	return snap, nil
}

// GetAuthor retrieves the author a block.
func (api *BorImpl) GetAuthor(blockNrOrHash *rpc.BlockNumberOrHash) (*common.Address, error) {
	// init consensus engine
	borEngine, err := api.bor()

	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Retrieve the requested block number (or current if none requested)
	var header *types.Header

	//nolint:nestif
	if blockNrOrHash == nil {
		latestBlockNum, err2 := rpchelper.GetLatestBlockNumber(tx)
		if err2 != nil {
			return nil, err2
		}
		header, err = api._blockReader.HeaderByNumber(ctx, tx, latestBlockNum)
	} else {
		if blockNr, ok := blockNrOrHash.Number(); ok {
			header, err = api._blockReader.HeaderByNumber(ctx, tx, uint64(blockNr))
		} else {
			if blockHash, ok := blockNrOrHash.Hash(); ok {
				header, err = api._blockReader.HeaderByHash(ctx, tx, blockHash)
			}
		}
	}

	// Ensure we have an actually valid block and return its snapshot
	if header == nil || err != nil {
		return nil, errUnknownBlock
	}

	author, err := borEngine.Author(header)

	return &author, err
}

// GetSnapshotAtHash retrieves the state snapshot at a given block.
func (api *BorImpl) GetSnapshotAtHash(hash common.Hash) (*Snapshot, error) {
	// init chain db
	ctx := context.Background()
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Retreive the header
	header, err := getHeaderByHash(ctx, api, tx, hash)
	if err != nil {
		return nil, err
	}

	// Ensure we have an actually valid block
	if header == nil {
		return nil, errUnknownBlock
	}

	validatorSet, err := api.spanProducersReader.Producers(ctx, header.Number.Uint64())
	if err != nil {
		return nil, err
	}

	snap := &Snapshot{
		Number:       header.Number.Uint64(),
		Hash:         header.Hash(),
		ValidatorSet: validatorSet,
	}
	return snap, nil
}

// GetSigners retrieves the list of authorized signers at the specified block.
func (api *BorImpl) GetSigners(number *rpc.BlockNumber) ([]common.Address, error) {
	// init chain db
	ctx := context.Background()
	tx, err := api.db.BeginTemporalRo(ctx)
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

	validatorSet, err := api.spanProducersReader.Producers(ctx, header.Number.Uint64())
	if err != nil {
		return nil, err
	}
	return validatorSet.Signers(), nil
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

	// Retrieve the header
	header, _ := getHeaderByHash(ctx, api, tx, hash)

	// Ensure we have an actually valid block
	if header == nil {
		return nil, errUnknownBlock
	}

	validatorSet, err := api.spanProducersReader.Producers(ctx, header.Number.Uint64())
	if err != nil {
		return nil, err
	}
	return validatorSet.Signers(), err

}

// GetCurrentProposer gets the current proposer
func (api *BorImpl) GetCurrentProposer() (common.Address, error) {
	ctx := context.Background()
	latestBlockNum, err := api.getLatestBlockNum(ctx)
	if err != nil {
		return common.Address{}, err
	}
	validatorSet, err := api.spanProducersReader.Producers(ctx, latestBlockNum)
	if err != nil {
		return common.Address{}, err
	}
	return validatorSet.Proposer.Address, nil
}

// GetCurrentValidators gets the current validators
func (api *BorImpl) GetCurrentValidators() ([]*valset.Validator, error) {
	ctx := context.Background()
	latestBlockNum, err := api.getLatestBlockNum(ctx)
	if err != nil {
		return make([]*valset.Validator, 0), err
	}
	validatorSet, err := api.spanProducersReader.Producers(ctx, latestBlockNum)
	if err != nil {
		return make([]*valset.Validator, 0), err
	}
	return validatorSet.Validators, nil
}

// GetVoteOnHash gets the vote on milestone hash
func (api *BorImpl) GetVoteOnHash(ctx context.Context, starBlockNr uint64, endBlockNr uint64, hash string, milestoneId string) (bool, error) {
	tx, err := api.db.BeginRo(context.Background())
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

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

	return localEndBlockHash == hash, nil
}

// GetRootHash returns the merkle root of the start to end block headers
func (api *BorImpl) GetRootHash(start, end uint64) (string, error) {
	borEngine, err := api.bor()

	if err != nil {
		return "", err
	}

	ctx := context.Background()
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()

	return borEngine.GetRootHash(ctx, tx, start, end)
}

// helper to get the latest block number
func (api *BorImpl) getLatestBlockNum(ctx context.Context) (uint64, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	return rpchelper.GetLatestBlockNumber(tx)
}

// GetSnapshotProposer retrieves the in-turn signer at a given block.
func (api *BorImpl) GetSnapshotProposer(blockNrOrHash *rpc.BlockNumberOrHash) (common.Address, error) {
	// init chain db
	ctx := context.Background()
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return common.Address{}, err
	}
	defer tx.Rollback()

	var header *types.Header
	//nolint:nestif
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

	if header == nil || err != nil {
		return common.Address{}, errUnknownBlock
	}

	validatorSet, err := api.spanProducersReader.Producers(ctx, header.Number.Uint64())
	if err != nil {
		return common.Address{}, err
	}
	return validatorSet.GetProposer().Address, nil
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

	validatorSet, err := api.spanProducersReader.Producers(ctx, header.Number.Uint64())
	if err != nil {
		return BlockSigners{}, err
	}

	var difficulties = make(map[common.Address]uint64)

	proposer := validatorSet.GetProposer().Address
	proposerIndex, _ := validatorSet.GetByAddress(proposer)

	signers := validatorSet.Signers()
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
