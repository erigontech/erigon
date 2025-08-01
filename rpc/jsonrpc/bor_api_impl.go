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
	"fmt"

	"github.com/erigontech/erigon-db/rawdb"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/polygon/bor/finality/whitelist"
	"github.com/erigontech/erigon/polygon/bor/valset"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

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

	// init consensus engine
	borEngine, err := api.bor()

	if err != nil {
		return nil, err
	}

	borTx, err := borEngine.DB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer borTx.Rollback()

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

	// init consensus engine
	borEngine, err := api.bor()

	if err != nil {
		return nil, err
	}

	borTx, err := borEngine.DB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer borTx.Rollback()

	validatorSet, err := api.spanProducersReader.Producers(ctx, header.Number.Uint64())
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

	// TODO whitelisting service is pending removal - https://github.com/erigontech/erigon/issues/12855
	if service := whitelist.GetWhitelistingService(); service != nil {
		isLocked := service.LockMutex(endBlockNr)

		if !isLocked {
			service.UnlockMutex(false, "", endBlockNr, common.Hash{})
			return false, errors.New("whitelisted number or locked sprint number is more than the received end block number")
		}

		if localEndBlockHash != hash {
			service.UnlockMutex(false, "", endBlockNr, common.Hash{})
			return false, fmt.Errorf("hash mismatch: localChainHash %s, milestoneHash %s", localEndBlockHash, hash)
		}

		service.UnlockMutex(true, milestoneId, endBlockNr, localEndBlock.Hash())

		return true, nil
	}

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
