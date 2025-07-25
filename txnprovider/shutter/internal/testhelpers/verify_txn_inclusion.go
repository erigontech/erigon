// Copyright 2025 The Erigon Authors
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

package testhelpers

import (
	"context"
	"errors"
	"fmt"

	mapset "github.com/deckarep/golang-set/v2"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/rpc/requests"
)

type TxnInclusionVerifier struct {
	rpcApiClient requests.RequestGenerator
}

func NewTxnInclusionVerifier(rpcApiClient requests.RequestGenerator) TxnInclusionVerifier {
	return TxnInclusionVerifier{
		rpcApiClient: rpcApiClient,
	}
}

func (v TxnInclusionVerifier) VerifyTxnsInclusion(
	ctx context.Context,
	payload *enginetypes.ExecutionPayload,
	inclusions ...common.Hash,
) error {
	inclusionHashes := mapset.NewSet[common.Hash](inclusions...)
	for i, txnBytes := range payload.Transactions {
		txn, err := types.DecodeTransaction(txnBytes)
		if err != nil {
			return err
		}

		r, err := v.rpcApiClient.GetTransactionReceipt(ctx, txn.Hash())
		if err != nil {
			return err
		}

		if r.Status != types.ReceiptStatusSuccessful {
			return fmt.Errorf("txn %d in block %d not successful", i, r.BlockNumber)
		}

		inclusionHashes.Remove(txn.Hash())
	}

	if inclusionHashes.Cardinality() == 0 {
		return nil
	}

	err := errors.New("txns not found in block")
	inclusionHashes.Each(func(txnHash common.Hash) bool {
		err = fmt.Errorf("%w: %s", err, txnHash)
		return true // continue
	})
	return err
}

func (v TxnInclusionVerifier) VerifyTxnsOrderedInclusion(
	ctx context.Context,
	payload *enginetypes.ExecutionPayload,
	inclusions ...OrderedInclusion,
) error {
	orderedInclusions := make(map[uint64]common.Hash, len(inclusions))
	for _, inclusion := range inclusions {
		orderedInclusions[inclusion.TxnIndex] = inclusion.TxnHash
	}

	var accErr error
	for i, txnBytes := range payload.Transactions {
		txn, err := types.DecodeTransaction(txnBytes)
		if err != nil {
			return err
		}

		inclusionHash, ok := orderedInclusions[uint64(i)]
		if !ok {
			continue
		}

		r, err := v.rpcApiClient.GetTransactionReceipt(ctx, txn.Hash())
		if err != nil {
			return err
		}

		if r.Status != types.ReceiptStatusSuccessful {
			return fmt.Errorf("txn %d in block %d not successful", i, r.BlockNumber)
		}

		if txn.Hash() == inclusionHash {
			continue
		}

		if accErr == nil {
			accErr = errors.New("txns missing")
		}

		accErr = fmt.Errorf("%w: (%d,%s)", accErr, i, inclusionHash)
	}

	return accErr
}

type OrderedInclusion struct {
	TxnHash  common.Hash
	TxnIndex uint64
}
