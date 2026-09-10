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

package engineapitester

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	mapset "github.com/deckarep/golang-set/v2"

	"github.com/erigontech/erigon/common"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/types"
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

		// fcu persistance is now asynchronous so this can get called
		// in the test loop before the tx data is coommited in which
		// case it will fail and needs to retry
		txCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		backOff := backoff.WithContext(backoff.BackOff(backoff.NewConstantBackOff(50*time.Millisecond)), txCtx)
		r, err := backoff.RetryWithData(func() (*types.Receipt, error) {
			return v.rpcApiClient.GetTransactionReceipt(txCtx, txn.Hash())
		}, backOff)
		cancel()

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
	var accErr error
	markMissing := func(inclusion OrderedInclusion) {
		if accErr == nil {
			accErr = errors.New("txns missing")
		}

		accErr = fmt.Errorf("%w: (%d,%s)", accErr, inclusion.TxnIndex, inclusion.TxnHash)
	}

	for _, inclusion := range inclusions {
		if inclusion.TxnIndex >= uint64(len(payload.Transactions)) {
			markMissing(inclusion)
			continue
		}

		txn, err := types.DecodeTransaction(payload.Transactions[inclusion.TxnIndex])
		if err != nil {
			return err
		}

		// fcu persistance is now asynchronous so this can get called
		// in the test loop before the tx data is coommited in which
		// case it will fail and needs to retry
		txCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		backOff := backoff.WithContext(backoff.BackOff(backoff.NewConstantBackOff(50*time.Millisecond)), txCtx)
		r, err := backoff.RetryWithData(func() (*types.Receipt, error) {
			return v.rpcApiClient.GetTransactionReceipt(txCtx, txn.Hash())
		}, backOff)
		cancel()
		if err != nil {
			return err
		}

		if r.Status != types.ReceiptStatusSuccessful {
			return fmt.Errorf("txn %d in block %d not successful", inclusion.TxnIndex, r.BlockNumber)
		}

		if txn.Hash() != inclusion.TxnHash {
			markMissing(inclusion)
		}
	}

	return accErr
}

type OrderedInclusion struct {
	TxnHash  common.Hash
	TxnIndex uint64
}
