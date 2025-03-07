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
	"errors"
	"fmt"

	mapset "github.com/deckarep/golang-set/v2"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/types"
	enginetypes "github.com/erigontech/erigon/turbo/engineapi/engine_types"
)

func VerifyTxnsInclusion(payload *enginetypes.ExecutionPayload, txns ...libcommon.Hash) error {
	txnHashes := mapset.NewSet[libcommon.Hash](txns...)
	for _, txnBytes := range payload.Transactions {
		txn, err := types.DecodeTransaction(txnBytes)
		if err != nil {
			return err
		}

		txnHashes.Remove(txn.Hash())
	}

	if txnHashes.Cardinality() == 0 {
		return nil
	}

	err := errors.New("txns not found in block")
	txnHashes.Each(func(txnHash libcommon.Hash) bool {
		err = fmt.Errorf("%w: %s", err, txnHash)
		return true // continue
	})
	return err
}
