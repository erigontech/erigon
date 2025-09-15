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

package executiontests

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/state/contracts"
	"github.com/erigontech/erigon/execution/abi/bind"
	"github.com/erigontech/erigon/execution/chain/params"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
)

func TestEngineApiInvalidPayloadThenValidCanonicalFcuWithPayloadShouldSucceed(t *testing.T) {
	eat := DefaultEngineApiTester(t)
	eat.Run(t, func(ctx context.Context, t *testing.T, eat EngineApiTester) {
		// deploy changer at b2
		transactOpts, err := bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, eat.ChainId())
		require.NoError(t, err)
		transactOpts.GasLimit = params.MaxTxnGasLimit
		_, txn, changer, err := contracts.DeployChanger(transactOpts, eat.ContractBackend)
		require.NoError(t, err)
		b2Canon, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, b2Canon.ExecutionPayload, txn.Hash())
		require.NoError(t, err)
		// change changer at b3
		txn, err = changer.Change(transactOpts)
		require.NoError(t, err)
		b3Canon, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, b3Canon.ExecutionPayload, txn.Hash())
		require.NoError(t, err)
		// create an invalid fork at b3
		b3Faulty := TamperMockClPayloadStateRoot(b3Canon, common.HexToHash("0xb3f"))
		status, err := eat.MockCl.InsertNewPayload(ctx, b3Faulty)
		require.NoError(t, err)
		require.Equal(t, enginetypes.InvalidStatus, status.Status)
		require.True(t, strings.Contains(status.ValidationError.Error().Error(), "wrong trie root"))
		// build b4 on the canonical chain
		txn, err = changer.Change(transactOpts)
		require.NoError(t, err)
		b4Canon, err := eat.MockCl.BuildCanonicalBlock(ctx)
		require.NoError(t, err)
		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, b4Canon.ExecutionPayload, txn.Hash())
		require.NoError(t, err)
	})
}
