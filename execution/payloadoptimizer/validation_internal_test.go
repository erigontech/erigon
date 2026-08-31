// Copyright 2026 The Erigon Authors
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

package payloadoptimizer

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/execmodule"
	protocolparams "github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
)

func TestValidateCandidateIndependentlyEnforcesConsensusBounds(t *testing.T) {
	tests := []struct {
		name      string
		gasLimit  uint64
		extra     []byte
		wantField string
	}{
		{name: "gas below minimum", gasLimit: protocolparams.MinBlockGasLimit - 1, wantField: "gas limit bounds"},
		{name: "gas above maximum", gasLimit: protocolparams.MaxBlockGasLimit + 1, wantField: "gas limit bounds"},
		{name: "extra above maximum", gasLimit: 30_000_000, extra: make([]byte, protocolparams.MaximumExtraDataSize+1), wantField: "extra data bounds"},
		{name: "minimum gas and empty extra", gasLimit: protocolparams.MinBlockGasLimit},
		{name: "maximum gas and maximum extra", gasLimit: protocolparams.MaxBlockGasLimit, extra: make([]byte, protocolparams.MaximumExtraDataSize)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			buildContext, result := internallyMalformedCandidate(test.gasLimit, test.extra)
			err := validateCandidate(buildContext, result)
			if test.wantField == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, ErrCandidateContextMismatch)
			require.ErrorContains(t, err, test.wantField)
		})
	}
}

func internallyMalformedCandidate(gasLimit uint64, extra []byte) (BuildContext, execmodule.AssembledBlockResult) {
	maxBlobs := uint64(0)
	withdrawals := make([]*types.Withdrawal, 0)
	params := &builder.Parameters{
		Withdrawals:      withdrawals,
		ExtraData:        extra,
		MaxBlobsPerBlock: &maxBlobs,
	}
	requestsHash := types.FlatRequests(nil).Hash()
	header := &types.Header{
		Number:       *uint256.NewInt(1),
		GasLimit:     gasLimit,
		Extra:        extra,
		RequestsHash: requestsHash,
	}
	block := types.NewBlock(header, nil, nil, nil, withdrawals, nil)
	return BuildContext{params: params, stateVersion: clparams.ElectraVersion, parentGasLimit: gasLimit, targetGasLimit: gasLimit}, execmodule.AssembledBlockResult{
		Block:      &types.BlockWithReceipts{Block: block},
		BlockValue: uint256.NewInt(0),
	}
}
