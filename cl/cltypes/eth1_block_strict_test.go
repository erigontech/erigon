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

package cltypes

import (
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	commonssz "github.com/erigontech/erigon/common/ssz"
	"github.com/stretchr/testify/require"
)

func eth1StrictDecodeFallbackAllowed(obj commonssz.Unmarshaler) bool {
	switch obj.(type) {
	case *solid.TransactionsSSZ, *solid.ByteListSSZ, *Withdrawal:
		return true
	default:
		return false
	}
}

func TestEth1BlockStrictSchemaCoverage(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	for version := clparams.BellatrixVersion; version <= clparams.GloasVersion; version++ {
		t.Run(version.String(), func(t *testing.T) {
			block := NewEth1Block(version, &cfg)
			schema := block.getSchema()
			if version >= clparams.CapellaVersion {
				schema = append(schema, (*Withdrawal)(nil))
			}
			for _, field := range schema {
				obj, ok := field.(commonssz.Unmarshaler)
				if !ok {
					continue
				}
				if _, ok := obj.(commonssz.StrictUnmarshaler); ok {
					continue
				}
				require.Truef(
					t,
					eth1StrictDecodeFallbackAllowed(obj),
					"%T must implement StrictUnmarshaler or be explicitly allowed",
					obj,
				)
			}
		})
	}
}

func TestEth1BlockTransactionLimitsChangeAtGloas(t *testing.T) {
	decoderCfg := clparams.MainnetBeaconConfig
	decoderCfg.MaxTransactionsPerPayload = 1
	for _, test := range []struct {
		version   clparams.StateVersion
		wantError bool
	}{
		{version: clparams.DenebVersion, wantError: true},
		{version: clparams.GloasVersion},
	} {
		t.Run(test.version.String(), func(t *testing.T) {
			block := NewEth1Block(test.version, &clparams.MainnetBeaconConfig)
			block.Extra = solid.NewExtraData()
			block.Transactions = solid.NewTransactionsSSZFromTransactions([][]byte{{1}, {2}})
			block.Withdrawals = solid.NewStaticListSSZ[*Withdrawal](int(clparams.MainnetBeaconConfig.MaxWithdrawalsPerPayload), 44)
			encoded, err := block.EncodeSSZ(nil)
			require.NoError(t, err)

			decoded := NewEth1Block(test.version, &decoderCfg)
			err = decoded.DecodeSSZStrict(encoded, int(test.version))
			if test.wantError {
				require.ErrorContains(t, err, "expected at most 1 transactions")
				return
			}
			require.NoError(t, err)
			require.Len(t, decoded.Transactions.UnderlyngReference(), 2)
		})
	}
}
