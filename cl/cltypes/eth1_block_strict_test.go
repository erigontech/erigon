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
