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
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
)

// configSSZ keeps configuration attached when the generic container decoder
// dispatches to a nested object, including recursive strict decoding.
type configSSZ struct {
	ssz2.SizedObjectSSZ
	decode func([]byte, int, bool) error
}

func (c configSSZ) DecodeSSZ(buf []byte, version int) error { return c.decode(buf, version, false) }
func (c configSSZ) DecodeSSZStrict(buf []byte, version int) error {
	return c.decode(buf, version, true)
}

func withSSZConfig(i *IndexedAttestation, cfg *clparams.BeaconChainConfig) configSSZ {
	return configSSZ{i, func(buf []byte, version int, strict bool) error {
		return i.decodeSSZWithConfig(buf, version, cfg, strict)
	}}
}

func slashingDecodeSchema(schema []any, cfg *clparams.BeaconChainConfig) []any {
	for n, field := range schema {
		if list, ok := field.(*solid.ListSSZ[*AttesterSlashing]); ok {
			schema[n] = configSSZ{list, func(buf []byte, version int, strict bool) error {
				return list.DecodeDynamicSSZWithDecoder(buf, version, strict, func(a *AttesterSlashing, buf []byte, version int) error {
					return a.decodeSSZWithConfig(buf, version, cfg, strict)
				})
			}}
		}
	}
	return schema
}
