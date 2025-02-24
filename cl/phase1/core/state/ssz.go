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

package state

import (
	"github.com/erigontech/erigon-lib/metrics"
	"github.com/erigontech/erigon-lib/types/clonable"
)

func (b *CachingBeaconState) EncodeSSZ(buf []byte) ([]byte, error) {
	bts, err := b.BeaconState.EncodeSSZ(buf)
	if err != nil {
		return nil, err
	}
	sz := metrics.NewHistTimer("encode_ssz_beacon_state_size")
	sz.Observe(float64(len(bts)))
	return bts, nil
}

func (b *CachingBeaconState) DecodeSSZ(buf []byte, version int) error {
	if err := b.BeaconState.DecodeSSZ(buf, version); err != nil {
		return err
	}
	sz := metrics.NewHistTimer("decode_ssz_beacon_state_size")
	sz.Observe(float64(len(buf)))
	return b.InitBeaconState()
}

// SSZ size of the Beacon State
func (b *CachingBeaconState) EncodingSizeSSZ() (size int) {
	sz := b.BeaconState.EncodingSizeSSZ()
	return sz
}

func (b *CachingBeaconState) Clone() clonable.Clonable {
	return New(b.BeaconConfig())
}
