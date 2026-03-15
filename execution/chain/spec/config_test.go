// Copyright 2017 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package chainspec

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
)

func TestCheckCompatible(t *testing.T) {
	type test struct {
		stored, new *chain.Config
		head        uint64
		wantErr     *chain.ConfigCompatError
	}
	tests := []test{
		{stored: chain.AllProtocolChanges, new: chain.AllProtocolChanges, head: 0, wantErr: nil},
		{stored: chain.AllProtocolChanges, new: chain.AllProtocolChanges, head: 100, wantErr: nil},
		{
			stored:  &chain.Config{TangerineWhistleBlock: common.NewUint64(10)},
			new:     &chain.Config{TangerineWhistleBlock: common.NewUint64(20)},
			head:    9,
			wantErr: nil,
		},
		{
			stored: chain.AllProtocolChanges,
			new:    &chain.Config{HomesteadBlock: nil},
			head:   3,
			wantErr: &chain.ConfigCompatError{
				What:         "Homestead fork block",
				StoredConfig: common.NewUint64(0),
				NewConfig:    nil,
				RewindTo:     0,
			},
		},
		{
			stored: chain.AllProtocolChanges,
			new:    &chain.Config{HomesteadBlock: common.NewUint64(1)},
			head:   3,
			wantErr: &chain.ConfigCompatError{
				What:         "Homestead fork block",
				StoredConfig: common.NewUint64(0),
				NewConfig:    common.NewUint64(1),
				RewindTo:     0,
			},
		},
		{
			stored: &chain.Config{HomesteadBlock: common.NewUint64(30), TangerineWhistleBlock: common.NewUint64(10)},
			new:    &chain.Config{HomesteadBlock: common.NewUint64(25), TangerineWhistleBlock: common.NewUint64(20)},
			head:   25,
			wantErr: &chain.ConfigCompatError{
				What:         "Tangerine Whistle fork block",
				StoredConfig: common.NewUint64(10),
				NewConfig:    common.NewUint64(20),
				RewindTo:     9,
			},
		},
		{
			stored:  &chain.Config{ConstantinopleBlock: common.NewUint64(30)},
			new:     &chain.Config{ConstantinopleBlock: common.NewUint64(30), PetersburgBlock: common.NewUint64(30)},
			head:    40,
			wantErr: nil,
		},
		{
			stored: &chain.Config{ConstantinopleBlock: common.NewUint64(30)},
			new:    &chain.Config{ConstantinopleBlock: common.NewUint64(30), PetersburgBlock: common.NewUint64(31)},
			head:   40,
			wantErr: &chain.ConfigCompatError{
				What:         "Petersburg fork block",
				StoredConfig: nil,
				NewConfig:    common.NewUint64(31),
				RewindTo:     30,
			},
		},
	}

	for _, test := range tests {
		err := test.stored.CheckCompatible(test.new, test.head)
		if !reflect.DeepEqual(err, test.wantErr) {
			t.Errorf("error mismatch:\nstored: %v\nnew: %v\nhead: %v\nerr: %v\nwant: %v", test.stored, test.new, test.head, err, test.wantErr)
		}
	}
}

func TestMainnetBlobSchedule(t *testing.T) {
	c := Mainnet.Config
	// Original EIP-4844 values
	time := *c.CancunTime
	assert.Equal(t, uint64(6), c.GetMaxBlobsPerBlock(time))
	assert.Equal(t, uint64(3), c.GetTargetBlobsPerBlock(time))
	assert.Equal(t, uint64(3338477), c.GetBlobGasPriceUpdateFraction(time))

	// EIP-7691: Blob throughput increase
	time = *c.PragueTime
	assert.Equal(t, uint64(9), c.GetMaxBlobsPerBlock(time))
	assert.Equal(t, uint64(6), c.GetTargetBlobsPerBlock(time))
	assert.Equal(t, uint64(5007716), c.GetBlobGasPriceUpdateFraction(time))
}

func TestGnosisBlobSchedule(t *testing.T) {
	c := Gnosis.Config

	// Cancun values
	time := *c.CancunTime
	assert.Equal(t, uint64(2), c.GetMaxBlobsPerBlock(time))
	assert.Equal(t, uint64(1), c.GetTargetBlobsPerBlock(time))
	assert.Equal(t, uint64(1112826), c.GetBlobGasPriceUpdateFraction(time))

	// should remain the same in Pectra for Gnosis
	assert.Equal(t, uint64(2), c.GetMaxBlobsPerBlock(time))
	assert.Equal(t, uint64(1), c.GetTargetBlobsPerBlock(time))
	assert.Equal(t, uint64(1112826), c.GetBlobGasPriceUpdateFraction(time))
}
