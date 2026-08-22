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

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
)

func TestCheckCompatible(t *testing.T) {
	type test struct {
		stored, new *chain.Config
		head        uint64
		headTime    uint64
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
		err := test.stored.CheckCompatible(test.new, test.head, test.headTime)
		if !reflect.DeepEqual(err, test.wantErr) {
			t.Errorf("error mismatch:\nstored: %v\nnew: %v\nhead: %v\nerr: %v\nwant: %v", test.stored, test.new, test.head, err, test.wantErr)
		}
	}
}

// Post-merge forks are scheduled by timestamp. Before this they were compared against
// nothing at all, so one could be rescheduled on a chain already past it and the node would
// run a different schedule from its peers with no error.
func TestCheckCompatibleTimestampForks(t *testing.T) {
	for _, tc := range []struct {
		name           string
		stored, newcfg *chain.Config
		headTime       uint64
		wantWhat       string
		wantRewind     uint64
	}{
		{
			name:     "not yet reached, may be rescheduled",
			stored:   &chain.Config{PragueTime: common.NewUint64(100)},
			newcfg:   &chain.Config{PragueTime: common.NewUint64(200)},
			headTime: 50,
		},
		{
			name:       "already past it, may not",
			stored:     &chain.Config{PragueTime: common.NewUint64(100)},
			newcfg:     &chain.Config{PragueTime: common.NewUint64(200)},
			headTime:   150,
			wantWhat:   "Prague fork timestamp",
			wantRewind: 99,
		},
		{
			name:       "unscheduling an active fork",
			stored:     &chain.Config{AmsterdamTime: common.NewUint64(100)},
			newcfg:     &chain.Config{},
			headTime:   150,
			wantWhat:   "Amsterdam fork timestamp",
			wantRewind: 99,
		},
		{
			name:       "stored at genesis time, rescheduled, head past it",
			stored:     &chain.Config{PragueTime: common.NewUint64(0)},
			newcfg:     &chain.Config{PragueTime: common.NewUint64(200)},
			headTime:   150,
			wantWhat:   "Prague fork timestamp",
			wantRewind: 0,
		},
		{
			name:       "stored at time 1, rescheduled, head past it",
			stored:     &chain.Config{PragueTime: common.NewUint64(1)},
			newcfg:     &chain.Config{PragueTime: common.NewUint64(200)},
			headTime:   150,
			wantWhat:   "Prague fork timestamp",
			wantRewind: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.stored.CheckCompatible(tc.newcfg, 0, tc.headTime)
			if tc.wantWhat == "" {
				require.Nil(t, err)
				return
			}
			require.NotNil(t, err, "a fork the chain is already past must not be reschedulable")
			require.Equal(t, tc.wantWhat, err.What)
			require.True(t, err.IsTimestampFork(), "a zero RewindToTime must not be mistaken for no conflict")
			require.Equal(t, tc.wantRewind, err.RewindToTime)
			require.Zero(t, err.RewindTo, "a timestamp fork rewinds by time, not by block")
		})
	}
}

func TestCheckCompatibleBpoAndBalancerTimestamps(t *testing.T) {
	for _, tc := range []struct {
		name           string
		stored, newcfg *chain.Config
		wantWhat       string
	}{
		{name: "BPO1", stored: &chain.Config{Bpo1Time: common.NewUint64(100)}, newcfg: &chain.Config{Bpo1Time: common.NewUint64(200)}, wantWhat: "BPO1 fork timestamp"},
		{name: "BPO2", stored: &chain.Config{Bpo2Time: common.NewUint64(100)}, newcfg: &chain.Config{Bpo2Time: common.NewUint64(200)}, wantWhat: "BPO2 fork timestamp"},
		{name: "BPO3", stored: &chain.Config{Bpo3Time: common.NewUint64(100)}, newcfg: &chain.Config{Bpo3Time: common.NewUint64(200)}, wantWhat: "BPO3 fork timestamp"},
		{name: "BPO4", stored: &chain.Config{Bpo4Time: common.NewUint64(100)}, newcfg: &chain.Config{Bpo4Time: common.NewUint64(200)}, wantWhat: "BPO4 fork timestamp"},
		{name: "BPO5", stored: &chain.Config{Bpo5Time: common.NewUint64(100)}, newcfg: &chain.Config{Bpo5Time: common.NewUint64(200)}, wantWhat: "BPO5 fork timestamp"},
		{name: "Balancer", stored: &chain.Config{BalancerTime: common.NewUint64(100)}, newcfg: &chain.Config{BalancerTime: common.NewUint64(200)}, wantWhat: "Balancer fork timestamp"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.stored.CheckCompatible(tc.newcfg, 0, 150)
			require.NotNil(t, err)
			require.Equal(t, tc.wantWhat, err.What)
			require.True(t, err.IsTimestampFork())
		})
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
