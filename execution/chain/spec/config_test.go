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

package chainspec_test

import (
	"reflect"
	"testing"

	chainspec "github.com/erigontech/erigon/execution/chain/spec"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
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
				// AllProtocolChanges schedules Shanghai at 0, and the new config
				// unschedules it, so the timestamp axis conflicts at headTime 0 too.
				WhatTime:     "Shanghai fork timestamp",
				StoredTime:   common.NewUint64(0),
				NewTime:      nil,
				RewindToTime: 0,
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
				// AllProtocolChanges schedules Shanghai at 0, and the new config
				// unschedules it, so the timestamp axis conflicts at headTime 0 too.
				WhatTime:     "Shanghai fork timestamp",
				StoredTime:   common.NewUint64(0),
				NewTime:      nil,
				RewindToTime: 0,
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

// Post-merge forks are scheduled by timestamp, so they compare against the head's
// time rather than its number.
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
			require.Equal(t, tc.wantWhat, err.WhatTime)
			require.True(t, err.HasTimestampConflict(), "a zero RewindToTime must not be mistaken for no conflict")
			require.False(t, err.HasBlockConflict(), "no block fork conflicts in this case")
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
			require.Equal(t, tc.wantWhat, err.WhatTime)
			require.True(t, err.HasTimestampConflict())
		})
	}
}

// A block conflict the chain cannot rewind past must not hide a timestamp one behind
// it: checkCompatibleBlocks returns at its first conflict, and every modern chain has
// SpuriousDragon at block 0, so "EIP155 chain ID" alone would mask the whole axis.
func TestCheckCompatibleBlockConflictDoesNotMaskTimestamp(t *testing.T) {
	for _, tc := range []struct {
		name             string
		stored, newcfg   *chain.Config
		head, headTime   uint64
		wantWhat         string
		wantWhatTime     string
		wantRewind       uint64
		wantRewindToTime uint64
	}{
		{
			name:             "zero-rewind block conflict alongside a timestamp one",
			stored:           &chain.Config{HomesteadBlock: common.NewUint64(0), PragueTime: common.NewUint64(100)},
			newcfg:           &chain.Config{PragueTime: common.NewUint64(900)},
			head:             50,
			headTime:         150,
			wantWhat:         "Homestead fork block",
			wantWhatTime:     "Prague fork timestamp",
			wantRewind:       0,
			wantRewindToTime: 99,
		},
		{
			name:             "both axes conflict, both rewind targets survive",
			stored:           &chain.Config{HomesteadBlock: common.NewUint64(10), PragueTime: common.NewUint64(100)},
			newcfg:           &chain.Config{HomesteadBlock: common.NewUint64(20), PragueTime: common.NewUint64(900)},
			head:             50,
			headTime:         150,
			wantWhat:         "Homestead fork block",
			wantWhatTime:     "Prague fork timestamp",
			wantRewind:       9,
			wantRewindToTime: 99,
		},
		{
			name:         "a chain ID change must not swallow the timestamp axis",
			stored:       &chain.Config{ChainID: uint256.NewInt(1), SpuriousDragonBlock: common.NewUint64(0), PragueTime: common.NewUint64(100)},
			newcfg:       &chain.Config{ChainID: uint256.NewInt(2), SpuriousDragonBlock: common.NewUint64(0), PragueTime: common.NewUint64(900)},
			head:         50,
			headTime:     150,
			wantWhat:     "EIP155 chain ID",
			wantWhatTime: "Prague fork timestamp",
			// SpuriousDragon sits at block 0, so there is nothing to rewind to.
			wantRewind:       0,
			wantRewindToTime: 99,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.stored.CheckCompatible(tc.newcfg, tc.head, tc.headTime)
			require.NotNil(t, err)
			require.Equal(t, tc.wantWhat, err.What)
			require.Equal(t, tc.wantWhatTime, err.WhatTime)
			require.True(t, err.HasBlockConflict())
			require.True(t, err.HasTimestampConflict(),
				"the timestamp conflict must survive the block one, or the caller writes the moved schedule")
			require.Equal(t, tc.wantRewind, err.RewindTo)
			require.Equal(t, tc.wantRewindToTime, err.RewindToTime)
			require.Contains(t, err.Error(), tc.wantWhat)
			require.Contains(t, err.Error(), tc.wantWhatTime)
		})
	}
}

// CheckConfigForkOrder walked the block forks only, so a schedule that runs backwards
// in time was accepted and committed.
func TestCheckConfigForkOrderTimestamps(t *testing.T) {
	require.NoError(t, (&chain.Config{
		ShanghaiTime: common.NewUint64(100),
		CancunTime:   common.NewUint64(200),
		PragueTime:   common.NewUint64(200),
	}).CheckConfigForkOrder(), "equal and ascending timestamps are both fine")

	require.NoError(t, (&chain.Config{
		ShanghaiTime: common.NewUint64(100),
		PragueTime:   common.NewUint64(200),
	}).CheckConfigForkOrder(), "an unscheduled fork in the middle is not a gap")

	err := (&chain.Config{
		ShanghaiTime: common.NewUint64(200),
		CancunTime:   common.NewUint64(100),
	}).CheckConfigForkOrder()
	require.Error(t, err, "cancun cannot activate before shanghai")
	require.Contains(t, err.Error(), "shanghaiTime")
	require.Contains(t, err.Error(), "cancunTime")
}

// Copy must not turn a nil slice into an empty one: newValidatorSetFromJson tests
// List before Multi, so an empty non-nil List reads as a validator set with no
// validators at all, at every nesting depth of the Aura config.
func TestConfigCopyKeepsAuraValidators(t *testing.T) {
	for _, name := range []string{networkname.Gnosis, networkname.Chiado} {
		spec, err := chainspec.ChainSpecByName(name)
		require.NoError(t, err)
		require.NotNil(t, spec.Config.Aura, "chain %s", name)

		src := spec.Config.Aura.Validators
		dst := spec.Config.Copy().Aura.Validators
		require.Equal(t, src.List == nil, dst.List == nil, "chain %s: top-level List nil-ness", name)
		require.Len(t, dst.Multi, len(src.Multi), "chain %s", name)
		for block, want := range src.Multi {
			got := dst.Multi[block]
			require.NotNil(t, got, "chain %s: multi[%d] lost", name, block)
			require.Equal(t, want.List == nil, got.List == nil,
				"chain %s: multi[%d] List nil-ness", name, block)
		}
	}
}

// Copy exists so applyOverrides cannot write through into a shared config.
func TestConfigCopyIsolatesReassignment(t *testing.T) {
	src := &chain.Config{OsakaTime: common.NewUint64(1)}
	cp := src.Copy()
	cp.OsakaTime = common.NewUint64(500)
	require.Equal(t, uint64(1), *src.OsakaTime)
	require.Equal(t, uint64(500), *cp.OsakaTime)
}

// Every shipped chainspec must satisfy the ordering check the previous test added.
func TestRegisteredChainSpecsForkOrder(t *testing.T) {
	for _, name := range []string{
		networkname.Mainnet, networkname.Sepolia, networkname.Hoodi,
		networkname.Gnosis, networkname.Chiado, networkname.Test, networkname.Bloatnet,
	} {
		spec, err := chainspec.ChainSpecByName(name)
		require.NoError(t, err)
		require.NoError(t, spec.Config.CheckConfigForkOrder(), "chain %s", name)
	}
}

func TestMainnetBlobSchedule(t *testing.T) {
	c := chainspec.Mainnet.Config
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
	c := chainspec.Gnosis.Config

	// Cancun values
	time := *c.CancunTime
	assert.Equal(t, uint64(2), c.GetMaxBlobsPerBlock(time))
	assert.Equal(t, uint64(1), c.GetTargetBlobsPerBlock(time))
	assert.Equal(t, uint64(1112826), c.GetBlobGasPriceUpdateFraction(time))

	// should remain the same in Pectra for chainspec.Gnosis
	assert.Equal(t, uint64(2), c.GetMaxBlobsPerBlock(time))
	assert.Equal(t, uint64(1), c.GetTargetBlobsPerBlock(time))
	assert.Equal(t, uint64(1112826), c.GetBlobGasPriceUpdateFraction(time))
}
