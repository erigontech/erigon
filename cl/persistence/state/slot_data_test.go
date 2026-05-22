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

package state_accessors

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clparams/initial_state"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

func TestSlotData(t *testing.T) {
	s, err := initial_state.GetGenesisState(chainspec.MainnetChainID)
	require.NoError(t, err)
	m := &SlotData{
		Version:                       clparams.ElectraVersion,
		Eth1Data:                      &cltypes.Eth1Data{},
		Eth1DepositIndex:              1,
		NextWithdrawalIndex:           2,
		NextWithdrawalValidatorIndex:  3,
		DepositRequestsStartIndex:     4,
		DepositBalanceToConsume:       5,
		ExitBalanceToConsume:          6,
		EarliestExitEpoch:             7,
		ConsolidationBalanceToConsume: 8,
		EarliestConsolidationEpoch:    9,
		Fork:                          &cltypes.Fork{Epoch: 12},
	}
	var b bytes.Buffer
	if err := m.WriteTo(&b); err != nil {
		t.Fatal(err)
	}
	m2 := &SlotData{}
	if err := m2.ReadFrom(&b, s.BeaconConfig()); err != nil {
		t.Fatal(err)
	}

	require.Equal(t, m, m2)
}

func TestSlotDataGloas(t *testing.T) {
	s, err := initial_state.GetGenesisState(chainspec.MainnetChainID)
	require.NoError(t, err)
	m := &SlotData{
		Version:                       clparams.GloasVersion,
		Eth1Data:                      &cltypes.Eth1Data{},
		Eth1DepositIndex:              1,
		NextWithdrawalIndex:           2,
		NextWithdrawalValidatorIndex:  3,
		DepositRequestsStartIndex:     4,
		DepositBalanceToConsume:       5,
		ExitBalanceToConsume:          6,
		EarliestExitEpoch:             7,
		ConsolidationBalanceToConsume: 8,
		EarliestConsolidationEpoch:    9,
		NextWithdrawalBuilderIndex:    42,
		LatestBlockHash:               common.HexToHash("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"),
		Fork:                          &cltypes.Fork{Epoch: 12},
	}
	var b bytes.Buffer
	require.NoError(t, m.WriteTo(&b))

	m2 := &SlotData{}
	require.NoError(t, m2.ReadFrom(&b, s.BeaconConfig()))

	require.Equal(t, m, m2)
}

func TestSlotDataGloasDefaultHash(t *testing.T) {
	s, err := initial_state.GetGenesisState(chainspec.MainnetChainID)
	require.NoError(t, err)
	// Test with zero-value hash to ensure it round-trips correctly
	m := &SlotData{
		Version:                       clparams.GloasVersion,
		Eth1Data:                      &cltypes.Eth1Data{},
		Eth1DepositIndex:              0,
		NextWithdrawalIndex:           0,
		NextWithdrawalValidatorIndex:  0,
		DepositRequestsStartIndex:     0,
		DepositBalanceToConsume:       0,
		ExitBalanceToConsume:          0,
		EarliestExitEpoch:             0,
		ConsolidationBalanceToConsume: 0,
		EarliestConsolidationEpoch:    0,
		NextWithdrawalBuilderIndex:    0,
		LatestBlockHash:               common.Hash{},
		Fork:                          &cltypes.Fork{},
	}
	var b bytes.Buffer
	require.NoError(t, m.WriteTo(&b))

	m2 := &SlotData{}
	require.NoError(t, m2.ReadFrom(&b, s.BeaconConfig()))

	require.Equal(t, m, m2)
}

// TestSlotDataGloasAllFieldsNonZero verifies that every GLOAS field round-trips
// correctly when all fields carry non-trivial values (including Eth1Data with
// non-zero sub-fields and large uint64 values).
func TestSlotDataGloasAllFieldsNonZero(t *testing.T) {
	s, err := initial_state.GetGenesisState(chainspec.MainnetChainID)
	require.NoError(t, err)

	m := &SlotData{
		Version: clparams.GloasVersion,
		Eth1Data: &cltypes.Eth1Data{
			Root:         common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111"),
			DepositCount: 999,
			BlockHash:    common.HexToHash("0x2222222222222222222222222222222222222222222222222222222222222222"),
		},
		Eth1DepositIndex:              100,
		ValidatorLength:               200,
		Eth1DataLength:                300,
		NextWithdrawalIndex:           400,
		NextWithdrawalValidatorIndex:  500,
		DepositRequestsStartIndex:     600,
		DepositBalanceToConsume:       700,
		ExitBalanceToConsume:          800,
		EarliestExitEpoch:             900,
		ConsolidationBalanceToConsume: 1000,
		EarliestConsolidationEpoch:    1100,
		NextWithdrawalBuilderIndex:    ^uint64(0), // max uint64
		LatestBlockHash:               common.HexToHash("0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"),
		AttestationsRewards:           1200,
		SyncAggregateRewards:          1300,
		ProposerSlashings:             1400,
		AttesterSlashings:             1500,
		Fork: &cltypes.Fork{
			PreviousVersion: [4]byte{0x01, 0x02, 0x03, 0x04},
			CurrentVersion:  [4]byte{0x05, 0x06, 0x07, 0x08},
			Epoch:           9999,
		},
	}
	var b bytes.Buffer
	require.NoError(t, m.WriteTo(&b))

	m2 := &SlotData{}
	require.NoError(t, m2.ReadFrom(&b, s.BeaconConfig()))

	require.Equal(t, m, m2)
}

// TestSlotDataPreGloasDoesNotIncludeGloasFields verifies that when we serialize
// a pre-GLOAS SlotData (Electra or Fulu), even if the GLOAS fields happen to be
// populated on the struct, they are NOT included in the serialized form.
// After deserialization, the GLOAS fields must be zero-valued.
func TestSlotDataPreGloasDoesNotIncludeGloasFields(t *testing.T) {
	s, err := initial_state.GetGenesisState(chainspec.MainnetChainID)
	require.NoError(t, err)

	for _, version := range []clparams.StateVersion{
		clparams.ElectraVersion,
		clparams.FuluVersion,
	} {
		t.Run(version.String(), func(t *testing.T) {
			m := &SlotData{
				Version:                       version,
				Eth1Data:                      &cltypes.Eth1Data{},
				Eth1DepositIndex:              1,
				NextWithdrawalIndex:           2,
				NextWithdrawalValidatorIndex:  3,
				DepositRequestsStartIndex:     4,
				DepositBalanceToConsume:       5,
				ExitBalanceToConsume:          6,
				EarliestExitEpoch:             7,
				ConsolidationBalanceToConsume: 8,
				EarliestConsolidationEpoch:    9,
				// These GLOAS fields are set on the struct but should NOT be serialized
				// for pre-GLOAS versions.
				NextWithdrawalBuilderIndex: 42,
				LatestBlockHash:            common.HexToHash("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"),
				Fork:                       &cltypes.Fork{Epoch: 12},
			}
			var b bytes.Buffer
			require.NoError(t, m.WriteTo(&b))

			m2 := &SlotData{}
			require.NoError(t, m2.ReadFrom(&b, s.BeaconConfig()))

			// GLOAS fields should be zero after round-trip through a pre-GLOAS version.
			require.Equal(t, uint64(0), m2.NextWithdrawalBuilderIndex)
			require.Equal(t, common.Hash{}, m2.LatestBlockHash)

			// Non-GLOAS fields must survive the round-trip.
			require.Equal(t, version, m2.Version)
			require.Equal(t, uint64(1), m2.Eth1DepositIndex)
			require.Equal(t, uint64(2), m2.NextWithdrawalIndex)
			require.Equal(t, uint64(3), m2.NextWithdrawalValidatorIndex)
			require.Equal(t, uint64(4), m2.DepositRequestsStartIndex)
			require.Equal(t, uint64(9), m2.EarliestConsolidationEpoch)
		})
	}
}

// TestSlotDataGloasEncodesMoreBytesThanElectra verifies that the GLOAS version
// produces a strictly larger serialized payload than Electra (because of the
// extra NextWithdrawalBuilderIndex and LatestBlockHash fields).
func TestSlotDataGloasEncodesMoreBytesThanElectra(t *testing.T) {
	base := func(v clparams.StateVersion) *SlotData {
		return &SlotData{
			Version:                       v,
			Eth1Data:                      &cltypes.Eth1Data{},
			Eth1DepositIndex:              1,
			NextWithdrawalIndex:           2,
			NextWithdrawalValidatorIndex:  3,
			DepositRequestsStartIndex:     4,
			DepositBalanceToConsume:       5,
			ExitBalanceToConsume:          6,
			EarliestExitEpoch:             7,
			ConsolidationBalanceToConsume: 8,
			EarliestConsolidationEpoch:    9,
			NextWithdrawalBuilderIndex:    42,
			LatestBlockHash:               common.HexToHash("0xaa"),
			Fork:                          &cltypes.Fork{Epoch: 12},
		}
	}

	var electraBuf, gloasBuf bytes.Buffer
	require.NoError(t, base(clparams.ElectraVersion).WriteTo(&electraBuf))
	require.NoError(t, base(clparams.GloasVersion).WriteTo(&gloasBuf))

	// GLOAS adds uint64 (8 bytes) + Hash (32 bytes) = 40 bytes more
	require.Greater(t, gloasBuf.Len(), electraBuf.Len(),
		"GLOAS SlotData should be larger than Electra due to extra fields")
	require.Equal(t, 40, gloasBuf.Len()-electraBuf.Len(),
		"difference should be exactly 40 bytes (8 for NextWithdrawalBuilderIndex + 32 for LatestBlockHash)")
}

// TestSlotDataReadSlotDataRoundTrip tests the full ReadSlotData path using a
// mock GetValFn that simulates reading from the kv.SlotData table. This tests
// the integration between WriteTo, ReadSlotData, and the SlotData DB encoding.
func TestSlotDataReadSlotDataRoundTrip(t *testing.T) {
	s, err := initial_state.GetGenesisState(chainspec.MainnetChainID)
	require.NoError(t, err)

	testCases := []struct {
		name string
		sd   *SlotData
	}{
		{
			name: "Electra",
			sd: &SlotData{
				Version:                       clparams.ElectraVersion,
				Eth1Data:                      &cltypes.Eth1Data{},
				Eth1DepositIndex:              10,
				NextWithdrawalIndex:           20,
				NextWithdrawalValidatorIndex:  30,
				DepositRequestsStartIndex:     40,
				DepositBalanceToConsume:       50,
				ExitBalanceToConsume:          60,
				EarliestExitEpoch:             70,
				ConsolidationBalanceToConsume: 80,
				EarliestConsolidationEpoch:    90,
				Fork:                          &cltypes.Fork{Epoch: 1},
			},
		},
		{
			name: "GLOAS",
			sd: &SlotData{
				Version:                       clparams.GloasVersion,
				Eth1Data:                      &cltypes.Eth1Data{},
				Eth1DepositIndex:              10,
				NextWithdrawalIndex:           20,
				NextWithdrawalValidatorIndex:  30,
				DepositRequestsStartIndex:     40,
				DepositBalanceToConsume:       50,
				ExitBalanceToConsume:          60,
				EarliestExitEpoch:             70,
				ConsolidationBalanceToConsume: 80,
				EarliestConsolidationEpoch:    90,
				NextWithdrawalBuilderIndex:    123456,
				LatestBlockHash:               common.HexToHash("0xabcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"),
				Fork:                          &cltypes.Fork{Epoch: 2},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			require.NoError(t, tc.sd.WriteTo(&buf))

			slot := uint64(1000)
			key := base_encoding.Encode64ToBytes4(slot)
			serialized := buf.Bytes()

			// Build a mock GetValFn that returns data for kv.SlotData table only.
			mockGetVal := func(table string, k []byte) ([]byte, error) {
				if table == kv.SlotData && bytes.Equal(k, key) {
					return serialized, nil
				}
				return nil, nil
			}

			sd, err := ReadSlotData(mockGetVal, slot, s.BeaconConfig())
			require.NoError(t, err)
			require.NotNil(t, sd)
			require.Equal(t, tc.sd, sd)
		})
	}
}

// TestSlotDataReadSlotDataPreGloasReturnsZeroGloasFields reads an Electra SlotData
// via ReadSlotData and confirms the GLOAS fields are zero.
func TestSlotDataReadSlotDataPreGloasReturnsZeroGloasFields(t *testing.T) {
	s, err := initial_state.GetGenesisState(chainspec.MainnetChainID)
	require.NoError(t, err)

	m := &SlotData{
		Version:                       clparams.ElectraVersion,
		Eth1Data:                      &cltypes.Eth1Data{},
		Eth1DepositIndex:              1,
		NextWithdrawalIndex:           2,
		NextWithdrawalValidatorIndex:  3,
		DepositRequestsStartIndex:     4,
		DepositBalanceToConsume:       5,
		ExitBalanceToConsume:          6,
		EarliestExitEpoch:             7,
		ConsolidationBalanceToConsume: 8,
		EarliestConsolidationEpoch:    9,
		Fork:                          &cltypes.Fork{Epoch: 12},
	}
	var buf bytes.Buffer
	require.NoError(t, m.WriteTo(&buf))

	slot := uint64(500)
	key := base_encoding.Encode64ToBytes4(slot)
	serialized := buf.Bytes()

	mockGetVal := func(table string, k []byte) ([]byte, error) {
		if table == kv.SlotData && bytes.Equal(k, key) {
			return serialized, nil
		}
		return nil, nil
	}

	sd, err := ReadSlotData(mockGetVal, slot, s.BeaconConfig())
	require.NoError(t, err)
	require.NotNil(t, sd)

	// Pre-GLOAS: GLOAS fields must be zero
	require.Equal(t, uint64(0), sd.NextWithdrawalBuilderIndex)
	require.Equal(t, common.Hash{}, sd.LatestBlockHash)

	// But pre-GLOAS fields survived
	require.Equal(t, clparams.ElectraVersion, sd.Version)
	require.Equal(t, uint64(1), sd.Eth1DepositIndex)
	require.Equal(t, uint64(9), sd.EarliestConsolidationEpoch)
}
