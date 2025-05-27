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
	"encoding/binary"
	"errors"
	"io"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
)

type SlotData struct {
	// Block Header and Execution Headers can be retrieved from block snapshots
	Version clparams.StateVersion
	// Lengths
	ValidatorLength uint64
	Eth1DataLength  uint64
	// Phase0
	Eth1Data         *cltypes.Eth1Data
	Eth1DepositIndex uint64
	Fork             *cltypes.Fork
	// Capella
	NextWithdrawalIndex          uint64
	NextWithdrawalValidatorIndex uint64
	// Electra
	DepositRequestsStartIndex     uint64
	DepositBalanceToConsume       uint64
	ExitBalanceToConsume          uint64
	EarliestExitEpoch             uint64
	ConsolidationBalanceToConsume uint64
	EarliestConsolidationEpoch    uint64

	// BlockRewards for proposer
	AttestationsRewards  uint64
	SyncAggregateRewards uint64
	ProposerSlashings    uint64
	AttesterSlashings    uint64
}

func SlotDataFromBeaconState(s *state.CachingBeaconState) *SlotData {
	justificationCopy := &cltypes.JustificationBits{}
	jj := s.JustificationBits()
	copy(justificationCopy[:], jj[:])
	return &SlotData{
		ValidatorLength: uint64(s.ValidatorLength()),
		Eth1DataLength:  uint64(s.Eth1DataVotes().Len()),

		Version:                      s.Version(),
		Eth1Data:                     s.Eth1Data(),
		Eth1DepositIndex:             s.Eth1DepositIndex(),
		NextWithdrawalIndex:          s.NextWithdrawalIndex(),
		NextWithdrawalValidatorIndex: s.NextWithdrawalValidatorIndex(),
		// Electra
		DepositRequestsStartIndex:     s.DepositRequestsStartIndex(),
		DepositBalanceToConsume:       s.DepositBalanceToConsume(),
		ExitBalanceToConsume:          s.ExitBalanceToConsume(),
		EarliestExitEpoch:             s.EarliestExitEpoch(),
		ConsolidationBalanceToConsume: s.ConsolidationBalanceToConsume(),
		EarliestConsolidationEpoch:    s.EarliestConsolidationEpoch(),
		Fork:                          s.Fork(),
	}
}

// Serialize serializes the state into a byte slice with zstd compression.
func (m *SlotData) WriteTo(w io.Writer) error {
	buf, err := ssz2.MarshalSSZ(nil, m.getSchema()...)
	if err != nil {
		return err
	}
	lenB := make([]byte, 8)
	binary.BigEndian.PutUint64(lenB, uint64(len(buf)))
	if _, err = w.Write([]byte{byte(m.Version)}); err != nil {
		return err
	}
	if _, err = w.Write(lenB); err != nil {
		return err
	}
	_, err = w.Write(buf)
	if err != nil {
		return err
	}
	return nil
}

// Deserialize deserializes the state from a byte slice with zstd compression.
func (m *SlotData) ReadFrom(r io.Reader, cfg *clparams.BeaconChainConfig) error {
	m.Eth1Data = &cltypes.Eth1Data{}
	m.Fork = &cltypes.Fork{}
	var err error

	versionByte := make([]byte, 1)
	if _, err = r.Read(versionByte); err != nil {
		return err
	}
	m.Version = clparams.StateVersion(versionByte[0])

	lenB := make([]byte, 8)
	if _, err = r.Read(lenB); err != nil {
		return err
	}

	buf := make([]byte, binary.BigEndian.Uint64(lenB))
	var n int

	n, err = r.Read(buf)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		return err
	}
	if n != len(buf) {
		return io.ErrUnexpectedEOF
	}

	return ssz2.UnmarshalSSZ(buf, int(m.Version), m.getSchema()...)
}

func (m *SlotData) getSchema() []interface{} {
	schema := []interface{}{m.Eth1Data, m.Fork, &m.Eth1DepositIndex, &m.ValidatorLength, &m.Eth1DataLength, &m.AttestationsRewards, &m.SyncAggregateRewards, &m.ProposerSlashings, &m.AttesterSlashings}
	if m.Version >= clparams.CapellaVersion {
		schema = append(schema, &m.NextWithdrawalIndex, &m.NextWithdrawalValidatorIndex)
	}

	if m.Version >= clparams.ElectraVersion {
		schema = append(schema,
			&m.DepositRequestsStartIndex,
			&m.DepositBalanceToConsume,
			&m.ExitBalanceToConsume,
			&m.EarliestExitEpoch,
			&m.ConsolidationBalanceToConsume,
			&m.EarliestConsolidationEpoch,
		)
	}
	return schema
}
