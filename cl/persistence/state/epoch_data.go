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
	"io"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
)

// EpochData stores the data for the epoch (valid throughout the epoch)
type EpochData struct {
	TotalActiveBalance          uint64
	JustificationBits           *cltypes.JustificationBits
	CurrentJustifiedCheckpoint  solid.Checkpoint
	PreviousJustifiedCheckpoint solid.Checkpoint
	FinalizedCheckpoint         solid.Checkpoint
	HistoricalSummariesLength   uint64
	HistoricalRootsLength       uint64
	ProposerLookahead           solid.Uint64VectorSSZ

	BeaconConfig *clparams.BeaconChainConfig // Used to determine the version of the state
	Version      clparams.StateVersion
}

func EpochDataFromBeaconState(s *state.CachingBeaconState) *EpochData {
	justificationCopy := &cltypes.JustificationBits{}
	jj := s.JustificationBits()
	copy(justificationCopy[:], jj[:])
	totalBalance, err := state.GetTotalBalance(s, s.GetActiveValidatorsIndices(state.Epoch(s)))
	if err != nil {
		return nil
	}
	return &EpochData{
		JustificationBits:           justificationCopy,
		TotalActiveBalance:          totalBalance,
		CurrentJustifiedCheckpoint:  s.CurrentJustifiedCheckpoint(),
		PreviousJustifiedCheckpoint: s.PreviousJustifiedCheckpoint(),
		FinalizedCheckpoint:         s.FinalizedCheckpoint(),
		HistoricalSummariesLength:   s.HistoricalSummariesLength(),
		HistoricalRootsLength:       s.HistoricalRootsLength(),
		ProposerLookahead:           s.GetProposerLookahead(),

		BeaconConfig: s.BeaconConfig(),
		Version:      s.Version(),
	}
}

// Serialize serializes the state into a byte slice with zstd compression.
func (m *EpochData) WriteTo(w io.Writer) error {
	buf, err := ssz2.MarshalSSZ(nil, m.getSchema()...)
	if err != nil {
		return err
	}
	lenB := make([]byte, 8)
	binary.LittleEndian.PutUint64(lenB, uint64(len(buf)))
	if _, err := w.Write(lenB); err != nil {
		return err
	}
	_, err = w.Write(buf)
	return err
}

// Deserialize deserializes the state from a byte slice with zstd compression.
func (m *EpochData) ReadFrom(r io.Reader) error {
	m.JustificationBits = &cltypes.JustificationBits{}

	lenB := make([]byte, 8)
	if _, err := io.ReadFull(r, lenB); err != nil {
		return err
	}
	len := binary.LittleEndian.Uint64(lenB)
	buf := make([]byte, len)
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}
	if m.Version >= clparams.FuluVersion {
		m.ProposerLookahead = solid.NewUint64VectorSSZ(int((1 + m.BeaconConfig.MinSeedLookahead) * m.BeaconConfig.SlotsPerEpoch))
	}
	return ssz2.UnmarshalSSZ(buf, 0, m.getSchema()...)
}

func (m *EpochData) getSchema() []interface{} {
	if m.Version < clparams.FuluVersion {
		return []interface{}{&m.TotalActiveBalance, m.JustificationBits, &m.CurrentJustifiedCheckpoint, &m.PreviousJustifiedCheckpoint, &m.FinalizedCheckpoint, &m.HistoricalSummariesLength, &m.HistoricalRootsLength}
	}
	return []interface{}{
		&m.TotalActiveBalance,
		m.JustificationBits,
		&m.CurrentJustifiedCheckpoint,
		&m.PreviousJustifiedCheckpoint,
		&m.FinalizedCheckpoint,
		&m.HistoricalSummariesLength,
		&m.HistoricalRootsLength,
		&m.ProposerLookahead,
	}
}
