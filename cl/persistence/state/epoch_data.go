package state_accessors

import (
	"encoding/binary"
	"io"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
)

// EpochData stores the data for the epoch (valid throughout the epoch)
type EpochData struct {
	TotalActiveBalance          uint64
	JustificationBits           *cltypes.JustificationBits
	Fork                        *cltypes.Fork
	CurrentJustifiedCheckpoint  solid.Checkpoint
	PreviousJustifiedCheckpoint solid.Checkpoint
	FinalizedCheckpoint         solid.Checkpoint
	HistoricalSummariesLength   uint64
	HistoricalRootsLength       uint64
}

func EpochDataFromBeaconState(s *state.CachingBeaconState) *EpochData {
	justificationCopy := &cltypes.JustificationBits{}
	jj := s.JustificationBits()
	copy(justificationCopy[:], jj[:])
	return &EpochData{
		Fork:                        s.Fork(),
		JustificationBits:           justificationCopy,
		TotalActiveBalance:          s.GetTotalActiveBalance(),
		CurrentJustifiedCheckpoint:  s.CurrentJustifiedCheckpoint(),
		PreviousJustifiedCheckpoint: s.PreviousJustifiedCheckpoint(),
		FinalizedCheckpoint:         s.FinalizedCheckpoint(),
		HistoricalSummariesLength:   s.HistoricalSummariesLength(),
		HistoricalRootsLength:       s.HistoricalRootsLength(),
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
	m.Fork = &cltypes.Fork{}
	m.FinalizedCheckpoint = solid.NewCheckpoint()
	m.CurrentJustifiedCheckpoint = solid.NewCheckpoint()
	m.PreviousJustifiedCheckpoint = solid.NewCheckpoint()
	lenB := make([]byte, 8)
	if _, err := io.ReadFull(r, lenB); err != nil {
		return err
	}
	len := binary.LittleEndian.Uint64(lenB)
	buf := make([]byte, len)
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}
	return ssz2.UnmarshalSSZ(buf, 0, m.getSchema()...)
}

func (m *EpochData) getSchema() []interface{} {
	return []interface{}{&m.TotalActiveBalance, m.JustificationBits, m.Fork, m.CurrentJustifiedCheckpoint, m.PreviousJustifiedCheckpoint, m.FinalizedCheckpoint, &m.HistoricalSummariesLength, &m.HistoricalRootsLength}
}
