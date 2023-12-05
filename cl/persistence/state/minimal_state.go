package state_accessors

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/raw"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
)

type MinimalBeaconState struct {
	// Block Header and Execution Headers can be retrieved from block snapshots
	Version clparams.StateVersion
	// Lengths
	ValidatorLength                 uint64
	Eth1DataLength                  uint64
	PreviousEpochAttestationsLength uint64
	CurrentEpochAttestationsLength  uint64
	HistoricalSummariesLength       uint64
	HistoricalRootsLength           uint64
	// Phase0
	Eth1Data          *cltypes.Eth1Data
	Eth1DepositIndex  uint64
	JustificationBits *cltypes.JustificationBits
	Fork              *cltypes.Fork
	// Capella
	NextWithdrawalIndex          uint64
	NextWithdrawalValidatorIndex uint64
}

func MinimalBeaconStateFromBeaconState(s *raw.BeaconState) *MinimalBeaconState {
	justificationCopy := &cltypes.JustificationBits{}
	jj := s.JustificationBits()
	copy(justificationCopy[:], jj[:])
	return &MinimalBeaconState{
		Fork:                            s.Fork(),
		ValidatorLength:                 uint64(s.ValidatorLength()),
		Eth1DataLength:                  uint64(s.Eth1DataVotes().Len()),
		PreviousEpochAttestationsLength: uint64(s.PreviousEpochAttestations().Len()),
		CurrentEpochAttestationsLength:  uint64(s.CurrentEpochAttestations().Len()),
		HistoricalSummariesLength:       s.HistoricalSummariesLength(),
		HistoricalRootsLength:           s.HistoricalRootsLength(),
		Version:                         s.Version(),
		Eth1Data:                        s.Eth1Data(),
		Eth1DepositIndex:                s.Eth1DepositIndex(),
		JustificationBits:               justificationCopy,
		NextWithdrawalIndex:             s.NextWithdrawalIndex(),
		NextWithdrawalValidatorIndex:    s.NextWithdrawalValidatorIndex(),
	}

}

// Serialize serializes the state into a byte slice with zstd compression.
func (m *MinimalBeaconState) WriteTo(w io.Writer) error {
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
func (m *MinimalBeaconState) ReadFrom(r io.Reader) error {
	m.Eth1Data = &cltypes.Eth1Data{}
	m.JustificationBits = &cltypes.JustificationBits{}
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

func (m *MinimalBeaconState) getSchema() []interface{} {
	schema := []interface{}{m.Eth1Data, m.Fork, &m.Eth1DepositIndex, m.JustificationBits, &m.ValidatorLength, &m.Eth1DataLength, &m.PreviousEpochAttestationsLength, &m.CurrentEpochAttestationsLength, &m.HistoricalSummariesLength, &m.HistoricalRootsLength}
	if m.Version >= clparams.CapellaVersion {
		schema = append(schema, &m.NextWithdrawalIndex, &m.NextWithdrawalValidatorIndex)
	}
	return schema
}
