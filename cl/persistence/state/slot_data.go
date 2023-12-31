package state_accessors

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
)

type SlotData struct {
	// Block Header and Execution Headers can be retrieved from block snapshots
	Version clparams.StateVersion
	// Lengths
	ValidatorLength                 uint64
	Eth1DataLength                  uint64
	PreviousEpochAttestationsLength uint64
	CurrentEpochAttestationsLength  uint64
	// Phase0
	Eth1Data         *cltypes.Eth1Data
	Eth1DepositIndex uint64
	// Capella
	NextWithdrawalIndex          uint64
	NextWithdrawalValidatorIndex uint64

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
		ValidatorLength:                 uint64(s.ValidatorLength()),
		Eth1DataLength:                  uint64(s.Eth1DataVotes().Len()),
		PreviousEpochAttestationsLength: uint64(s.PreviousEpochAttestations().Len()),
		CurrentEpochAttestationsLength:  uint64(s.CurrentEpochAttestations().Len()),
		Version:                         s.Version(),
		Eth1Data:                        s.Eth1Data(),
		Eth1DepositIndex:                s.Eth1DepositIndex(),
		NextWithdrawalIndex:             s.NextWithdrawalIndex(),
		NextWithdrawalValidatorIndex:    s.NextWithdrawalValidatorIndex(),
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
func (m *SlotData) ReadFrom(r io.Reader) error {
	m.Eth1Data = &cltypes.Eth1Data{}
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
	schema := []interface{}{m.Eth1Data, &m.Eth1DepositIndex, &m.ValidatorLength, &m.Eth1DataLength, &m.PreviousEpochAttestationsLength, &m.CurrentEpochAttestationsLength, &m.AttestationsRewards, &m.SyncAggregateRewards, &m.ProposerSlashings, &m.AttesterSlashings}
	if m.Version >= clparams.CapellaVersion {
		schema = append(schema, &m.NextWithdrawalIndex, &m.NextWithdrawalValidatorIndex)
	}
	return schema
}
