package state_accessors

import (
	"compress/zlib"
	"encoding/binary"
	"io"
	"sync"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state/raw"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
)

// pool of zlib compressor objects
var zlibCompressorPool = sync.Pool{
	New: func() interface{} {
		return zlib.NewWriter(nil)
	},
}

type MinimalBeaconState struct {
	Version clparams.StateVersion
	// Lengths
	validatorLength                 uint64
	eth1DataLength                  uint64
	previousEpochAttestationsLength uint64
	currentEpochAttestationsLength  uint64
	HistoricalSummariesLength       uint64
	HistoricalRootsLength           uint64

	// Phase0
	Eth1Data          *cltypes.Eth1Data
	Eth1DepositIndex  uint64
	JustificationBits *cltypes.JustificationBits
	// Bellatrix
	LatestExecutionPayloadHeader *cltypes.Eth1Header
	// Capella
	NextWithdrawalIndex          uint64
	NextWithdrawalValidatorIndex uint64
}

func MinimalBeaconStateFromBeaconState(s *raw.BeaconState) *MinimalBeaconState {
	justificationCopy := &cltypes.JustificationBits{}
	jj := s.JustificationBits()
	copy(justificationCopy[:], jj[:])
	return &MinimalBeaconState{
		validatorLength:                 uint64(s.ValidatorLength()),
		eth1DataLength:                  uint64(s.Eth1DataVotes().Len()),
		previousEpochAttestationsLength: uint64(s.PreviousEpochAttestations().Len()),
		currentEpochAttestationsLength:  uint64(s.CurrentEpochAttestations().Len()),
		HistoricalSummariesLength:       s.HistoricalSummariesLength(),
		HistoricalRootsLength:           s.HistoricalRootsLength(),
		Version:                         s.Version(),
		Eth1Data:                        s.Eth1Data(),
		Eth1DepositIndex:                s.Eth1DepositIndex(),
		JustificationBits:               justificationCopy,
		NextWithdrawalIndex:             s.NextWithdrawalIndex(),
		NextWithdrawalValidatorIndex:    s.NextWithdrawalValidatorIndex(),
		LatestExecutionPayloadHeader:    s.LatestExecutionPayloadHeader(),
	}

}

// Serialize serializes the state into a byte slice with zlib compression.
func (m *MinimalBeaconState) Serialize(w io.Writer) error {
	compressor := zlibCompressorPool.Get().(*zlib.Writer)
	compressor.Reset(w)
	buf, err := ssz2.MarshalSSZ(nil, m.getSchema()...)
	if err != nil {
		return err
	}
	lenB := make([]byte, 8)
	binary.BigEndian.PutUint64(lenB, uint64(len(buf)))
	if _, err = compressor.Write([]byte{byte(m.Version)}); err != nil {
		return err
	}
	if _, err = compressor.Write(lenB); err != nil {
		return err
	}
	_, err = compressor.Write(buf)
	if err != nil {
		return err
	}
	return compressor.Flush()
}

// Deserialize deserializes the state from a byte slice with zlib compression.
func (m *MinimalBeaconState) Deserialize(r io.Reader) error {
	m.Eth1Data = &cltypes.Eth1Data{}
	m.JustificationBits = &cltypes.JustificationBits{}

	decompressor, err := zlib.NewReader(r)
	if err != nil {
		return err
	}
	defer decompressor.Close()
	versionByte := make([]byte, 1)
	if _, err = decompressor.Read(versionByte); err != nil {
		return err
	}
	m.Version = clparams.StateVersion(versionByte[0])

	if m.Version >= clparams.BellatrixVersion {
		m.LatestExecutionPayloadHeader = cltypes.NewEth1Header(m.Version)
	}
	lenB := make([]byte, 8)
	if _, err = decompressor.Read(lenB); err != nil {
		return err
	}

	buf := make([]byte, binary.BigEndian.Uint64(lenB))

	if _, err = decompressor.Read(buf); err != nil {
		return err
	}
	return ssz2.UnmarshalSSZ(buf, int(m.Version), m.getSchema()...)
}

func (m *MinimalBeaconState) getSchema() []interface{} {
	schema := []interface{}{m.Eth1Data, &m.Eth1DepositIndex, m.JustificationBits, &m.validatorLength, &m.eth1DataLength, &m.previousEpochAttestationsLength, &m.currentEpochAttestationsLength, &m.HistoricalSummariesLength, &m.HistoricalRootsLength}
	if m.Version >= clparams.BellatrixVersion {
		schema = append(schema, m.LatestExecutionPayloadHeader)
	}
	if m.Version >= clparams.CapellaVersion {
		schema = append(schema, &m.NextWithdrawalIndex, &m.NextWithdrawalValidatorIndex)
	}
	return schema
}
