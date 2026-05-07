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

package solid

import (
	"bytes"
	"encoding/json"

	"github.com/erigontech/erigon/cl/merkle_tree"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/clonable"
	"github.com/erigontech/erigon/common/ssz"
)

// Default committee size for mainnet preset. Used when no config is available.
const defaultSyncCommitteeSize = 512

// SyncCommittee holds N validator public keys (48 bytes each) plus one
// aggregate public key. The size is determined by SYNC_COMMITTEE_SIZE from
// the beacon chain config (512 for mainnet, 32 for minimal).
type SyncCommittee struct {
	data          []byte // (committeeSize + 1) * 48 bytes
	committeeSize int    // number of validators (not including aggregate key)
}

// NewSyncCommittee creates a SyncCommittee with the default mainnet size.
func NewSyncCommittee() *SyncCommittee {
	return NewSyncCommitteeWithSize(defaultSyncCommitteeSize)
}

// NewSyncCommitteeWithSize creates a SyncCommittee for the given committee size.
func NewSyncCommitteeWithSize(committeeSize int) *SyncCommittee {
	return &SyncCommittee{
		data:          make([]byte, (committeeSize+1)*48),
		committeeSize: committeeSize,
	}
}

func NewSyncCommitteeFromParameters(
	committee []common.Bytes48,
	aggregatePublicKey common.Bytes48,
) *SyncCommittee {
	s := NewSyncCommitteeWithSize(len(committee))
	s.SetAggregatePublicKey(aggregatePublicKey)
	s.SetCommittee(committee)
	return s
}

// Bytes returns the raw backing data. Replaces the old s[:] slice syntax.
func (s *SyncCommittee) Bytes() []byte {
	s.ensureData()
	return s.data
}

func (s *SyncCommittee) CommitteeSize() int {
	if s.committeeSize == 0 && len(s.data) > 0 {
		// Backwards compat: infer from data length
		s.committeeSize = len(s.data)/48 - 1
	}
	if s.committeeSize == 0 {
		return defaultSyncCommitteeSize
	}
	return s.committeeSize
}

func (s *SyncCommittee) ensureData() {
	if len(s.data) == 0 {
		s.data = make([]byte, (s.CommitteeSize()+1)*48)
	}
}

func (s *SyncCommittee) GetCommittee() []common.Bytes48 {
	s.ensureData()
	size := s.CommitteeSize()
	committee := make([]common.Bytes48, size)
	for i := range committee {
		copy(committee[i][:], s.data[i*48:])
	}
	return committee
}

func (s *SyncCommittee) AggregatePublicKey() (out common.Bytes48) {
	s.ensureData()
	copy(out[:], s.data[len(s.data)-48:])
	return
}

func (s *SyncCommittee) SetCommittee(committee []common.Bytes48) {
	s.ensureData()
	for i := range committee {
		if i*48 >= len(s.data)-48 {
			break
		}
		copy(s.data[i*48:], committee[i][:])
	}
}

func (s *SyncCommittee) SetAggregatePublicKey(k common.Bytes48) {
	s.ensureData()
	copy(s.data[len(s.data)-48:], k[:])
}

func (s *SyncCommittee) EncodingSizeSSZ() int {
	s.ensureData()
	return len(s.data)
}

func (s *SyncCommittee) DecodeSSZ(buf []byte, _ int) error {
	size := s.EncodingSizeSSZ()
	if len(buf) < size {
		return ssz.ErrLowBufferSize
	}
	s.committeeSize = size/48 - 1
	s.data = make([]byte, size)
	copy(s.data, buf[:size])
	return nil
}

func (s *SyncCommittee) EncodeSSZ(dst []byte) ([]byte, error) {
	s.ensureData()
	return append(dst, s.data...), nil
}

func (s *SyncCommittee) Clone() clonable.Clonable {
	return NewSyncCommitteeWithSize(s.CommitteeSize())
}

func (s *SyncCommittee) Copy() *SyncCommittee {
	t := NewSyncCommitteeWithSize(s.CommitteeSize())
	copy(t.data, s.data)
	return t
}

func (s *SyncCommittee) Equal(o *SyncCommittee) bool {
	if s == o {
		return true
	}
	if s == nil || o == nil {
		return false
	}
	return bytes.Equal(s.data, o.data)
}

func (s *SyncCommittee) HashSSZ() ([32]byte, error) {
	s.ensureData()
	size := s.CommitteeSize()
	syncCommitteeLayer := make([]byte, size*32)
	for i := 0; i < size; i++ {
		root, err := merkle_tree.BytesRoot(s.data[i*48 : (i*48)+48])
		if err != nil {
			return [32]byte{}, err
		}
		copy(syncCommitteeLayer[i*32:], root[:])
	}
	return merkle_tree.HashTreeRoot(syncCommitteeLayer, s.data[len(s.data)-48:])
}

func (s *SyncCommittee) Static() bool {
	return true
}

func (s *SyncCommittee) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Committee          []common.Bytes48 `json:"pubkeys"`
		AggregatePublicKey common.Bytes48   `json:"aggregate_pubkey"`
	}{
		Committee:          s.GetCommittee(),
		AggregatePublicKey: s.AggregatePublicKey(),
	})
}

func (s *SyncCommittee) UnmarshalJSON(input []byte) error {
	var tmp struct {
		Committee          []common.Bytes48 `json:"pubkeys"`
		AggregatePublicKey common.Bytes48   `json:"aggregate_pubkey"`
	}
	if err := json.Unmarshal(input, &tmp); err != nil {
		return err
	}
	s.committeeSize = len(tmp.Committee)
	s.data = make([]byte, (s.committeeSize+1)*48)
	s.SetAggregatePublicKey(tmp.AggregatePublicKey)
	s.SetCommittee(tmp.Committee)
	return nil
}
