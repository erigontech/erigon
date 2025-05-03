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
	"encoding/json"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon-lib/types/ssz"
	"github.com/erigontech/erigon/cl/merkle_tree"
)

// Whole committee(512) public key and the aggregate public key.
const syncCommitteeSize = 48 * 513

type SyncCommittee [syncCommitteeSize]byte

func NewSyncCommitteeFromParameters(
	committee []common.Bytes48,
	aggregatePublicKey common.Bytes48,
) *SyncCommittee {
	s := &SyncCommittee{}
	s.SetAggregatePublicKey(aggregatePublicKey)
	s.SetCommittee(committee)
	return s
}

func (s *SyncCommittee) GetCommittee() []common.Bytes48 {
	committee := make([]common.Bytes48, 512)
	for i := range committee {
		copy(committee[i][:], s[i*48:])
	}
	return committee
}

func (s *SyncCommittee) AggregatePublicKey() (out common.Bytes48) {
	copy(out[:], s[syncCommitteeSize-48:])
	return
}

func (s *SyncCommittee) SetCommittee(committee []common.Bytes48) {
	for i := range committee {
		copy(s[i*48:], committee[i][:])
	}
}

func (s *SyncCommittee) SetAggregatePublicKey(k common.Bytes48) {
	copy(s[syncCommitteeSize-48:], k[:])
}

func (s *SyncCommittee) EncodingSizeSSZ() int {
	return syncCommitteeSize
}

func (s *SyncCommittee) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) < s.EncodingSizeSSZ() {
		return ssz.ErrLowBufferSize
	}
	copy(s[:], buf)
	return nil
}

func (s *SyncCommittee) EncodeSSZ(dst []byte) ([]byte, error) {
	return append(dst, s[:]...), nil
}

func (s *SyncCommittee) Clone() clonable.Clonable {
	return &SyncCommittee{}
}

func (s *SyncCommittee) Copy() *SyncCommittee {
	t := &SyncCommittee{}
	copy(t[:], s[:])
	return t
}

func (s *SyncCommittee) Equal(o *SyncCommittee) bool {
	return *s == *o
}

func (s *SyncCommittee) HashSSZ() ([32]byte, error) {
	syncCommitteeLayer := make([]byte, 512*32)
	for i := 0; i < 512; i++ {
		root, err := merkle_tree.BytesRoot(s[i*48 : (i*48)+48])
		if err != nil {
			return [32]byte{}, err
		}
		copy(syncCommitteeLayer[i*32:], root[:])
	}
	return merkle_tree.HashTreeRoot(syncCommitteeLayer, s[syncCommitteeSize-48:])
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
	var err error
	var tmp struct {
		Committee          []common.Bytes48 `json:"pubkeys"`
		AggregatePublicKey common.Bytes48   `json:"aggregate_pubkey"`
	}
	if err = json.Unmarshal(input, &tmp); err != nil {
		return err
	}
	s.SetAggregatePublicKey(tmp.AggregatePublicKey)
	s.SetCommittee(tmp.Committee)
	return nil
}
