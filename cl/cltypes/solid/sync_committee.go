package solid

import (
	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
)

// Whole committee(512) public key and the aggregate public key.
const syncCommitteeSize = 48 * 513

type SyncCommittee [syncCommitteeSize]byte

func NewSyncCommitteeFromParameters(
	committee [][48]byte,
	aggregatePublicKey [48]byte,
) *SyncCommittee {
	s := &SyncCommittee{}
	s.SetAggregatePublicKey(aggregatePublicKey)
	s.SetCommittee(committee)
	return s
}

func (s *SyncCommittee) GetCommittee() [][48]byte {
	committee := make([][48]byte, 512)
	for i := range committee {
		copy(committee[i][:], s[i*48:])
	}
	return committee
}

func (s *SyncCommittee) AggregatePublicKey() (out [48]byte) {
	copy(out[:], s[syncCommitteeSize-48:])
	return
}

func (s *SyncCommittee) SetCommittee(committee [][48]byte) {
	for i := range committee {
		copy(s[i*48:], committee[i][:])
	}
}

func (s *SyncCommittee) SetAggregatePublicKey(k [48]byte) {
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
