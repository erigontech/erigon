package cltypes

import (
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/clonable"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/ssz"
)

var (
	_ ssz.HashableSSZ = (*ProposerPreferences)(nil)
	_ ssz.HashableSSZ = (*SignedProposerPreferences)(nil)

	_ ssz2.SizedObjectSSZ = (*ProposerPreferences)(nil)
	_ ssz2.SizedObjectSSZ = (*SignedProposerPreferences)(nil)
)

// ProposerPreferences represents the proposer's preferences for a slot.
// This is used in the gossip network to communicate proposer preferences to builders.
// [New in Gloas:EIP7732]
type ProposerPreferences struct {
	ProposalSlot   uint64         `json:"proposal_slot,string"`
	ValidatorIndex uint64         `json:"validator_index,string"`
	FeeRecipient   common.Address `json:"fee_recipient"`
	GasLimit       uint64         `json:"gas_limit,string"`
}

func (p *ProposerPreferences) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(
		p.ProposalSlot,
		p.ValidatorIndex,
		p.FeeRecipient[:],
		p.GasLimit,
	)
}

func (p *ProposerPreferences) EncodingSizeSSZ() int {
	return 8 + 8 + length.Addr + 8
}

func (p *ProposerPreferences) Static() bool {
	return true
}

func (p *ProposerPreferences) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, p.ProposalSlot, p.ValidatorIndex, p.FeeRecipient[:], p.GasLimit)
}

func (p *ProposerPreferences) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, &p.ProposalSlot, &p.ValidatorIndex, p.FeeRecipient[:], &p.GasLimit)
}

func (p *ProposerPreferences) Clone() clonable.Clonable {
	return &ProposerPreferences{
		ProposalSlot:   p.ProposalSlot,
		ValidatorIndex: p.ValidatorIndex,
		FeeRecipient:   p.FeeRecipient,
		GasLimit:       p.GasLimit,
	}
}

// SignedProposerPreferences represents a signed proposer preferences message.
// [New in Gloas:EIP7732]
type SignedProposerPreferences struct {
	Message   *ProposerPreferences `json:"message"`
	Signature common.Bytes96       `json:"signature"`
}

func (s *SignedProposerPreferences) EncodingSizeSSZ() int {
	if s.Message == nil {
		return length.Bytes96
	}
	return s.Message.EncodingSizeSSZ() + length.Bytes96
}

func (s *SignedProposerPreferences) Static() bool {
	return true
}

func (s *SignedProposerPreferences) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, s.Message, s.Signature[:])
}

func (s *SignedProposerPreferences) DecodeSSZ(buf []byte, version int) error {
	s.Message = new(ProposerPreferences)
	return ssz2.UnmarshalSSZ(buf, version, s.Message, s.Signature[:])
}

func (s *SignedProposerPreferences) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(s.Message, s.Signature[:])
}

func (s *SignedProposerPreferences) Clone() clonable.Clonable {
	return &SignedProposerPreferences{
		Message:   s.Message.Clone().(*ProposerPreferences),
		Signature: s.Signature,
	}
}
