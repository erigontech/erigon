package solid

import (
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/clonable"
	"github.com/erigontech/erigon/common/length"
)

var (
	_ EncodableHashableSSZ = (*BuilderDepositRequest)(nil)
	_ ssz2.SizedObjectSSZ  = (*BuilderDepositRequest)(nil)

	_ EncodableHashableSSZ = (*BuilderExitRequest)(nil)
	_ ssz2.SizedObjectSSZ  = (*BuilderExitRequest)(nil)
)

const (
	SizeBuilderDepositRequest = length.Bytes48 + length.Hash + 8 + length.Bytes96
	SizeBuilderExitRequest    = length.Addr + length.Bytes48
)

type BuilderDepositRequest struct {
	PubKey                common.Bytes48 `json:"pubkey"`
	WithdrawalCredentials common.Hash    `json:"withdrawal_credentials"`
	Amount                uint64         `json:"amount,string"`
	Signature             common.Bytes96 `json:"signature"`
}

func (p *BuilderDepositRequest) EncodingSizeSSZ() int {
	return SizeBuilderDepositRequest
}

func (p *BuilderDepositRequest) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, p.PubKey[:], p.WithdrawalCredentials[:], &p.Amount, p.Signature[:])
}

func (p *BuilderDepositRequest) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, p.PubKey[:], p.WithdrawalCredentials[:], &p.Amount, p.Signature[:])
}

func (p *BuilderDepositRequest) Clone() clonable.Clonable {
	return &BuilderDepositRequest{}
}

func (p *BuilderDepositRequest) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(p.PubKey[:], p.WithdrawalCredentials[:], &p.Amount, p.Signature[:])
}

func (p *BuilderDepositRequest) Static() bool {
	return true
}

type BuilderExitRequest struct {
	SourceAddress common.Address `json:"source_address"`
	PubKey        common.Bytes48 `json:"pubkey"`
}

func (p *BuilderExitRequest) EncodingSizeSSZ() int {
	return SizeBuilderExitRequest
}

func (p *BuilderExitRequest) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, p.SourceAddress[:], p.PubKey[:])
}

func (p *BuilderExitRequest) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, p.SourceAddress[:], p.PubKey[:])
}

func (p *BuilderExitRequest) Clone() clonable.Clonable {
	return &BuilderExitRequest{}
}

func (p *BuilderExitRequest) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(p.SourceAddress[:], p.PubKey[:])
}

func (p *BuilderExitRequest) Static() bool {
	return true
}
