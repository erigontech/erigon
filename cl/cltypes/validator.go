package cltypes

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon-lib/types/ssz"

	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
)

const (
	DepositProofLength = 33
	SyncCommitteeSize  = 512
)

type DepositData struct {
	PubKey                [48]byte
	WithdrawalCredentials libcommon.Hash
	Amount                uint64
	Signature             libcommon.Bytes96
	Root                  libcommon.Hash // Ignored if not for hashing
}

func (d *DepositData) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, d.PubKey[:], d.WithdrawalCredentials[:], ssz.Uint64SSZ(d.Amount), d.Signature[:])
}

func (d *DepositData) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, d.PubKey[:], d.WithdrawalCredentials[:], &d.Amount, d.Signature[:])
}

func (d *DepositData) EncodingSizeSSZ() int {
	return 184
}

func (d *DepositData) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(d.PubKey[:], d.WithdrawalCredentials[:], d.Amount, d.Signature[:])
}

func (d *DepositData) MessageHash() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(d.PubKey[:], d.WithdrawalCredentials[:], d.Amount)
}

func (*DepositData) Static() bool {
	return true
}

type Deposit struct {
	// Merkle proof is used for deposits
	Proof solid.HashVectorSSZ // 33 X 32 size.
	Data  *DepositData
}

func (d *Deposit) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, d.Proof, d.Data)
}

func (d *Deposit) DecodeSSZ(buf []byte, version int) error {
	d.Proof = solid.NewHashVector(33)
	d.Data = new(DepositData)

	return ssz2.UnmarshalSSZ(buf, version, d.Proof, d.Data)
}

func (d *Deposit) EncodingSizeSSZ() int {
	return 1240
}

func (d *Deposit) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(d.Proof, d.Data)
}

type VoluntaryExit struct {
	Epoch          uint64
	ValidatorIndex uint64
}

func (e *VoluntaryExit) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, e.Epoch, e.ValidatorIndex)
}

func (*VoluntaryExit) Clone() clonable.Clonable {
	return &VoluntaryExit{}
}

func (*VoluntaryExit) Static() bool {
	return true
}

func (e *VoluntaryExit) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, 0, &e.Epoch, &e.ValidatorIndex)
}

func (e *VoluntaryExit) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(e.Epoch, e.ValidatorIndex)
}

func (*VoluntaryExit) EncodingSizeSSZ() int {
	return 16
}

type SignedVoluntaryExit struct {
	VolunaryExit *VoluntaryExit
	Signature    libcommon.Bytes96
}

func (e *SignedVoluntaryExit) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, e.VolunaryExit, e.Signature[:])
}

func (e *SignedVoluntaryExit) DecodeSSZ(buf []byte, version int) error {
	e.VolunaryExit = new(VoluntaryExit)
	return ssz2.UnmarshalSSZ(buf, version, e.VolunaryExit, e.Signature[:])
}

func (e *SignedVoluntaryExit) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(e.VolunaryExit, e.Signature[:])
}

func (e *SignedVoluntaryExit) EncodingSizeSSZ() int {
	return 96 + e.VolunaryExit.EncodingSizeSSZ()
}
