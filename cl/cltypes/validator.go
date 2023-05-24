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
	WithdrawalCredentials [32]byte // 32 byte
	Amount                uint64
	Signature             [96]byte
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
	Signature    [96]byte
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

// Validator, contains if we were on bellatrix/alteir/phase0 and transition epoch.
type Validator struct {
	solid.Validator

	// This is all stuff used by phase0 state transition. It makes many operations faster.
	// Source attesters
	IsCurrentMatchingSourceAttester  bool
	IsPreviousMatchingSourceAttester bool
	// Target Attesters
	IsCurrentMatchingTargetAttester  bool
	IsPreviousMatchingTargetAttester bool
	// Head attesters
	IsCurrentMatchingHeadAttester  bool
	IsPreviousMatchingHeadAttester bool
	// MinInclusionDelay
	MinCurrentInclusionDelayAttestation  *solid.PendingAttestation
	MinPreviousInclusionDelayAttestation *solid.PendingAttestation
}

// DutiesAttested returns how many of its duties the validator attested and missed
func (v *Validator) DutiesAttested() (attested, missed uint64) {
	if v.Slashed() {
		return 0, 3
	}
	if v.IsPreviousMatchingSourceAttester {
		attested++
	}
	if v.IsPreviousMatchingTargetAttester {
		attested++
	}
	if v.IsPreviousMatchingHeadAttester {
		attested++
	}
	missed = 3 - attested
	return
}
func (v *Validator) IsSlashable(epoch uint64) bool {
	return !v.Slashed() && (v.ActivationEpoch() <= epoch) && (epoch < v.WithdrawableEpoch())
}

func (v *Validator) EncodeSSZ(dst []byte) ([]byte, error) {
	return v.Validator.EncodeSSZ(dst), nil
}

func (v *Validator) DecodeSSZ(buf []byte, _ int) error {
	return v.Validator.DecodeSSZ(buf, 0)
}

func (v *Validator) HashSSZ() (o [32]byte, err error) {
	leaves := make([]byte, 8*32)
	v.CopyHashBufferTo(leaves)
	leaves = leaves[:(8 * 32)]
	err = merkle_tree.MerkleRootFromFlatLeaves(leaves, o[:])
	return
}

// Active returns if validator is active for given epoch
func (v *Validator) Active(epoch uint64) bool {
	return v.ActivationEpoch() <= epoch && epoch < v.ExitEpoch()
}

func (v *Validator) CopyTo(target *Validator) {
	v.Validator.CopyTo(&target.Validator)
}
