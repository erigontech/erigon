package cltypes

import (
	"bytes"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cl/cltypes/ssz_utils"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"github.com/ledgerwatch/erigon/cl/utils"
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

func (d *DepositData) EncodeSSZ(dst []byte) []byte {
	buf := dst
	buf = append(buf, d.PubKey[:]...)
	buf = append(buf, d.WithdrawalCredentials[:]...)
	buf = append(buf, ssz_utils.Uint64SSZ(d.Amount)...)
	buf = append(buf, d.Signature[:]...)
	return buf
}

func (d *DepositData) DecodeSSZ(buf []byte) error {
	copy(d.PubKey[:], buf)
	copy(d.WithdrawalCredentials[:], buf[48:])
	d.Amount = ssz_utils.UnmarshalUint64SSZ(buf[80:])
	copy(d.Signature[:], buf[88:])
	return nil
}

func (d *DepositData) EncodingSizeSSZ() int {
	return 184
}

func (d *DepositData) HashSSZ() ([32]byte, error) {
	var (
		leaves = make([][32]byte, 4)
		err    error
	)
	leaves[0], err = merkle_tree.PublicKeyRoot(d.PubKey)
	if err != nil {
		return [32]byte{}, err
	}
	leaves[1] = d.WithdrawalCredentials
	leaves[2] = merkle_tree.Uint64Root(d.Amount)
	leaves[3], err = merkle_tree.SignatureRoot(d.Signature)
	if err != nil {
		return [32]byte{}, err
	}
	return merkle_tree.ArraysRoot(leaves, 4)
}

func (d *DepositData) MessageHash() ([32]byte, error) {
	var (
		leaves = make([][32]byte, 4)
		err    error
	)
	leaves[0], err = merkle_tree.PublicKeyRoot(d.PubKey)
	if err != nil {
		return [32]byte{}, err
	}
	leaves[1] = d.WithdrawalCredentials
	leaves[2] = merkle_tree.Uint64Root(d.Amount)
	return merkle_tree.ArraysRoot(leaves, 4)
}

type Deposit struct {
	// Merkle proof is used for deposits
	Proof []libcommon.Hash // 33 X 32 size.
	Data  *DepositData
}

func (d *Deposit) EncodeSSZ(dst []byte) []byte {

	buf := dst
	for _, proofSeg := range d.Proof {
		buf = append(buf, proofSeg[:]...)
	}
	buf = d.Data.EncodeSSZ(buf)
	return buf
}

func (d *Deposit) DecodeSSZ(buf []byte) error {
	d.Proof = make([]libcommon.Hash, DepositProofLength)
	for i := range d.Proof {
		copy(d.Proof[i][:], buf[i*32:i*32+32])
	}

	if d.Data == nil {
		d.Data = new(DepositData)
	}
	return d.Data.DecodeSSZ(buf[33*32:])
}

func (d *Deposit) DecodeSSZWithVersion(buf []byte, _ int) error {
	return d.DecodeSSZ(buf)
}

func (d *Deposit) EncodingSizeSSZ() int {
	return 1240
}

func (d *Deposit) HashSSZ() ([32]byte, error) {
	proofLeaves := make([][32]byte, DepositProofLength)
	for i, segProof := range d.Proof {
		copy(proofLeaves[i][:], segProof[:])
	}

	proofRoot, err := merkle_tree.ArraysRoot(proofLeaves, 64)
	if err != nil {
		return [32]byte{}, err
	}

	depositRoot, err := d.Data.HashSSZ()
	if err != nil {
		return [32]byte{}, err
	}

	return merkle_tree.ArraysRoot([][32]byte{proofRoot, depositRoot}, 2)
}

type VoluntaryExit struct {
	Epoch          uint64
	ValidatorIndex uint64
}

func (e *VoluntaryExit) EncodeSSZ(buf []byte) []byte {
	return append(buf, append(ssz_utils.Uint64SSZ(e.Epoch), ssz_utils.Uint64SSZ(e.ValidatorIndex)...)...)
}

func (e *VoluntaryExit) DecodeSSZ(buf []byte) error {
	e.Epoch = ssz_utils.UnmarshalUint64SSZ(buf)
	e.ValidatorIndex = ssz_utils.UnmarshalUint64SSZ(buf[8:])
	return nil
}

func (e *VoluntaryExit) HashSSZ() ([32]byte, error) {
	epochRoot := merkle_tree.Uint64Root(e.Epoch)
	indexRoot := merkle_tree.Uint64Root(e.ValidatorIndex)
	return utils.Keccak256(epochRoot[:], indexRoot[:]), nil
}

func (e *VoluntaryExit) EncodingSizeSSZ() int {
	return 16
}

type SignedVoluntaryExit struct {
	VolunaryExit *VoluntaryExit
	Signature    [96]byte
}

func (e *SignedVoluntaryExit) EncodeSSZ(dst []byte) []byte {
	buf := e.VolunaryExit.EncodeSSZ(dst)
	return append(buf, e.Signature[:]...)
}

func (e *SignedVoluntaryExit) DecodeSSZ(buf []byte) error {
	if e.VolunaryExit == nil {
		e.VolunaryExit = new(VoluntaryExit)
	}

	if err := e.VolunaryExit.DecodeSSZ(buf); err != nil {
		return err
	}
	copy(e.Signature[:], buf[16:])
	return nil
}

func (e *SignedVoluntaryExit) DecodeSSZWithVersion(buf []byte, _ int) error {
	return e.DecodeSSZ(buf)
}

func (e *SignedVoluntaryExit) HashSSZ() ([32]byte, error) {
	sigRoot, err := merkle_tree.SignatureRoot(e.Signature)
	if err != nil {
		return [32]byte{}, err
	}
	exitRoot, err := e.VolunaryExit.HashSSZ()
	if err != nil {
		return [32]byte{}, err
	}
	return utils.Keccak256(exitRoot[:], sigRoot[:]), nil
}

func (e *SignedVoluntaryExit) EncodingSizeSSZ() int {
	return 96 + e.VolunaryExit.EncodingSizeSSZ()
}

/*
 * Sync committe public keys and their aggregate public keys, we use array of pubKeys.
 */
type SyncCommittee struct {
	PubKeys            [][48]byte `ssz-size:"512,48"`
	AggregatePublicKey [48]byte   `ssz-size:"48"`
}

// MarshalSSZTo ssz marshals the SyncCommittee object to a target array
func (s *SyncCommittee) EncodeSSZ(buf []byte) ([]byte, error) {
	dst := buf

	if len(s.PubKeys) != SyncCommitteeSize {
		return nil, fmt.Errorf("wrong sync committee size")
	}
	for _, key := range s.PubKeys {
		dst = append(dst, key[:]...)
	}
	dst = append(dst, s.AggregatePublicKey[:]...)

	return dst, nil
}

// DecodeSSZ ssz unmarshals the SyncCommittee object
func (s *SyncCommittee) DecodeSSZ(buf []byte) error {
	if len(buf) < 24624 {
		return ssz_utils.ErrLowBufferSize
	}

	s.PubKeys = make([][48]byte, SyncCommitteeSize)
	for i := range s.PubKeys {
		copy(s.PubKeys[i][:], buf[i*48:(i*48)+48])
	}
	copy(s.AggregatePublicKey[:], buf[24576:])

	return nil
}

// EncodingSizeSSZ returns the ssz encoded size in bytes for the SyncCommittee object
func (s *SyncCommittee) EncodingSizeSSZ() (size int) {
	size = 24624
	return
}

// HashTreeRootWith ssz hashes the SyncCommittee object with a hasher
func (s *SyncCommittee) HashSSZ() ([32]byte, error) {
	// Compute the sync committee leaf
	pubKeysLeaves := make([][32]byte, SyncCommitteeSize)
	if len(s.PubKeys) != SyncCommitteeSize {
		return [32]byte{}, fmt.Errorf("wrong sync committee size")
	}
	var err error
	for i, key := range s.PubKeys {
		pubKeysLeaves[i], err = merkle_tree.PublicKeyRoot(key)
		if err != nil {
			return [32]byte{}, err
		}
	}
	pubKeyLeaf, err := merkle_tree.ArraysRoot(pubKeysLeaves, SyncCommitteeSize)
	if err != nil {
		return [32]byte{}, err
	}
	aggregatePublicKeyRoot, err := merkle_tree.PublicKeyRoot(s.AggregatePublicKey)
	if err != nil {
		return [32]byte{}, err
	}

	return merkle_tree.ArraysRoot([][32]byte{pubKeyLeaf, aggregatePublicKeyRoot}, 2)
}

func (s *SyncCommittee) Equal(s2 *SyncCommittee) bool {
	if !bytes.Equal(s.AggregatePublicKey[:], s2.AggregatePublicKey[:]) {
		return false
	}
	if len(s.PubKeys) != len(s2.PubKeys) {
		return false
	}
	for i := range s.PubKeys {
		if !bytes.Equal(s.PubKeys[i][:], s2.PubKeys[i][:]) {
			return false
		}
	}
	return true
}

// Validator, contains if we were on bellatrix/alteir/phase0 and transition epoch.
type Validator struct {
	PublicKey                  [48]byte
	WithdrawalCredentials      libcommon.Hash
	EffectiveBalance           uint64
	Slashed                    bool
	ActivationEligibilityEpoch uint64
	ActivationEpoch            uint64
	ExitEpoch                  uint64
	WithdrawableEpoch          uint64
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
	MinCurrentInclusionDelayAttestation  *PendingAttestation
	MinPreviousInclusionDelayAttestation *PendingAttestation
}

// DutiesAttested returns how many of its duties the validator attested and missed
func (v *Validator) DutiesAttested() (attested, missed uint64) {
	if v.Slashed {
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
	return !v.Slashed && (v.ActivationEpoch <= epoch) && (epoch < v.WithdrawableEpoch)
}

func (v *Validator) EncodeSSZ(dst []byte) ([]byte, error) {
	buf := dst
	buf = append(buf, v.PublicKey[:]...)
	buf = append(buf, v.WithdrawalCredentials[:]...)
	buf = append(buf, ssz_utils.Uint64SSZ(v.EffectiveBalance)...)
	buf = append(buf, ssz_utils.BoolSSZ(v.Slashed))
	buf = append(buf, ssz_utils.Uint64SSZ(v.ActivationEligibilityEpoch)...)
	buf = append(buf, ssz_utils.Uint64SSZ(v.ActivationEpoch)...)
	buf = append(buf, ssz_utils.Uint64SSZ(v.ExitEpoch)...)
	buf = append(buf, ssz_utils.Uint64SSZ(v.WithdrawableEpoch)...)
	return buf, nil
}

func (v *Validator) DecodeSSZWithVersion(buf []byte, _ int) error {
	return v.DecodeSSZ(buf)
}

func (v *Validator) DecodeSSZ(buf []byte) error {
	if len(buf) < v.EncodingSizeSSZ() {
		return ssz_utils.ErrLowBufferSize
	}
	copy(v.PublicKey[:], buf)
	copy(v.WithdrawalCredentials[:], buf[48:])
	v.EffectiveBalance = ssz_utils.UnmarshalUint64SSZ(buf[80:])
	v.Slashed = buf[88] == 1
	v.ActivationEligibilityEpoch = ssz_utils.UnmarshalUint64SSZ(buf[89:])
	v.ActivationEpoch = ssz_utils.UnmarshalUint64SSZ(buf[97:])
	v.ExitEpoch = ssz_utils.UnmarshalUint64SSZ(buf[105:])
	v.WithdrawableEpoch = ssz_utils.UnmarshalUint64SSZ(buf[113:])
	return nil
}

func (v *Validator) EncodingSizeSSZ() int {
	return 121
}

func (v *Validator) HashSSZ() ([32]byte, error) {
	var (
		leaves = make([][32]byte, 8)
		err    error
	)

	leaves[0], err = merkle_tree.PublicKeyRoot(v.PublicKey)
	if err != nil {
		return [32]byte{}, err
	}
	leaves[1] = v.WithdrawalCredentials
	leaves[2] = merkle_tree.Uint64Root(v.EffectiveBalance)
	leaves[3] = merkle_tree.BoolRoot(v.Slashed)
	leaves[4] = merkle_tree.Uint64Root(v.ActivationEligibilityEpoch)
	leaves[5] = merkle_tree.Uint64Root(v.ActivationEpoch)
	leaves[6] = merkle_tree.Uint64Root(v.ExitEpoch)
	leaves[7] = merkle_tree.Uint64Root(v.WithdrawableEpoch)
	return merkle_tree.ArraysRoot(leaves, 8)
}

// Active returns if validator is active for given epoch
func (v *Validator) Active(epoch uint64) bool {
	return v.ActivationEpoch <= epoch && epoch < v.ExitEpoch
}
