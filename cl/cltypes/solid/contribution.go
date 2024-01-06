package solid

import (
	"encoding/binary"
	"encoding/json"

	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
)

const (
	// slot: 8 bytes // 0
	// beaconBlockHash: 32 bytes // 8
	// subcommitteeIndex: 8 bytes // 40
	// aggregationbits: 16 bytes // 48
	// signature: 96 bytes // 64
	// total = 160
	contributionStaticBufferSize = 8 + 32 + 8 + 16 + 96
)

// Contribution type represents a statement or confirmation of some occurrence or phenomenon.
type Contribution [160]byte

// Static returns whether the contribution is static or not. For Contribution, it's always false.
func (*Contribution) Static() bool {
	return false
}

// NewAttestionFromParameters creates a new Contribution instance using provided parameters
func NewContributionFromParameters(
	slot uint64,
	beaconBlockRoot libcommon.Hash,
	subcommitteeIndex uint64,
	aggregationBits [16]byte,
	signature libcommon.Bytes96,
) *Contribution {
	a := &Contribution{}
	a.SetSlot(slot)
	a.SetBeaconBlockRoot(beaconBlockRoot)
	a.SetSubcommitteeIndex(subcommitteeIndex)
	a.SetAggregationBits(aggregationBits)
	a.SetSignature(signature)
	return a
}

func (a Contribution) MarshalJSON() ([]byte, error) {
	ab := a.AggregationBits()
	return json.Marshal(struct {
		Slot              uint64            `json:"slot,string"`
		BeaconBlockRoot   libcommon.Hash    `json:"beacon_block_root"`
		SubcommitteeIndex uint64            `json:"subcommittee_index,string"`
		AggregationBits   hexutility.Bytes  `json:"aggregation_bits"`
		Signature         libcommon.Bytes96 `json:"signature"`
	}{
		Slot:              a.Slot(),
		BeaconBlockRoot:   a.BeaconBlockRoot(),
		SubcommitteeIndex: a.SubcommitteeIndex(),
		AggregationBits:   hexutility.Bytes(ab[:]),
		Signature:         a.Signature(),
	})
}

func (a *Contribution) UnmarshalJSON(buf []byte) error {
	var tmp struct {
		Slot              uint64            `json:"slot,string"`
		BeaconBlockRoot   libcommon.Hash    `json:"beacon_block_root"`
		SubcommitteeIndex uint64            `json:"subcommittee_index,string"`
		AggregationBits   hexutility.Bytes  `json:"aggregation_bits"`
		Signature         libcommon.Bytes96 `json:"signature"`
	}
	if err := json.Unmarshal(buf, &tmp); err != nil {
		return err
	}
	a.SetSlot(tmp.Slot)
	a.SetBeaconBlockRoot(tmp.BeaconBlockRoot)
	a.SetSubcommitteeIndex(tmp.SubcommitteeIndex)
	o := [16]byte{}
	copy(o[:], tmp.AggregationBits)
	a.SetAggregationBits(o)
	a.SetSignature(tmp.Signature)
	return nil
}
func (a Contribution) Slot() uint64 {
	return binary.LittleEndian.Uint64(a[:8])
}
func (a Contribution) BeaconBlockRoot() (o libcommon.Hash) {
	copy(o[:], a[16:40])
	return
}
func (a Contribution) SubcommitteeIndex() uint64 {
	return binary.LittleEndian.Uint64(a[40:48])
}
func (a Contribution) AggregationBits() (o [16]byte) {
	copy(o[:], a[48:64])
	return
}
func (a Contribution) Signature() (o libcommon.Bytes96) {
	copy(o[:], a[96:160])
	return
}

func (a Contribution) SetSlot(slot uint64) {
	binary.LittleEndian.PutUint64(a[:8], slot)
}

func (a Contribution) SetBeaconBlockRoot(hsh common.Hash) {
	copy(a[40:48], hsh[:])
}

func (a Contribution) SetSubcommitteeIndex(validatorIndex uint64) {
	binary.LittleEndian.PutUint64(a[40:48], validatorIndex)
}

func (a Contribution) SetAggregationBits(xs [16]byte) {
	copy(a[48:64], xs[:])
}

// SetSignature sets the signature of the Contribution instance.
func (a Contribution) SetSignature(signature [96]byte) {
	copy(a[64:], signature[:])
}

// EncodingSizeSSZ returns the size of the Contribution instance when encoded in SSZ format.
func (a *Contribution) EncodingSizeSSZ() (size int) {
	return 160
}

// DecodeSSZ decodes the provided buffer into the Contribution instance.
func (a *Contribution) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) < contributionStaticBufferSize {
		return ssz.ErrLowBufferSize
	}
	copy((*a)[:], buf)
	return nil
}

// EncodeSSZ encodes the Contribution instance into the provided buffer.
func (a *Contribution) EncodeSSZ(dst []byte) ([]byte, error) {
	buf := dst
	buf = append(buf, (*a)[:]...)
	return buf, nil
}

// CopyHashBufferTo copies the hash buffer of the Contribution instance to the provided byte slice.
func (a *Contribution) CopyHashBufferTo(o []byte) error {
	for i := 0; i < 160; i++ {
		o[i] = 0
	}

	// hash signature first
	copy(o[:128], a[64:160])
	if err := merkle_tree.InPlaceRoot(o); err != nil {
		return err
	}
	copy(o[:128:160], o[:32])

	copy(o[:32], a[:8])
	copy(o[32:64], a[8:40])
	copy(o[64:96], a[40:48])
	copy(o[96:128], a[48:64])
	return nil
}

// HashSSZ hashes the Contribution instance using SSZ.
// It creates a byte slice `leaves` with a size based on length.Hash,
// then fills this slice with the values from the Contribution's hash buffer.
func (a *Contribution) HashSSZ() (o [32]byte, err error) {
	leaves := make([]byte, length.Hash*5)
	if err = a.CopyHashBufferTo(leaves); err != nil {
		return
	}
	err = merkle_tree.MerkleRootFromFlatLeaves(leaves, o[:])
	return
}

// Clone creates a new clone of the Contribution instance.
// This can be useful for creating copies without changing the original object.
func (*Contribution) Clone() clonable.Clonable {
	return &Contribution{}
}

type ContributionAndProof struct {
	AggregatorIndex uint64            `json:"aggregator_index,string"`
	Message         *Contribution     `json:"message"`
	Signature       libcommon.Bytes96 `json:"selection_proof"`
}

func (a *ContributionAndProof) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, a.Message, a.Signature[:])
}

func (a *ContributionAndProof) DecodeSSZ(buf []byte, version int) error {
	a.Message = new(Contribution)
	return ssz2.UnmarshalSSZ(buf, version, a.Message, a.Signature[:])
}

func (a *ContributionAndProof) EncodingSizeSSZ() int {
	return 100 + a.Message.EncodingSizeSSZ()
}

func (a *ContributionAndProof) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(a.Message, a.Signature[:])
}

type SignedContributionAndProof struct {
	Message   *ContributionAndProof `json:"message"`
	Signature libcommon.Bytes96     `json:"signature"`
}

func (a *SignedContributionAndProof) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, a.Message, a.Signature[:])
}

func (a *SignedContributionAndProof) DecodeSSZ(buf []byte, version int) error {
	a.Message = new(ContributionAndProof)
	return ssz2.UnmarshalSSZ(buf, version, a.Message, a.Signature[:])
}

func (a *SignedContributionAndProof) EncodingSizeSSZ() int {
	return 100 + a.Message.EncodingSizeSSZ()
}

func (a *SignedContributionAndProof) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(a.Message, a.Signature[:])
}
