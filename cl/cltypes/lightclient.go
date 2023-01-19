package cltypes

import (
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes/ssz_utils"
	"github.com/ledgerwatch/erigon/cl/utils"
	ssz "github.com/prysmaticlabs/fastssz"
)

const (
	SyncCommitteeBranchLength = 5
	FinalityBranchLength      = 6
)

// LightClientBootstrap is used to bootstrap the lightclient from checkpoint sync.
type LightClientBootstrap struct {
	Header                     *BeaconBlockHeader
	CurrentSyncCommittee       *SyncCommittee
	CurrentSyncCommitteeBranch []libcommon.Hash
}

func (l *LightClientBootstrap) UnmarshalSSZWithVersion(buf []byte, _ int) error {
	return l.UnmarshalSSZ(buf)
}

// MarshalSSZ ssz marshals the LightClientBootstrap object
func (l *LightClientBootstrap) MarshalSSZ() ([]byte, error) {
	buf := make([]byte, 0, l.SizeSSZ())
	var err error
	buf = l.Header.EncodeSSZ(buf)
	buf, err = l.CurrentSyncCommittee.EncodeSSZ(buf)
	if err != nil {
		return nil, err
	}
	if len(l.CurrentSyncCommitteeBranch) != SyncCommitteeBranchLength {
		return nil, fmt.Errorf("sync committee branch is invalid")
	}
	for _, leaf := range l.CurrentSyncCommitteeBranch {
		buf = append(buf, leaf[:]...)
	}
	return buf, nil
}

// UnmarshalSSZ ssz unmarshals the LightClientBootstrap object
func (l *LightClientBootstrap) UnmarshalSSZ(buf []byte) error {
	var err error
	if len(buf) < l.SizeSSZ() {
		return ssz.ErrSize
	}

	l.Header = new(BeaconBlockHeader)

	if err = l.Header.DecodeSSZ(buf[0:112]); err != nil {
		return err
	}
	l.CurrentSyncCommittee = new(SyncCommittee)
	if err = l.CurrentSyncCommittee.DecodeSSZ(buf[112:24736]); err != nil {
		return err
	}
	pos := 24736

	l.CurrentSyncCommitteeBranch = make([]libcommon.Hash, 5)
	for i := range l.CurrentSyncCommitteeBranch {
		copy(l.CurrentSyncCommitteeBranch[i][:], buf[pos:pos+32])
		pos += 32
	}

	return err
}

// SizeSSZ returns the ssz encoded size in bytes for the LightClientBootstrap object
func (l *LightClientBootstrap) SizeSSZ() (size int) {
	size = 24896
	return
}

// LightClientUpdate is used to update the sync committee every 27 hours.
type LightClientUpdate struct {
	AttestedHeader          *BeaconBlockHeader
	NextSyncCommitee        *SyncCommittee
	NextSyncCommitteeBranch []libcommon.Hash
	FinalizedHeader         *BeaconBlockHeader
	FinalityBranch          []libcommon.Hash
	SyncAggregate           *SyncAggregate
	SignatureSlot           uint64
}

func (l *LightClientUpdate) UnmarshalSSZWithVersion(buf []byte, _ int) error {
	return l.UnmarshalSSZ(buf)
}

func (l *LightClientUpdate) HasNextSyncCommittee() bool {
	return l.NextSyncCommitee != nil
}

func (l *LightClientUpdate) IsFinalityUpdate() bool {
	return l.FinalityBranch != nil
}

func (l *LightClientUpdate) HasSyncFinality() bool {
	return l.FinalizedHeader != nil &&
		utils.SlotToPeriod(l.AttestedHeader.Slot) == utils.SlotToPeriod(l.FinalizedHeader.Slot)
}

// MarshalSSZTo ssz marshals the LightClientUpdate object to a target array
func (l *LightClientUpdate) MarshalSSZ() ([]byte, error) {
	dst := make([]byte, 0, l.SizeSSZ())
	var err error
	// Generic Update Specific
	dst = l.AttestedHeader.EncodeSSZ(dst)
	if dst, err = l.NextSyncCommitee.EncodeSSZ(dst); err != nil {
		return nil, err
	}

	if len(l.NextSyncCommitteeBranch) != SyncCommitteeBranchLength {
		return nil, fmt.Errorf("invalid size for nextSyncCommitteeBranch")
	}
	for _, leaf := range l.NextSyncCommitteeBranch {
		dst = append(dst, leaf[:]...)
	}

	dst, err = encodeFinalityUpdateSpecificField(dst, l.FinalizedHeader, l.FinalityBranch)
	if err != nil {
		return nil, err
	}
	dst = encodeUpdateFooter(dst, l.SyncAggregate, l.SignatureSlot)
	return dst, nil
}

func encodeFinalityUpdateSpecificField(buf []byte, finalizedHeader *BeaconBlockHeader, finalityBranch []libcommon.Hash) ([]byte, error) {
	dst := buf
	dst = finalizedHeader.EncodeSSZ(dst)
	if len(finalityBranch) != FinalityBranchLength {
		return nil, fmt.Errorf("invalid finality branch length")
	}
	for _, leaf := range finalityBranch {
		dst = append(dst, leaf[:]...)
	}
	return dst, nil
}

func encodeUpdateFooter(buf []byte, aggregate *SyncAggregate, signatureSlot uint64) []byte {
	dst := buf
	dst = aggregate.EncodeSSZ(dst)
	return append(dst, ssz_utils.Uint64SSZ(signatureSlot)...)
}

func decodeFinalityUpdateSpecificField(buf []byte) (*BeaconBlockHeader, []libcommon.Hash, error) {
	header := &BeaconBlockHeader{}
	if err := header.DecodeSSZ(buf); err != nil {
		return nil, nil, err
	}
	finalityBranch := make([]libcommon.Hash, FinalityBranchLength)
	startBranch := 112
	for i := range finalityBranch {
		copy(finalityBranch[i][:], buf[startBranch+i*32:])
	}
	return header, finalityBranch, nil
}

func decodeUpdateFooter(buf []byte) (*SyncAggregate, uint64, error) {
	aggregate := &SyncAggregate{}
	if err := aggregate.DecodeSSZ(buf); err != nil {
		return nil, 0, err
	}
	return aggregate, ssz_utils.UnmarshalUint64SSZ(buf[aggregate.EncodingSizeSSZ():]), nil
}

// LightClientFinalityUpdate is used to update the sync aggreggate every 6 minutes.
type LightClientFinalityUpdate struct {
	AttestedHeader  *BeaconBlockHeader
	FinalizedHeader *BeaconBlockHeader
	FinalityBranch  []libcommon.Hash `ssz-size:"6,32"`
	SyncAggregate   *SyncAggregate
	SignatureSlot   uint64
}

func (l *LightClientFinalityUpdate) UnmarshalSSZWithVersion(buf []byte, _ int) error {
	return l.UnmarshalSSZ(buf)
}

// UnmarshalSSZ ssz unmarshals the LightClientUpdate object
func (l *LightClientUpdate) UnmarshalSSZ(buf []byte) error {
	var err error
	if len(buf) < l.SizeSSZ() {
		return ssz_utils.ErrBadOffset
	}

	l.AttestedHeader = new(BeaconBlockHeader)

	if err = l.AttestedHeader.DecodeSSZ(buf[0:112]); err != nil {
		return err
	}

	// Field (1) 'NextSyncCommitee'
	if l.NextSyncCommitee == nil {
		l.NextSyncCommitee = new(SyncCommittee)
	}
	if err = l.NextSyncCommitee.DecodeSSZ(buf[112:24736]); err != nil {
		return err
	}

	l.NextSyncCommitteeBranch = make([]libcommon.Hash, SyncCommitteeBranchLength)
	pos := 24736
	for i := range l.NextSyncCommitteeBranch {
		copy(l.NextSyncCommitteeBranch[i][:], buf[pos:])
		pos += 32
	}

	l.FinalizedHeader, l.FinalityBranch, err = decodeFinalityUpdateSpecificField(buf[24896:])
	if err != nil {
		return err
	}

	l.SyncAggregate, l.SignatureSlot, err = decodeUpdateFooter(buf[25200:])
	fmt.Println(l.SignatureSlot)
	return err
}

// SizeSSZ returns the ssz encoded size in bytes for the LightClientUpdate object
func (l *LightClientUpdate) SizeSSZ() int {
	return 25368
}

func (l *LightClientFinalityUpdate) MarshalSSZ() ([]byte, error) {
	dst := make([]byte, 0, l.SizeSSZ())
	var err error

	dst = l.AttestedHeader.EncodeSSZ(dst)

	dst, err = encodeFinalityUpdateSpecificField(dst, l.FinalizedHeader, l.FinalityBranch)
	if err != nil {
		return nil, err
	}
	dst = encodeUpdateFooter(dst, l.SyncAggregate, l.SignatureSlot)

	return dst, nil
}

// UnmarshalSSZ ssz unmarshals the LightClientFinalityUpdate object
func (l *LightClientFinalityUpdate) UnmarshalSSZ(buf []byte) error {
	var err error
	if len(buf) < l.SizeSSZ() {
		return ssz_utils.ErrBadOffset
	}

	l.AttestedHeader = new(BeaconBlockHeader)
	if err = l.AttestedHeader.DecodeSSZ(buf[0:112]); err != nil {
		return err
	}

	l.FinalizedHeader, l.FinalityBranch, err = decodeFinalityUpdateSpecificField(buf[112:416])
	if err != nil {
		return err
	}

	l.SyncAggregate, l.SignatureSlot, err = decodeUpdateFooter(buf[416:584])
	if err != nil {
		return err
	}

	return err
}

func (l *LightClientFinalityUpdate) SizeSSZ() int {
	return 584
}

// LightClientOptimisticUpdate is used for verifying N-1 block.
type LightClientOptimisticUpdate struct {
	AttestedHeader *BeaconBlockHeader
	SyncAggregate  *SyncAggregate
	SignatureSlot  uint64
}

func (l *LightClientOptimisticUpdate) UnmarshalSSZWithVersion(buf []byte, _ int) error {
	return l.UnmarshalSSZ(buf)
}

func (l *LightClientOptimisticUpdate) MarshalSSZ() ([]byte, error) {
	dst := make([]byte, 0, l.SizeSSZ())

	dst = l.AttestedHeader.EncodeSSZ(dst)
	dst = encodeUpdateFooter(dst, l.SyncAggregate, l.SignatureSlot)

	return dst, nil
}

func (l *LightClientOptimisticUpdate) UnmarshalSSZ(buf []byte) error {
	var err error
	if len(buf) < l.SizeSSZ() {
		return ssz.ErrSize
	}

	// Field (0) 'AttestedHeader'
	if l.AttestedHeader == nil {
		l.AttestedHeader = new(BeaconBlockHeader)
	}
	if err = l.AttestedHeader.DecodeSSZ(buf[0:112]); err != nil {
		return err
	}
	l.SyncAggregate, l.SignatureSlot, err = decodeUpdateFooter(buf[112:])
	return err
}

func (l *LightClientOptimisticUpdate) SizeSSZ() (size int) {
	return 280
}
