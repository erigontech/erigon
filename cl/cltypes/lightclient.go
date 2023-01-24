package cltypes

import (
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/ssz_utils"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/core/types"
)

const (
	SyncCommitteeBranchLength = 5
	FinalityBranchLength      = 6
	ExecutionBranchLength     = 4
)

type LightClientHeader struct {
	HeaderEth2      *BeaconBlockHeader
	HeaderEth1      *types.Header
	ExecutionBranch [ExecutionBranchLength]libcommon.Hash
	version         clparams.StateVersion
}

func (l *LightClientHeader) DecodeSSZ([]byte) error {
	panic("not implemnted")
}

func (l *LightClientHeader) DecodeSSZWithVersion(buf []byte, v int) error {
	var (
		err error
	)
	l.version = clparams.StateVersion(v)
	l.HeaderEth2 = new(BeaconBlockHeader)
	if err = l.HeaderEth2.DecodeSSZ(buf); err != nil {
		return err
	}
	if l.version <= clparams.BellatrixVersion {
		return nil
	}
	pos := l.HeaderEth2.EncodingSizeSSZ() + 4 // Skip the offset, assume it is at the end.
	// Decode branch
	for i := range l.ExecutionBranch {
		copy(l.ExecutionBranch[i][:], buf[pos:])
		pos += length.Hash
	}
	l.HeaderEth1 = new(types.Header)
	return l.HeaderEth1.DecodeSSZ(buf[pos:])
}

func (l *LightClientHeader) EncodeSSZ(buf []byte) ([]byte, error) {
	var (
		err error
		dst = buf
	)
	if dst, err = l.HeaderEth2.EncodeSSZ(dst); err != nil {
		return nil, err
	}
	// Pre-capella is easy, encode only header.
	if l.version < clparams.CapellaVersion {
		return dst, nil
	}
	// Post-Capella
	offset := uint32(l.HeaderEth2.EncodingSizeSSZ() + len(l.ExecutionBranch)*length.Hash + 4)
	dst = append(dst, ssz_utils.OffsetSSZ(offset)...)
	for _, root := range l.ExecutionBranch {
		dst = append(dst, root[:]...)
	}
	return l.HeaderEth1.EncodeSSZ(dst)
}

func (l *LightClientHeader) EncodingSize() int {
	size := 112
	if l.version >= clparams.CapellaVersion {
		if l.HeaderEth1 == nil {
			l.HeaderEth1 = new(types.Header)
		}
		size += l.HeaderEth1.EncodingSize() + 4
		size += length.Hash * 5
	}
	return size
}

// LightClientBootstrap is used to bootstrap the lightclient from checkpoint sync.
type LightClientBootstrap struct {
	Header                     *BeaconBlockHeader
	CurrentSyncCommittee       *SyncCommittee
	CurrentSyncCommitteeBranch []libcommon.Hash
}

func (l *LightClientBootstrap) DecodeSSZWithVersion(buf []byte, _ int) error {
	return l.DecodeSSZ(buf)
}

// EncodeSSZ ssz marshals the LightClientBootstrap object
func (l *LightClientBootstrap) EncodeSSZ(dst []byte) ([]byte, error) {
	buf := dst
	var err error
	if buf, err = l.Header.EncodeSSZ(buf); err != nil {
		return nil, err
	}

	if buf, err = l.CurrentSyncCommittee.EncodeSSZ(buf); err != nil {
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

// DecodeSSZ ssz unmarshals the LightClientBootstrap object
func (l *LightClientBootstrap) DecodeSSZ(buf []byte) error {
	var err error
	if len(buf) < l.EncodingSizeSSZ() {
		return ssz_utils.ErrLowBufferSize
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

// EncodingSizeSSZ returns the ssz encoded size in bytes for the LightClientBootstrap object
func (l *LightClientBootstrap) EncodingSizeSSZ() (size int) {
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

func (l *LightClientUpdate) DecodeSSZWithVersion(buf []byte, _ int) error {
	return l.DecodeSSZ(buf)
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
func (l *LightClientUpdate) EncodeSSZ(buf []byte) ([]byte, error) {
	dst := buf
	var err error
	// Generic Update Specific
	if dst, err = l.AttestedHeader.EncodeSSZ(dst); err != nil {
		return nil, err
	}
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
	var err error
	if dst, err = finalizedHeader.EncodeSSZ(dst); err != nil {
		return nil, err
	}
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

func (l *LightClientFinalityUpdate) DecodeSSZWithVersion(buf []byte, _ int) error {
	return l.DecodeSSZ(buf)
}

// DecodeSSZ ssz unmarshals the LightClientUpdate object
func (l *LightClientUpdate) DecodeSSZ(buf []byte) error {
	var err error
	if len(buf) < l.EncodingSizeSSZ() {
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

// EncodingSizeSSZ returns the ssz encoded size in bytes for the LightClientUpdate object
func (l *LightClientUpdate) EncodingSizeSSZ() int {
	return 25368
}

func (l *LightClientFinalityUpdate) EncodeSSZ(buf []byte) ([]byte, error) {
	dst := buf
	var err error

	if dst, err = l.AttestedHeader.EncodeSSZ(dst); err != nil {
		return nil, err
	}

	if dst, err = encodeFinalityUpdateSpecificField(dst, l.FinalizedHeader, l.FinalityBranch); err != nil {
		return nil, err
	}
	dst = encodeUpdateFooter(dst, l.SyncAggregate, l.SignatureSlot)

	return dst, nil
}

// DecodeSSZ ssz unmarshals the LightClientFinalityUpdate object
func (l *LightClientFinalityUpdate) DecodeSSZ(buf []byte) error {
	var err error
	if len(buf) < l.EncodingSizeSSZ() {
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

func (l *LightClientFinalityUpdate) EncodingSizeSSZ() int {
	return 584
}

// LightClientOptimisticUpdate is used for verifying N-1 block.
type LightClientOptimisticUpdate struct {
	AttestedHeader *BeaconBlockHeader
	SyncAggregate  *SyncAggregate
	SignatureSlot  uint64
}

func (l *LightClientOptimisticUpdate) DecodeSSZWithVersion(buf []byte, _ int) error {
	return l.DecodeSSZ(buf)
}

func (l *LightClientOptimisticUpdate) EncodeSSZ(buf []byte) ([]byte, error) {
	dst := buf
	var err error
	if dst, err = l.AttestedHeader.EncodeSSZ(dst); err != nil {
		return nil, err
	}
	dst = encodeUpdateFooter(dst, l.SyncAggregate, l.SignatureSlot)

	return dst, nil
}

func (l *LightClientOptimisticUpdate) DecodeSSZ(buf []byte) error {
	var err error
	if len(buf) < l.EncodingSizeSSZ() {
		return ssz_utils.ErrLowBufferSize
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

func (l *LightClientOptimisticUpdate) EncodingSizeSSZ() (size int) {
	return 280
}
