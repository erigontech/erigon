package cltypes

import (
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/ssz_utils"
	"github.com/ledgerwatch/erigon/cl/utils"
)

const (
	SyncCommitteeBranchLength = 5
	FinalityBranchLength      = 6
	ExecutionBranchLength     = 4
)

type LightClientHeader struct {
	HeaderEth2      *BeaconBlockHeader
	HeaderEth1      *Eth1Header
	ExecutionBranch [ExecutionBranchLength]libcommon.Hash
	version         clparams.StateVersion
}

func (l *LightClientHeader) WithVersion(v clparams.StateVersion) *LightClientHeader {
	l.version = v
	l.HeaderEth1.version = v
	return l
}

func (l *LightClientHeader) DecodeSSZ([]byte) error {
	panic("not implemnted")
}

func (l *LightClientHeader) DecodeSSZWithVersion(buf []byte, v int) error {
	var err error
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
	l.HeaderEth1 = new(Eth1Header)
	return l.HeaderEth1.DecodeSSZWithVersion(buf[pos:], int(l.version))
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

func (l *LightClientHeader) EncodingSizeSSZ() int {
	if l.HeaderEth2 == nil {
		l.HeaderEth2 = new(BeaconBlockHeader)
	}
	size := l.HeaderEth2.EncodingSizeSSZ()
	if l.version >= clparams.CapellaVersion {
		if l.HeaderEth1 == nil {
			l.HeaderEth1 = NewEth1Header(l.version)
		}
		size += l.HeaderEth1.EncodingSizeSSZ() + 4
		size += length.Hash * len(l.ExecutionBranch)
	}
	return size
}

// LightClientBootstrap is used to bootstrap the lightclient from checkpoint sync.
type LightClientBootstrap struct {
	Header                     *LightClientHeader
	CurrentSyncCommittee       *SyncCommittee
	CurrentSyncCommitteeBranch []libcommon.Hash
	version                    clparams.StateVersion
}

func (l *LightClientBootstrap) WithVersion(v clparams.StateVersion) *LightClientBootstrap {
	l.version = v
	return l
}

func (l *LightClientBootstrap) DecodeSSZ(buf []byte) error {
	panic("AAAAAA")
}

// EncodeSSZ ssz marshals the LightClientBootstrap object
func (l *LightClientBootstrap) EncodeSSZ(dst []byte) ([]byte, error) {
	buf := dst
	var err error
	offset := l.CurrentSyncCommittee.EncodingSizeSSZ() + len(l.CurrentSyncCommitteeBranch)*length.Hash + 4
	if l.version < clparams.CapellaVersion {
		if buf, err = l.Header.EncodeSSZ(buf); err != nil {
			return nil, err
		}
	} else {
		buf = append(buf, ssz_utils.OffsetSSZ(uint32(offset))...)
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
	if l.version >= clparams.CapellaVersion {
		if buf, err = l.Header.EncodeSSZ(buf); err != nil {
			return nil, err
		}
	}
	return buf, nil
}

// DecodeSSZ ssz unmarshals the LightClientBootstrap object
func (l *LightClientBootstrap) DecodeSSZWithVersion(buf []byte, version int) error {
	var err error
	l.version = clparams.StateVersion(version)

	if len(buf) < l.EncodingSizeSSZ() {
		return ssz_utils.ErrLowBufferSize
	}
	l.Header = new(LightClientHeader)
	l.CurrentSyncCommittee = new(SyncCommittee)
	l.CurrentSyncCommitteeBranch = make([]libcommon.Hash, 5)

	pos := 0
	if l.version >= clparams.CapellaVersion {
		pos += 4
	} else {
		if err = l.Header.DecodeSSZWithVersion(buf[pos:], int(l.version)); err != nil {
			return err
		}
		pos += l.Header.EncodingSizeSSZ()
	}

	if err = l.CurrentSyncCommittee.DecodeSSZ(buf[pos:]); err != nil {
		return err
	}
	pos += l.CurrentSyncCommittee.EncodingSizeSSZ()

	for i := range l.CurrentSyncCommitteeBranch {
		copy(l.CurrentSyncCommitteeBranch[i][:], buf[pos:pos+32])
		pos += 32
	}
	if l.version >= clparams.CapellaVersion {
		if err = l.Header.DecodeSSZWithVersion(buf[pos:], int(l.version)); err != nil {
			return err
		}
	}

	return err
}

// EncodingSizeSSZ returns the ssz encoded size in bytes for the LightClientBootstrap object
func (l *LightClientBootstrap) EncodingSizeSSZ() (size int) {
	if l.Header == nil {
		l.Header = &LightClientHeader{version: l.version}
	}
	size = 0
	size += l.Header.EncodingSizeSSZ()
	if l.version >= clparams.CapellaVersion {
		size += 4
	}
	if l.CurrentSyncCommittee == nil {
		l.CurrentSyncCommittee = new(SyncCommittee)
		l.CurrentSyncCommittee.PubKeys = make([][48]byte, SyncCommitteeSize)
	}

	size += l.CurrentSyncCommittee.EncodingSizeSSZ()

	if len(l.CurrentSyncCommitteeBranch) == 0 {
		l.CurrentSyncCommitteeBranch = make([]libcommon.Hash, SyncCommitteeBranchLength)
	}
	size += len(l.CurrentSyncCommitteeBranch) * length.Hash

	return
}

// LightClientUpdate is used to update the sync committee every 27 hours.
type LightClientUpdate struct {
	AttestedHeader          *LightClientHeader
	NextSyncCommitee        *SyncCommittee
	NextSyncCommitteeBranch []libcommon.Hash
	FinalizedHeader         *LightClientHeader
	FinalityBranch          []libcommon.Hash
	SyncAggregate           *SyncAggregate
	SignatureSlot           uint64

	version clparams.StateVersion
}

func (l *LightClientUpdate) WithVersion(v clparams.StateVersion) *LightClientUpdate {
	l.version = v
	return l
}

func (l *LightClientUpdate) DecodeSSZ(buf []byte) error {
	panic("OOOH")
}

func (l *LightClientUpdate) HasNextSyncCommittee() bool {
	return l.NextSyncCommitee != nil
}

func (l *LightClientUpdate) IsFinalityUpdate() bool {
	return l.FinalityBranch != nil
}

func (l *LightClientUpdate) HasSyncFinality() bool {
	return l.FinalizedHeader != nil &&
		utils.SlotToPeriod(l.AttestedHeader.HeaderEth2.Slot) == utils.SlotToPeriod(l.FinalizedHeader.HeaderEth2.Slot)
}

// MarshalSSZTo ssz marshals the LightClientUpdate object to a target array
func (l *LightClientUpdate) EncodeSSZ(buf []byte) ([]byte, error) {
	dst := buf
	var err error
	offset := 8 + l.NextSyncCommitee.EncodingSizeSSZ() + len(l.NextSyncCommitteeBranch)*length.Hash +
		len(l.FinalityBranch)*length.Hash + l.SyncAggregate.EncodingSizeSSZ() + 8

	if l.version >= clparams.CapellaVersion {
		dst = append(dst, ssz_utils.OffsetSSZ(uint32(offset))...)
		offset += l.AttestedHeader.EncodingSizeSSZ()
	} else {
		// Generic Update Specific
		if dst, err = l.AttestedHeader.EncodeSSZ(dst); err != nil {
			return nil, err
		}
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

	dst, err = encodeFinalityUpdateSpecificField(dst, l.FinalizedHeader, l.FinalityBranch, uint32(offset))
	if err != nil {
		return nil, err
	}

	dst = encodeUpdateFooter(dst, l.SyncAggregate, l.SignatureSlot)
	if l.version >= clparams.CapellaVersion {
		if dst, err = l.AttestedHeader.EncodeSSZ(dst); err != nil {
			return nil, err
		}
		if dst, err = l.FinalizedHeader.EncodeSSZ(dst); err != nil {
			return nil, err
		}
	}
	return dst, nil
}

func encodeFinalityUpdateSpecificField(buf []byte, finalizedHeader *LightClientHeader, finalityBranch []libcommon.Hash, offset uint32) ([]byte, error) {
	dst := buf
	var err error
	if finalizedHeader.version >= clparams.CapellaVersion {
		dst = append(dst, ssz_utils.OffsetSSZ(offset)...)
	} else {
		if dst, err = finalizedHeader.EncodeSSZ(dst); err != nil {
			return nil, err
		}
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

func decodeFinalityUpdateSpecificField(buf []byte, version clparams.StateVersion) (*LightClientHeader, []libcommon.Hash, uint32, int, error) {
	header := &LightClientHeader{}
	var offset uint32
	pos := 0
	if version < clparams.CapellaVersion {
		if err := header.DecodeSSZWithVersion(buf, int(version)); err != nil {
			return nil, nil, 0, 0, err
		}
		pos += header.EncodingSizeSSZ()
	} else {
		offset = ssz_utils.DecodeOffset(buf)
		pos += 4
	}

	finalityBranch := make([]libcommon.Hash, FinalityBranchLength)
	for i := range finalityBranch {
		copy(finalityBranch[i][:], buf[pos:])
		pos += length.Hash
	}
	return header, finalityBranch, offset, pos, nil
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
	AttestedHeader  *LightClientHeader
	FinalizedHeader *LightClientHeader
	FinalityBranch  []libcommon.Hash `ssz-size:"6,32"`
	SyncAggregate   *SyncAggregate
	SignatureSlot   uint64

	version clparams.StateVersion
}

func (l *LightClientFinalityUpdate) WithVersion(v clparams.StateVersion) *LightClientFinalityUpdate {
	l.version = v
	return l
}

func (l *LightClientFinalityUpdate) DecodeSSZ(buf []byte) error {
	panic("OOOOOOOOOOOOO")
}

// DecodeSSZ ssz unmarshals the LightClientUpdate object
func (l *LightClientUpdate) DecodeSSZWithVersion(buf []byte, version int) error {
	var err error
	l.version = clparams.StateVersion(version)
	if len(buf) < l.EncodingSizeSSZ() {
		return ssz_utils.ErrLowBufferSize
	}

	l.AttestedHeader = new(LightClientHeader)
	l.NextSyncCommitee = new(SyncCommittee)
	l.NextSyncCommitteeBranch = make([]libcommon.Hash, SyncCommitteeBranchLength)
	l.FinalizedHeader = new(LightClientHeader)
	l.SyncAggregate = new(SyncAggregate)

	var (
		pos             int
		offsetAttested  uint32
		offsetFinalized uint32
	)

	if l.version >= clparams.CapellaVersion {
		offsetAttested = ssz_utils.DecodeOffset(buf)
		pos += 4
	} else {
		if err = l.AttestedHeader.DecodeSSZWithVersion(buf, version); err != nil {
			return err
		}
		pos += l.AttestedHeader.EncodingSizeSSZ()
	}

	if err = l.NextSyncCommitee.DecodeSSZ(buf[pos:]); err != nil {
		return err
	}
	pos += l.NextSyncCommitee.EncodingSizeSSZ()
	for i := range l.NextSyncCommitteeBranch {
		copy(l.NextSyncCommitteeBranch[i][:], buf[pos:])
		pos += 32
	}

	var written int
	l.FinalizedHeader, l.FinalityBranch, offsetFinalized, written, err = decodeFinalityUpdateSpecificField(buf[pos:], l.version)
	if err != nil {
		return err
	}
	pos += written

	l.SyncAggregate, l.SignatureSlot, err = decodeUpdateFooter(buf[pos:])
	if l.version >= clparams.CapellaVersion {
		if offsetAttested > offsetFinalized || offsetFinalized > uint32(len(buf)) {
			return ssz_utils.ErrBadOffset
		}
		if err = l.AttestedHeader.DecodeSSZWithVersion(buf[offsetAttested:offsetFinalized], version); err != nil {
			return err
		}
		if err = l.FinalizedHeader.DecodeSSZWithVersion(buf[offsetFinalized:], version); err != nil {
			return err
		}
	}
	return err
}

// EncodingSizeSSZ returns the ssz encoded size in bytes for the LightClientUpdate object
func (l *LightClientUpdate) EncodingSizeSSZ() int {
	size := 0
	if l.version >= clparams.CapellaVersion {
		size += 8
	}
	if l.AttestedHeader == nil {
		l.AttestedHeader = &LightClientHeader{version: l.version}
	}
	if l.NextSyncCommitee == nil {
		l.NextSyncCommitee = new(SyncCommittee)
		l.NextSyncCommitee.PubKeys = make([][48]byte, 512)
	}
	if l.FinalizedHeader == nil {
		l.FinalizedHeader = &LightClientHeader{version: l.version}
	}
	if l.SyncAggregate == nil {
		l.SyncAggregate = new(SyncAggregate)
	}
	if len(l.NextSyncCommitteeBranch) == 0 {
		l.NextSyncCommitteeBranch = make([]libcommon.Hash, SyncCommitteeBranchLength)
	}
	if len(l.FinalityBranch) == 0 {
		l.FinalityBranch = make([]libcommon.Hash, FinalityBranchLength)
	}
	return size + l.AttestedHeader.EncodingSizeSSZ() + l.FinalizedHeader.EncodingSizeSSZ() + l.SyncAggregate.EncodingSizeSSZ() +
		8 + len(l.FinalityBranch)*length.Hash + len(l.NextSyncCommitteeBranch)*length.Hash + l.NextSyncCommitee.EncodingSizeSSZ()
}

func (l *LightClientFinalityUpdate) EncodeSSZ(buf []byte) ([]byte, error) {
	dst := buf
	var err error
	offset := 8 + len(l.FinalityBranch)*length.Hash + l.SyncAggregate.EncodingSizeSSZ() + 8
	if l.version >= clparams.CapellaVersion {
		dst = append(dst, ssz_utils.OffsetSSZ(uint32(offset))...)
		offset += l.AttestedHeader.EncodingSizeSSZ()
	} else {
		// Generic Update Specific
		if dst, err = l.AttestedHeader.EncodeSSZ(dst); err != nil {
			return nil, err
		}
	}

	dst, err = encodeFinalityUpdateSpecificField(dst, l.FinalizedHeader, l.FinalityBranch, uint32(offset))
	if err != nil {
		return nil, err
	}
	dst = encodeUpdateFooter(dst, l.SyncAggregate, l.SignatureSlot)
	if l.version >= clparams.CapellaVersion {
		if dst, err = l.AttestedHeader.EncodeSSZ(dst); err != nil {
			return nil, err
		}
		if dst, err = l.FinalizedHeader.EncodeSSZ(dst); err != nil {
			return nil, err
		}
	}
	return dst, nil
}

// DecodeSSZ ssz unmarshals the LightClientFinalityUpdate object
func (l *LightClientFinalityUpdate) DecodeSSZWithVersion(buf []byte, version int) error {
	var err error
	l.version = clparams.StateVersion(version)
	if len(buf) < l.EncodingSizeSSZ() {
		return ssz_utils.ErrBadOffset
	}

	pos := 0
	l.AttestedHeader = new(LightClientHeader)

	var offsetAttested, offsetFinalized uint32
	if l.version < clparams.CapellaVersion {
		if err = l.AttestedHeader.DecodeSSZWithVersion(buf, int(l.version)); err != nil {
			return err
		}
		pos += l.AttestedHeader.EncodingSizeSSZ()
	} else {
		offsetAttested = ssz_utils.DecodeOffset(buf)
		pos += 4
	}

	var written int
	l.FinalizedHeader, l.FinalityBranch, offsetFinalized, written, err = decodeFinalityUpdateSpecificField(buf[pos:], l.version)
	if err != nil {
		return err
	}
	pos += written

	l.SyncAggregate, l.SignatureSlot, err = decodeUpdateFooter(buf[pos:])
	if err != nil {
		return err
	}
	if l.version >= clparams.CapellaVersion {
		if offsetAttested > offsetFinalized || offsetFinalized > uint32(len(buf)) {
			return ssz_utils.ErrBadOffset
		}
		if err = l.AttestedHeader.DecodeSSZWithVersion(buf[offsetAttested:offsetFinalized], version); err != nil {
			return err
		}
		if err = l.FinalizedHeader.DecodeSSZWithVersion(buf[offsetFinalized:], version); err != nil {
			return err
		}
	}
	return err
}

func (l *LightClientFinalityUpdate) EncodingSizeSSZ() int {
	size := 0
	if l.version >= clparams.CapellaVersion {
		size += 8
	}
	if l.AttestedHeader == nil {
		l.AttestedHeader = &LightClientHeader{version: l.version}
	}
	if l.FinalizedHeader == nil {
		l.FinalizedHeader = &LightClientHeader{version: l.version}
	}
	if l.SyncAggregate == nil {
		l.SyncAggregate = new(SyncAggregate)
	}
	if len(l.FinalityBranch) == 0 {
		l.FinalityBranch = make([]libcommon.Hash, FinalityBranchLength)
	}
	return size + l.AttestedHeader.EncodingSizeSSZ() + l.FinalizedHeader.EncodingSizeSSZ() + l.SyncAggregate.EncodingSizeSSZ() +
		8 + len(l.FinalityBranch)*length.Hash

}

// LightClientOptimisticUpdate is used for verifying N-1 block.
type LightClientOptimisticUpdate struct {
	AttestedHeader *LightClientHeader
	SyncAggregate  *SyncAggregate
	SignatureSlot  uint64

	version clparams.StateVersion
}

func (l *LightClientOptimisticUpdate) WithVersion(v clparams.StateVersion) *LightClientOptimisticUpdate {
	l.version = v
	return l
}

func (l *LightClientOptimisticUpdate) EncodeSSZ(buf []byte) ([]byte, error) {
	dst := buf
	var err error
	if l.version >= clparams.CapellaVersion {
		dst = append(dst, ssz_utils.OffsetSSZ(uint32(8+l.SyncAggregate.EncodingSizeSSZ()+4))...)
	} else {
		if dst, err = l.AttestedHeader.EncodeSSZ(dst); err != nil {
			return nil, err
		}
	}
	dst = encodeUpdateFooter(dst, l.SyncAggregate, l.SignatureSlot)
	if l.version >= clparams.CapellaVersion {
		if dst, err = l.AttestedHeader.EncodeSSZ(dst); err != nil {
			return nil, err
		}
	}
	return dst, nil
}

func (l *LightClientOptimisticUpdate) DecodeSSZWithVersion(buf []byte, version int) error {
	var err error
	l.version = clparams.StateVersion(version)
	if len(buf) < l.EncodingSizeSSZ() {
		return ssz_utils.ErrBadOffset
	}

	pos := 0
	l.AttestedHeader = new(LightClientHeader)

	if l.version < clparams.CapellaVersion {
		if err = l.AttestedHeader.DecodeSSZWithVersion(buf, int(l.version)); err != nil {
			return err
		}
		pos += l.AttestedHeader.EncodingSizeSSZ()
	} else {
		pos += 4
	}

	l.SyncAggregate, l.SignatureSlot, err = decodeUpdateFooter(buf[pos:])
	if err != nil {
		return err
	}
	pos += l.SyncAggregate.EncodingSizeSSZ() + 8
	if l.version >= clparams.CapellaVersion {
		if err = l.AttestedHeader.DecodeSSZWithVersion(buf[pos:], version); err != nil {
			return err
		}
	}
	return err

}

func (l *LightClientOptimisticUpdate) DecodeSSZ([]byte) error {
	panic("DJFCF")
}

func (l *LightClientOptimisticUpdate) EncodingSizeSSZ() (size int) {
	if l.version >= clparams.CapellaVersion {
		size += 4
	}
	if l.AttestedHeader == nil {
		l.AttestedHeader = &LightClientHeader{version: l.version}
	}
	if l.SyncAggregate == nil {
		l.SyncAggregate = new(SyncAggregate)
	}
	return size + l.AttestedHeader.EncodingSizeSSZ() + l.SyncAggregate.EncodingSizeSSZ() + 8
}
