package cltypes

import (
	gokzg4844 "github.com/crate-crypto/go-kzg-4844"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/crypto/kzg"
	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/log/v3"
)

const CommitmentBranchSize = 17

type BlobSidecar struct {
	Index                    uint64                   `json:"index,string"`
	Blob                     Blob                     `json:"blob"` // define byte vector of 4096 * 32 bytes
	KzgCommitment            libcommon.Bytes48        `json:"kzg_commitment"`
	KzgProof                 libcommon.Bytes48        `json:"kzg_proof"`
	SignedBlockHeader        *SignedBeaconBlockHeader `json:"signed_block_header"`
	CommitmentInclusionProof solid.HashVectorSSZ      `json:"proof"`
}

func NewBlobSidecar(index uint64, blob *Blob, kzgCommitment libcommon.Bytes48, kzgProof libcommon.Bytes48, signedBlockHeader *SignedBeaconBlockHeader, commitmentInclusionProof solid.HashVectorSSZ) *BlobSidecar {
	return &BlobSidecar{
		Index:                    index,
		Blob:                     *blob,
		KzgCommitment:            kzgCommitment,
		KzgProof:                 kzgProof,
		SignedBlockHeader:        new(SignedBeaconBlockHeader),
		CommitmentInclusionProof: solid.NewHashVector(CommitmentBranchSize),
	}
}

func (b *BlobSidecar) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, b.getSchema()...)
}

func (b *BlobSidecar) EncodingSizeSSZ() int {
	size := 8 + 4096*32 + 48 + 48
	if b.SignedBlockHeader != nil {
		size += b.SignedBlockHeader.EncodingSizeSSZ()
	}
	if b.CommitmentInclusionProof != nil {
		size += b.CommitmentInclusionProof.EncodingSizeSSZ()
	}
	return size
}

func (b *BlobSidecar) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, b.getSchema()...)
}

func (b *BlobSidecar) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(b.getSchema()...)
}

func (b *BlobSidecar) Clone() clonable.Clonable {
	blob := &Blob{}
	return NewBlobSidecar(b.Index, blob, b.KzgCommitment, b.KzgProof, b.SignedBlockHeader, b.CommitmentInclusionProof)
}

func (b *BlobSidecar) getSchema() []interface{} {
	s := []interface{}{&b.Index, b.Blob[:], b.KzgCommitment[:], b.KzgProof[:]}
	if b.SignedBlockHeader != nil {
		s = append(s, b.SignedBlockHeader)
	}
	if b.CommitmentInclusionProof != nil {
		s = append(s, b.CommitmentInclusionProof)
	}
	return s
}

type BlobIdentifier struct {
	BlockRoot libcommon.Hash `json:"block_root"`
	Index     uint64         `json:"index,string"`
}

func NewBlobIdentifier(blockRoot libcommon.Hash, index uint64) *BlobIdentifier {
	return &BlobIdentifier{
		BlockRoot: blockRoot,
		Index:     index,
	}
}

func (b *BlobIdentifier) EncodeSSZ(buf []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, b.getSchema()...)
}

func (b *BlobIdentifier) EncodingSizeSSZ() int {
	return 32 + 8
}

func (b *BlobIdentifier) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, b.getSchema()...)
}

func (b *BlobIdentifier) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(b.getSchema()...)
}

func (b *BlobIdentifier) Clone() clonable.Clonable {
	return NewBlobIdentifier(b.BlockRoot, b.Index)
}

func (b *BlobIdentifier) getSchema() []interface{} {
	return []interface{}{
		b.BlockRoot[:],
		&b.Index,
	}
}

func VerifyBlobsSidecarAgainstExpectedBlobs(sidecars []*BlobSidecar, commitmentsToSlot map[libcommon.Bytes48]uint64) error {
	for i, sidecar := range sidecars {
		if commitmentsToSlot != nil {
			if _, ok := commitmentsToSlot[sidecar.KzgCommitment]; !ok {
				continue
			}
		}
		if err := kzg.Ctx().VerifyBlobKZGProof(gokzg4844.Blob(sidecar.Blob), gokzg4844.KZGCommitment(sidecar.KzgCommitment), gokzg4844.KZGProof(sidecar.KzgProof)); err != nil {
			log.Debug("BlobSidecar KZG proof verification failed", "index", i, "err", err)
			continue
		}
		if commitmentsToSlot != nil {
			delete(commitmentsToSlot, sidecar.KzgCommitment)
		}
	}

	return nil
}

func VerifyCommitmentInclusionProof(commitment libcommon.Bytes48, commitmentInclusionProof solid.HashVectorSSZ, commitmentIndex uint64, version clparams.StateVersion, bodyRoot [32]byte) bool {
	// Initialize the merkle tree leaf
	value, err := merkle_tree.HashTreeRoot(commitment[:])
	if err != nil {
		return false
	}
	bodyDepth := 4
	commitmentsDepth := uint64(13) // log2(4096) + 1 = 13
	bIndex := uint64(11)

	// Start by constructing the commitments subtree
	for i := uint64(0); i < commitmentsDepth; i++ {
		curr := commitmentInclusionProof.Get(int(i))
		if (commitmentIndex / utils.PowerOf2(i) % 2) == 1 {
			value = utils.Sha256(append(curr[:], value[:]...))
		} else {
			value = utils.Sha256(append(value[:], curr[:]...))
		}
	}
	// Construct up the block giga tree
	for i := uint64(0); i < uint64(bodyDepth); i++ {
		curr := commitmentInclusionProof.Get(int(i + commitmentsDepth))
		if (bIndex / utils.PowerOf2(i) % 2) == 1 {
			value = utils.Sha256(append(curr[:], value[:]...))
		} else {
			value = utils.Sha256(append(value[:], curr[:]...))
		}
	}
	return value == bodyRoot
}
