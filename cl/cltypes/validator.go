package cltypes

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cl/cltypes/ssz_utils"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/common"
)

const DepositProofLength = 33

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

func (d *DepositData) UnmarshalSSZ(buf []byte) error {
	copy(d.PubKey[:], buf)
	copy(d.WithdrawalCredentials[:], buf[48:])
	d.Amount = ssz_utils.UnmarshalUint64SSZ(buf[80:])
	copy(d.Signature[:], buf[88:])
	return nil
}

func (d *DepositData) SizeSSZ() int {
	return 184
}

func (d *DepositData) HashTreeRoot() ([32]byte, error) {
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

type Deposit struct {
	// Merkle proof is used for deposits
	Proof [][]byte
	Data  *DepositData
}

func (d *Deposit) EncodeSSZ(dst []byte) []byte {

	buf := dst
	for _, proofSeg := range d.Proof {
		buf = append(buf, proofSeg...)
	}
	buf = d.Data.EncodeSSZ(buf)
	return buf
}

func (d *Deposit) UnmarshalSSZ(buf []byte) error {
	d.Proof = make([][]byte, DepositProofLength)
	for i := range d.Proof {
		d.Proof[i] = common.CopyBytes(buf[i*32 : i*32+32])
	}

	if d.Data == nil {
		d.Data = new(DepositData)
	}
	return d.Data.UnmarshalSSZ(buf[33*32:])
}

func (d *Deposit) UnmarshalSSZWithVersion(buf []byte, _ int) error {
	return d.UnmarshalSSZ(buf)
}

func (d *Deposit) EncodingSizeSSZ() int {
	return 1240
}

func (d *Deposit) HashTreeRoot() ([32]byte, error) {
	proofLeaves := make([][32]byte, DepositProofLength)
	for i, segProof := range d.Proof {
		proofLeaves[i] = libcommon.BytesToHash(segProof)
	}

	proofRoot, err := merkle_tree.ArraysRoot(proofLeaves, 64)
	if err != nil {
		return [32]byte{}, err
	}

	depositRoot, err := d.Data.HashTreeRoot()
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
	return append(buf, append(ssz_utils.Uint64SSZ(e.Epoch)[:], ssz_utils.Uint64SSZ(e.ValidatorIndex)[:]...)...)
}

func (e *VoluntaryExit) DecodeSSZ(buf []byte) error {
	e.Epoch = ssz_utils.UnmarshalUint64SSZ(buf)
	e.ValidatorIndex = ssz_utils.UnmarshalUint64SSZ(buf[8:])
	return nil
}

func (e *VoluntaryExit) HashTreeRoot() ([32]byte, error) {
	epochRoot := merkle_tree.Uint64Root(e.Epoch)
	indexRoot := merkle_tree.Uint64Root(e.ValidatorIndex)
	return utils.Keccak256(epochRoot[:], indexRoot[:]), nil
}

func (e *VoluntaryExit) SizeSSZ() int {
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

func (e *SignedVoluntaryExit) UnmarshalSSZ(buf []byte) error {
	if e.VolunaryExit == nil {
		e.VolunaryExit = new(VoluntaryExit)
	}

	if err := e.VolunaryExit.DecodeSSZ(buf); err != nil {
		return err
	}
	copy(e.Signature[:], buf[16:])
	return nil
}

func (e *SignedVoluntaryExit) UnmarshalSSZWithVersion(buf []byte, _ int) error {
	return e.UnmarshalSSZ(buf)
}

func (e *SignedVoluntaryExit) HashTreeRoot() ([32]byte, error) {
	sigRoot, err := merkle_tree.SignatureRoot(e.Signature)
	if err != nil {
		return [32]byte{}, err
	}
	exitRoot, err := e.VolunaryExit.HashTreeRoot()
	if err != nil {
		return [32]byte{}, err
	}
	return utils.Keccak256(exitRoot[:], sigRoot[:]), nil
}

func (e *SignedVoluntaryExit) EncodingSizeSSZ() int {
	return 96 + e.VolunaryExit.SizeSSZ()
}
