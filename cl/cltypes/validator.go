package cltypes

import (
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
	Root                  common.Hash // Ignored if not for hashing
}

func (d *DepositData) MarshalSSZ() ([]byte, error) {
	buf := make([]byte, d.SizeSSZ())
	copy(buf, d.PubKey[:])
	copy(buf[48:], d.WithdrawalCredentials[:])
	ssz_utils.MarshalUint64SSZ(buf[80:], d.Amount)
	copy(buf[88:], d.Signature[:])
	return buf, nil
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

func (d *Deposit) MarshalSSZ() ([]byte, error) {
	buf := make([]byte, d.SizeSSZ())
	for i, proofSeg := range d.Proof {
		copy(buf[i*32:], proofSeg)
	}

	dataEnc, err := d.Data.MarshalSSZ()
	if err != nil {
		return nil, err
	}
	copy(buf[33*32:], dataEnc)
	return buf, nil
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

func (d *Deposit) SizeSSZ() int {
	return 1240
}

func (d *Deposit) HashTreeRoot() ([32]byte, error) {
	proofLeaves := make([][32]byte, DepositProofLength)
	for i, segProof := range d.Proof {
		proofLeaves[i] = common.BytesToHash(segProof)
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

func (e *VoluntaryExit) MarshalSSZ() ([]byte, error) {
	buf := make([]byte, e.SizeSSZ())
	ssz_utils.MarshalUint64SSZ(buf, e.Epoch)
	ssz_utils.MarshalUint64SSZ(buf[8:], e.ValidatorIndex)
	return buf, nil
}

func (e *VoluntaryExit) UnmarshalSSZ(buf []byte) error {
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

func (e *SignedVoluntaryExit) MarshalSSZ() ([]byte, error) {
	marshalledExit, err := e.VolunaryExit.MarshalSSZ()
	if err != nil {
		return nil, err
	}
	return append(marshalledExit, e.Signature[:]...), nil
}

func (e *SignedVoluntaryExit) UnmarshalSSZ(buf []byte) error {
	if e.VolunaryExit == nil {
		e.VolunaryExit = new(VoluntaryExit)
	}

	if err := e.VolunaryExit.UnmarshalSSZ(buf); err != nil {
		return err
	}
	copy(e.Signature[:], buf[16:])
	return nil
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

func (e *SignedVoluntaryExit) SizeSSZ() int {
	return 96 + e.VolunaryExit.SizeSSZ()
}
