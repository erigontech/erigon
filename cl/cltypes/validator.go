// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package cltypes

import (
	"encoding/json"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon-lib/types/ssz"

	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
)

const (
	DepositProofLength = 33
	SyncCommitteeSize  = 512
)

type DepositData struct {
	PubKey                common.Bytes48 `json:"pubkey"`
	WithdrawalCredentials common.Hash    `json:"withdrawal_credentials"`
	Amount                uint64         `json:"amount,string"`
	Signature             common.Bytes96 `json:"signature"`
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
	Proof solid.HashVectorSSZ `json:"proof"` // 33 X 32 size.
	Data  *DepositData        `json:"data"`
}

func (d *Deposit) UnmarshalJSON(buf []byte) error {
	d.Proof = solid.NewHashVector(33)
	d.Data = new(DepositData)

	return json.Unmarshal(buf, &struct {
		Proof solid.HashVectorSSZ `json:"proof"`
		Data  *DepositData        `json:"data"`
	}{d.Proof, d.Data})
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
	Epoch          uint64 `json:"epoch,string"`
	ValidatorIndex uint64 `json:"validator_index,string"`
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
	VoluntaryExit *VoluntaryExit `json:"message"`
	Signature     common.Bytes96 `json:"signature"`
}

func (e *SignedVoluntaryExit) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, e.VoluntaryExit, e.Signature[:])
}

func (e *SignedVoluntaryExit) DecodeSSZ(buf []byte, version int) error {
	e.VoluntaryExit = new(VoluntaryExit)
	return ssz2.UnmarshalSSZ(buf, version, e.VoluntaryExit, e.Signature[:])
}

func (e *SignedVoluntaryExit) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(e.VoluntaryExit, e.Signature[:])
}

func (e *SignedVoluntaryExit) EncodingSizeSSZ() int {
	return 96 + e.VoluntaryExit.EncodingSizeSSZ()
}
