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

package solid

import (
	"bytes"
	"encoding/binary"
	"encoding/json"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon-lib/types/ssz"
	"github.com/erigontech/erigon/cl/merkle_tree"
)

// slot: 8 bytes
// committeeIndex: 8 bytes
// beaconBlockHash: 32 bytes
// source: 40 bytes
// target: 40 bytes
const AttestationDataBufferSize = 8 + 8 + 32 + 40*2

// AttestationData contains information about attestantion, including finalized/attested checkpoints.
type AttestationData []byte

func NewAttestionDataFromParameters(
	slot uint64,
	committeeIndex uint64,
	beaconBlockRoot libcommon.Hash,
	source Checkpoint,
	target Checkpoint,
) AttestationData {
	a := NewAttestationData()
	a.SetSlot(slot)
	a.SetCommitteeIndex(committeeIndex)
	a.SetBeaconBlockRoot(beaconBlockRoot)
	a.SetSource(source)
	a.SetTarget(target)
	return a
}

func (a AttestationData) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Slot            uint64         `json:"slot,string"`
		Index           uint64         `json:"index,string"`
		BeaconBlockRoot libcommon.Hash `json:"beacon_block_root"`
		Source          Checkpoint     `json:"source"`
		Target          Checkpoint     `json:"target"`
	}{
		Slot:            a.Slot(),
		BeaconBlockRoot: a.BeaconBlockRoot(),
		Index:           a.CommitteeIndex(),
		Source:          a.Source(),
		Target:          a.Target(),
	})
}

func (a AttestationData) UnmarshalJSON(buf []byte) error {
	var tmp struct {
		Slot            uint64         `json:"slot,string"`
		Index           uint64         `json:"index,string"`
		BeaconBlockRoot libcommon.Hash `json:"beacon_block_root"`
		Source          Checkpoint     `json:"source"`
		Target          Checkpoint     `json:"target"`
	}
	tmp.Source = NewCheckpoint()
	tmp.Target = NewCheckpoint()
	if err := json.Unmarshal(buf, &tmp); err != nil {
		return err
	}
	a.SetSlot(tmp.Slot)
	a.SetCommitteeIndex(tmp.Index)
	a.SetBeaconBlockRoot(tmp.BeaconBlockRoot)
	a.SetSource(tmp.Source)
	a.SetTarget(tmp.Target)
	return nil
}

func NewAttestationData() AttestationData {
	return make([]byte, AttestationDataBufferSize)
}

func (a AttestationData) Static() bool {
	return true
}

func (a AttestationData) Slot() uint64 {
	return binary.LittleEndian.Uint64(a[:8])
}

func (a AttestationData) CommitteeIndex() uint64 {
	return binary.LittleEndian.Uint64(a[8:16])
}

func (a AttestationData) BeaconBlockRoot() (o libcommon.Hash) {
	copy(o[:], a[16:])
	return
}

func (a AttestationData) Source() Checkpoint {
	return Checkpoint(a[48:88])
}

func (a AttestationData) Target() Checkpoint {
	return Checkpoint(a[88:128])
}

func (a AttestationData) SetSlot(slot uint64) {
	binary.LittleEndian.PutUint64(a[:8], slot)
}

func (a AttestationData) SetCommitteeIndex(committeeIndex uint64) {
	binary.LittleEndian.PutUint64(a[8:16], committeeIndex)
}

func (a AttestationData) SetBeaconBlockRoot(beaconBlockRoot libcommon.Hash) {
	copy(a[16:], beaconBlockRoot[:])
}

func (a AttestationData) SetSlotWithRawBytes(b []byte) {
	copy(a[:8], b)
}

func (a AttestationData) SetCommitteeIndexWithRawBytes(b []byte) {
	copy(a[8:16], b)

}

func (a AttestationData) RawSlot() []byte {
	return a[:8]
}

func (a AttestationData) RawBeaconBlockRoot() []byte {
	return a[16:48]
}

func (a AttestationData) SetBeaconBlockRootWithRawBytes(b []byte) {
	copy(a[16:48], b)
}

func (a AttestationData) SetSource(c Checkpoint) {
	copy(a[48:88], c)
}

func (a AttestationData) SetTarget(c Checkpoint) {
	copy(a[88:128], c)
}

func (a AttestationData) EncodingSizeSSZ() int {
	return 128
}

func (a AttestationData) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) < a.EncodingSizeSSZ() {
		return ssz.ErrLowBufferSize
	}
	copy(a, buf)
	return nil
}

func (a AttestationData) EncodeSSZ(dst []byte) ([]byte, error) {
	buf := dst
	buf = append(buf, a...)
	return buf, nil
}

func (a AttestationData) Clone() clonable.Clonable {
	return NewAttestationData()
}

func (a AttestationData) CopyHashBufferTo(o []byte) error {
	copy(o[:32], a[:8])
	copy(o[32:64], a[8:16])
	copy(o[64:96], a[16:48])
	sourceRoot, err := a.Source().HashSSZ()
	if err != nil {
		return err
	}
	targetRoot, err := a.Target().HashSSZ()
	if err != nil {
		return err
	}
	copy(o[96:128], sourceRoot[:])
	copy(o[128:160], targetRoot[:])
	return nil
}

func (a AttestationData) HashSSZ() (o [32]byte, err error) {
	leaves := make([]byte, 8*length.Hash)
	if err = a.CopyHashBufferTo(leaves); err != nil {
		return
	}
	err = merkle_tree.MerkleRootFromFlatLeaves(leaves, o[:])
	return
}

func (a AttestationData) Equal(other AttestationData) bool {
	return bytes.Equal(a[:], other[:])
}
