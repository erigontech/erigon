// Copyright 2026 The Erigon Authors
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

package era

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/golang/snappy"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
)

// Byte offset of the little-endian uint64 `slot` field within an SSZ-encoded
// SignedBeaconBlock and BeaconState. Both leading sections are fixed-size, so
// the slot can be peeked before a full decode (the slot is what selects the
// fork version needed to decode the rest).
//
//	SignedBeaconBlock = {Block (offset[4]), Signature [96]} | Block...
//	                    Block = {Slot uint64, ...}  -> slot at 4+96 = 100
//	BeaconState       = {genesis_time uint64, genesis_validators_root [32],
//	                     slot uint64, ...}          -> slot at 8+32 = 40
const (
	blockSlotPeekOffset = 100
	stateSlotPeekOffset = 40
)

// Era is one fully-decoded era group: the beacon blocks for the era's slot
// range (non-empty slots only, in slot order) and the BeaconState at the era
// boundary.
type Era struct {
	Number uint64 // era number (the era ends at slot Number*SlotsPerHistoricalRoot)
	Blocks []*cltypes.SignedBeaconBlock
	State  *state.CachingBeaconState

	cfg        *clparams.BeaconChainConfig
	blockIndex *SlotIndex
	stateIndex *SlotIndex
}

// ReadEra decodes a single-group .era file: e2store framing, snappy
// decompression, and SSZ decoding of every block and the state. The fork
// version for each SSZ decode is derived from the slot peeked out of the
// payload.
func ReadEra(r io.Reader, cfg *clparams.BeaconChainConfig) (*Era, error) {
	e := &Era{cfg: cfg}

	var sawVersion bool
	var slotIndexes []SlotIndex

	for {
		rec, err := readE2storeRecord(r)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		switch rec.typ {
		case TypeVersion:
			if len(rec.data) != 0 {
				return nil, fmt.Errorf("era: Version record must be empty, got %d bytes", len(rec.data))
			}
			if sawVersion {
				return nil, errors.New("era: ReadEra supports single-group files only")
			}
			sawVersion = true

		case TypeCompressedBlock:
			if !sawVersion {
				return nil, errNoVersion
			}
			blk, err := decodeBlock(rec.data, cfg)
			if err != nil {
				return nil, err
			}
			e.Blocks = append(e.Blocks, blk)

		case TypeCompressedState:
			if !sawVersion {
				return nil, errNoVersion
			}
			if e.State != nil {
				return nil, errors.New("era: multiple state records in one group")
			}
			st, err := decodeState(rec.data, cfg)
			if err != nil {
				return nil, err
			}
			e.State = st

		case TypeSlotIndex:
			si, err := parseSlotIndex(rec.data)
			if err != nil {
				return nil, err
			}
			slotIndexes = append(slotIndexes, si)

		default:
			// Spec requires unknown record types to be skipped.
		}
	}

	if e.State == nil {
		return nil, errors.New("era: no state record")
	}
	switch len(slotIndexes) {
	case 1:
		e.stateIndex = &slotIndexes[0]
	case 2:
		e.blockIndex = &slotIndexes[0]
		e.stateIndex = &slotIndexes[1]
	default:
		return nil, fmt.Errorf("era: %d SlotIndex records, want 1 or 2", len(slotIndexes))
	}

	e.Number = e.stateIndex.StartSlot / SlotsPerHistoricalRoot
	return e, nil
}

// decodeBlock snappy-decompresses and SSZ-decodes one CompressedSignedBeaconBlock.
func decodeBlock(compressed []byte, cfg *clparams.BeaconChainConfig) (*cltypes.SignedBeaconBlock, error) {
	raw, err := snappyDecode(compressed)
	if err != nil {
		return nil, fmt.Errorf("era: block snappy decode: %w", err)
	}
	if len(raw) < blockSlotPeekOffset+8 {
		return nil, fmt.Errorf("era: block SSZ too short: %d bytes", len(raw))
	}
	slot := binary.LittleEndian.Uint64(raw[blockSlotPeekOffset : blockSlotPeekOffset+8])
	version := cfg.GetCurrentStateVersion(slot / cfg.SlotsPerEpoch)

	blk := cltypes.NewSignedBeaconBlock(cfg, version)
	if err := blk.DecodeSSZ(raw, int(version)); err != nil {
		return nil, fmt.Errorf("era: block SSZ decode (slot %d, v%d): %w", slot, version, err)
	}
	return blk, nil
}

// decodeState snappy-decompresses and SSZ-decodes the CompressedBeaconState.
func decodeState(compressed []byte, cfg *clparams.BeaconChainConfig) (*state.CachingBeaconState, error) {
	raw, err := snappyDecode(compressed)
	if err != nil {
		return nil, fmt.Errorf("era: state snappy decode: %w", err)
	}
	if len(raw) < stateSlotPeekOffset+8 {
		return nil, fmt.Errorf("era: state SSZ too short: %d bytes", len(raw))
	}
	slot := binary.LittleEndian.Uint64(raw[stateSlotPeekOffset : stateSlotPeekOffset+8])
	version := cfg.GetCurrentStateVersion(slot / cfg.SlotsPerEpoch)

	st := state.New(cfg)
	if err := st.DecodeSSZ(raw, int(version)); err != nil {
		return nil, fmt.Errorf("era: state SSZ decode (slot %d, v%d): %w", slot, version, err)
	}
	return st, nil
}

// snappyDecode decodes snappy framed-format data.
func snappyDecode(compressed []byte) ([]byte, error) {
	return io.ReadAll(snappy.NewReader(bytes.NewReader(compressed)))
}
