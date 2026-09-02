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

package commitment

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
)

func recordTestData(shape string, extension []byte) cellEncodeData {
	var d cellEncodeData
	d.extLen = int16(len(extension))
	copy(d.extension[:], extension)
	switch shape {
	case "branch":
		d.hashLen = length.Hash
		for i := range d.hash {
			d.hash[i] = byte(i + 1)
		}
	case "storage":
		d.storageAddrLen = length.Addr + length.Hash
		for i := range d.storageAddr {
			d.storageAddr[i] = byte(0xa0 + i)
		}
		d.stateHashLen = length.Hash
		for i := range d.stateHash {
			d.stateHash[i] = byte(0x40 + i)
		}
	case "account":
		d.accountAddrLen = length.Addr
		for i := range d.accountAddr {
			d.accountAddr[i] = byte(0x20 + i)
		}
		d.stateHashLen = length.Hash
		for i := range d.stateHash {
			d.stateHash[i] = byte(0x60 + i)
		}
	case "account-storage":
		d.accountAddrLen = length.Addr
		for i := range d.accountAddr {
			d.accountAddr[i] = byte(0x20 + i)
		}
		d.storageAddrLen = length.Addr + length.Hash
		d.hashLen = length.Hash
		for i := range d.hash {
			d.hash[i] = byte(0x80 + i)
		}
		d.stateHashLen = length.Hash
		for i := range d.stateHash {
			d.stateHash[i] = byte(0xc0 + i)
		}
		d.storageMask = 0x5a3c
	default:
		panic("unknown record test shape")
	}
	return d
}

func packedRecordExtension(extension []byte) []byte {
	packed := make([]byte, (len(extension)+1)/2)
	for i, nibble := range extension {
		if i&1 == 0 {
			packed[i/2] = nibble << 4
		} else {
			packed[i/2] |= nibble & 0x0f
		}
	}
	return packed
}

func TestEncodeBranchChild(t *testing.T) {
	d := recordTestData("branch", []byte{0xa, 0xb, 0xc})
	got := EncodeBranchChild(0x1234, &d)

	want := []byte{recordFlagHash | recordFlagExtensionOdd, 0x12, 0x34}
	want = append(want, d.hash[:]...)
	want = append(want, packedRecordExtension(d.extension[:d.extLen])...)
	require.Equal(t, want, got)
}

func TestEncodeLeafChildShapes(t *testing.T) {
	for _, shape := range []string{"storage", "account", "account-storage"} {
		t.Run(shape, func(t *testing.T) {
			extension := []byte(nil)
			if shape == "account" || shape == "account-storage" {
				extension = []byte{1, 2, 3, 4}
			}
			d := recordTestData(shape, extension)
			got := EncodeLeafChild(&d)
			flags := got[0]
			require.Equal(t, uint8(recordFlagLeaf), flags&recordFlagLeaf)
			require.Equal(t, uint8(0), flags&recordFlagExtensionOdd)
			require.Equal(t, d.stateHashLen == length.Hash, flags&recordFlagHash != 0)

			switch shape {
			case "storage":
				require.NotZero(t, flags&recordFlagStorageLeaf)
				require.Zero(t, flags&recordFlagHasStorage)
				require.Len(t, got, 1+length.Hash+length.Hash)
				require.Equal(t, d.stateHash[:], got[1:1+length.Hash])
				require.Equal(t, d.storageAddr[length.Addr:], got[1+length.Hash:1+2*length.Hash])
			case "account":
				require.Zero(t, flags&recordFlagStorageLeaf)
				require.Zero(t, flags&recordFlagHasStorage)
				require.Len(t, got, 1+length.Hash+length.Addr+2)
				require.Equal(t, d.stateHash[:], got[1:1+length.Hash])
				require.Equal(t, d.accountAddr[:], got[1+length.Hash:1+length.Hash+length.Addr])
			case "account-storage":
				require.Zero(t, flags&recordFlagStorageLeaf)
				require.NotZero(t, flags&recordFlagHasStorage)
				require.Len(t, got, 1+length.Hash+length.Hash+2+length.Addr+2)
				require.Equal(t, d.stateHash[:], got[1:1+length.Hash])
				require.Equal(t, d.hash[:], got[1+length.Hash:1+2*length.Hash])
				require.Equal(t, []byte{0x5a, 0x3c}, got[1+2*length.Hash:1+2*length.Hash+2])
			}
		})
	}
}

func TestEncodeLeafChildOmitsEmbeddedHash(t *testing.T) {
	d := recordTestData("account", nil)
	d.stateHashLen = 31
	rec := EncodeLeafChild(&d)

	require.Zero(t, rec[0]&recordFlagHash)
	require.Len(t, rec, 1+length.Addr)
	require.Equal(t, d.accountAddr[:], rec[1:])

	var got cell
	_, err := DecodeRecordInto(rec, &got)
	require.NoError(t, err)
	require.Zero(t, got.stateHashLen)
}

func TestSynthesizeBranchRowCarriesLeafChildMask(t *testing.T) {
	d := recordTestData("account-storage", nil)
	var records [16][]byte
	records[0] = EncodeLeafChild(&d)

	read, err := SynthesizeBranchRow(1, true, records, 1, nil)
	require.NoError(t, err)
	require.Equal(t, uint16(1), read.ChildMasksKnown)
	require.Equal(t, d.storageMask, read.ChildMasks[0])
}

func TestSynthesizeBranchRowDoesNotMarkSingletonStorageAsBranch(t *testing.T) {
	d := recordTestData("account-storage", nil)
	d.storageMask = 0
	var records [16][]byte
	records[0] = EncodeLeafChild(&d)

	read, err := SynthesizeBranchRow(1, true, records, 1, nil)
	require.NoError(t, err)
	require.Zero(t, read.ChildMasksKnown)
}

func TestDecodeRecordIntoRoundTrip(t *testing.T) {
	for _, shape := range []string{"branch", "storage", "account", "account-storage"} {
		extensions := [][]byte{nil}
		if shape != "storage" {
			extensions = [][]byte{nil, {1, 2}, {1, 2, 3}, {1, 2, 3, 4}}
		}
		for _, extension := range extensions {
			t.Run(shape+"/"+string(rune('0'+len(extension))), func(t *testing.T) {
				d := recordTestData(shape, extension)
				var rec []byte
				wantMask := uint16(0)
				if shape == "branch" {
					wantMask = 0x1234
					rec = EncodeBranchChild(wantMask, &d)
				} else {
					rec = EncodeLeafChild(&d)
					if shape == "account-storage" {
						wantMask = d.storageMask
					}
				}

				var got cell
				mask, err := DecodeRecordInto(rec, &got)
				require.NoError(t, err)
				require.Equal(t, wantMask, mask)
				require.Equal(t, int16(len(extension)), got.extLen)
				if len(extension) > 0 {
					require.Equal(t, extension, got.extension[:got.extLen])
					require.Equal(t, extension, got.hashedExtension[:got.hashedExtLen])
				}

				switch shape {
				case "branch":
					require.Equal(t, d.hash[:], got.hash[:])
					require.Equal(t, int16(length.Hash), got.hashLen)
				case "storage":
					require.Equal(t, d.stateHash[:], got.stateHash[:])
					require.Equal(t, int16(length.Hash), got.storageAddrLen)
					require.Equal(t, d.storageAddr[length.Addr:], got.storageAddr[:length.Hash])
				case "account":
					require.Equal(t, d.accountAddr[:], got.accountAddr[:])
					require.Equal(t, d.stateHash[:], got.stateHash[:])
				case "account-storage":
					require.Equal(t, d.accountAddr[:], got.accountAddr[:])
					require.Equal(t, d.stateHash[:], got.stateHash[:])
					require.Equal(t, d.hash[:], got.hash[:])
					require.Equal(t, int16(length.Hash), got.hashLen)
					require.Equal(t, d.storageMask, got.storageMask)
				}
			})
		}
	}
}

func TestRecordRowReconstruction(t *testing.T) {
	rng := rand.New(rand.NewSource(20260828))
	for range 100 {
		var source [16]cellEncodeData
		var bitmap uint16
		for nibble := range 16 {
			if rng.Intn(3) == 0 {
				continue
			}
			bitmap |= uint16(1) << nibble
			d := recordTestData([]string{"branch", "account", "storage"}[rng.Intn(3)], nil)
			d.extLen = int16(rng.Intn(5))
			for i := range d.extLen {
				d.extension[i] = byte(rng.Intn(16))
			}
			source[nibble] = d
		}

		legacy, err := NewBranchEncoder(4096).EncodeBranch(bitmap, bitmap, bitmap, &source)
		require.NoError(t, err)
		var legacyCells [16]cell
		_, err = DecodeBranchInto(legacy[2:], false, &legacyCells)
		require.NoError(t, err)

		for bitset := bitmap; bitset != 0; bitset &= bitset - 1 {
			nibble := bitsTrailingZeros16(bitset)
			shape := "branch"
			if source[nibble].accountAddrLen > 0 {
				shape = "account"
			} else if source[nibble].storageAddrLen > 0 {
				shape = "storage"
			}
			var rec []byte
			if shape == "branch" {
				rec = EncodeBranchChild(bitmap, &source[nibble])
			} else {
				rec = EncodeLeafChild(&source[nibble])
			}
			var decoded cell
			_, err = DecodeRecordInto(rec, &decoded)
			require.NoError(t, err)
			require.Equal(t, legacyCells[nibble].stateHash[:legacyCells[nibble].stateHashLen], decoded.stateHash[:decoded.stateHashLen])
			require.Equal(t, legacyCells[nibble].hash[:legacyCells[nibble].hashLen], decoded.hash[:decoded.hashLen])
			require.Equal(t, legacyCells[nibble].accountAddr[:legacyCells[nibble].accountAddrLen], decoded.accountAddr[:decoded.accountAddrLen])
			if shape == "storage" {
				require.Equal(t, legacyCells[nibble].storageAddr[length.Addr:], decoded.storageAddr[:length.Hash])
			}
			require.Equal(t, legacyCells[nibble].extension[:legacyCells[nibble].extLen], decoded.extension[:decoded.extLen])
		}
	}
}

func TestDecodeRecordIntoRejectsMalformedRecords(t *testing.T) {
	branchData := recordTestData("branch", []byte{1, 2, 3})
	oddBranch := EncodeBranchChild(1, &branchData)
	truncatedTail := oddBranch[:len(oddBranch)-1]
	storageData := recordTestData("account-storage", nil)
	storageWithoutRoom := EncodeLeafChild(&storageData)
	storageWithoutRoom = storageWithoutRoom[:len(storageWithoutRoom)-1]
	badParity := bytes.Clone(oddBranch)
	badParity[len(badParity)-1] |= 1

	for _, tc := range []struct {
		name string
		rec  []byte
	}{
		{name: "truncated tail", rec: truncatedTail},
		{name: "storage fields truncated", rec: storageWithoutRoom},
		{name: "extension parity disagrees with packed tail", rec: badParity},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var c cell
			_, err := DecodeRecordInto(tc.rec, &c)
			require.Error(t, err)
		})
	}

	var c cell
	_, err := DecodeRecordInto([]byte{recordFlagHash}, &c)
	require.Error(t, err)
}

func TestDecodeRecordIntoRejectsLengthMismatches(t *testing.T) {
	d := recordTestData("storage", nil)
	rec := EncodeLeafChild(&d)
	var c cell
	_, err := DecodeRecordInto(append(rec, 1, 2, 3), &c)
	require.Error(t, err)

	bad := append([]byte(nil), rec...)
	bad[0] |= recordFlagExtensionOdd
	_, err = DecodeRecordInto(bad, &c)
	require.Error(t, err)
}

func bitsTrailingZeros16(v uint16) int {
	for i := range 16 {
		if v&(1<<i) != 0 {
			return i
		}
	}
	return 16
}

// A record that outgrows its reserved capacity reallocates and copies, once per record.
func TestEncodeLeafChildReservesExactCapacity(t *testing.T) {
	t.Parallel()

	stateHash := func(d *cellEncodeData) {
		d.stateHashLen = length.Hash
		for i := range d.stateHash {
			d.stateHash[i] = byte(0x40 + i)
		}
	}
	storageAddr := func(d *cellEncodeData) {
		d.storageAddrLen = length.Addr + length.Hash
		for i := range d.storageAddr {
			d.storageAddr[i] = byte(0xa0 + i)
		}
	}
	accountAddr := func(d *cellEncodeData) {
		d.accountAddrLen = length.Addr
		for i := range d.accountAddr {
			d.accountAddr[i] = byte(0x20 + i)
		}
	}
	hash := func(d *cellEncodeData) {
		d.hashLen = length.Hash
		for i := range d.hash {
			d.hash[i] = byte(i + 1)
		}
	}

	for _, tc := range []struct {
		name  string
		build func(*cellEncodeData)
	}{
		{"storage-leaf", func(d *cellEncodeData) { storageAddr(d); stateHash(d) }},
		{"hoisted-slot", func(d *cellEncodeData) { accountAddr(d); storageAddr(d); stateHash(d) }},
		{"account-with-storage", func(d *cellEncodeData) { accountAddr(d); hash(d); stateHash(d) }},
		{"plain-account", func(d *cellEncodeData) { accountAddr(d); stateHash(d) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var d cellEncodeData
			tc.build(&d)
			rec := EncodeLeafChild(&d)
			require.Equal(t, len(rec), cap(rec), "record outgrew its reserved capacity")
		})
	}
}

// storage-address is only meaningful on a hoisted account leaf. Accepting it elsewhere lets a
// corrupted record decode as valid instead of failing closed.
func TestDecodeRecordRejectsStorageAddrFlagOnWrongShapes(t *testing.T) {
	t.Parallel()

	var c cell

	var b cellEncodeData
	b.hashLen = length.Hash
	for i := range b.hash {
		b.hash[i] = byte(i + 1)
	}
	branch := EncodeBranchChild(0x1234, &b)
	branch[0] |= recordFlagStorageAddr
	_, err := DecodeRecordInto(branch, &c)
	require.Error(t, err, "branch record must reject the storage-address flag")

	var s cellEncodeData
	s.storageAddrLen = length.Addr + length.Hash
	for i := range s.storageAddr {
		s.storageAddr[i] = byte(0xa0 + i)
	}
	leaf := EncodeLeafChild(&s)
	leaf[0] |= recordFlagStorageAddr
	_, err = DecodeRecordInto(leaf, &c)
	require.Error(t, err, "storage leaf must reject the storage-address flag")
}
