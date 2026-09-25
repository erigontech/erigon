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

package commitmenttest

import "bytes"

type Leaf struct {
	Suffix []byte
	Value  []byte
}

type RecordSpec struct {
	Key           []byte
	Flags         byte
	ChildMask     uint16
	LeafMask      uint16
	ExtensionMask uint16
	SelfExtension []byte
	Extensions    [16][]byte
	Hashes        [16][]byte
	Leaves        [16]Leaf
	Root          []byte
}

type RecordValue struct {
	Key  []byte
	Data []byte
}

func Records(specs []RecordSpec) []RecordValue {
	out := make([]RecordValue, 0, len(specs))
	for i := range specs {
		spec := &specs[i]
		data := []byte{spec.Flags}
		if spec.Flags&0x80 != 0 {
			if spec.Root == nil {
				data = append(data, make([]byte, 33)...)
			} else {
				data = append(data, spec.Root...)
			}
			out = append(out, RecordValue{Key: bytes.Clone(spec.Key), Data: data})
			continue
		}
		if spec.Flags&0x10 != 0 {
			data = append(data, spec.SelfExtension...)
		}
		data = append(data, byte(spec.ChildMask>>8), byte(spec.ChildMask), byte(spec.LeafMask>>8), byte(spec.LeafMask))
		if spec.Flags&0x20 != 0 {
			data = append(data, byte(spec.ExtensionMask>>8), byte(spec.ExtensionMask))
		}
		for nib := range 16 {
			if spec.ChildMask&^spec.LeafMask&(1<<nib) != 0 {
				hash := spec.Hashes[nib]
				if hash == nil {
					hash = bytes.Repeat([]byte{byte(nib)}, 32)
				}
				data = append(data, hash...)
			}
		}
		for nib := range 16 {
			if spec.ExtensionMask&(1<<nib) != 0 {
				data = append(data, spec.Extensions[nib]...)
			}
		}
		for nib := range 16 {
			if spec.LeafMask&(1<<nib) != 0 {
				leaf := spec.Leaves[nib]
				data = append(data, leaf.Suffix...)
				data = append(data, byte(len(leaf.Value)))
				data = append(data, leaf.Value...)
			}
		}
		out = append(out, RecordValue{Key: bytes.Clone(spec.Key), Data: data})
	}
	return out
}
