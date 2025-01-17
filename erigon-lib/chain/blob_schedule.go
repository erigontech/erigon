// Copyright 2025 The Erigon Authors
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

package chain

// See EIP-7840: Add blob schedule to EL config files

type BlobConfig struct {
	Target                uint64 `json:"target"`
	Max                   uint64 `json:"max"`
	BaseFeeUpdateFraction uint64 `json:"baseFeeUpdateFraction"`
}

type BlobSchedule struct {
	Cancun *BlobConfig `json:"cancun,omitempty"`
	Prague *BlobConfig `json:"prague,omitempty"`
}

func (b *BlobSchedule) TargetBlobsPerBlock(isPrague bool) uint64 {
	eip4844default := uint64(3)
	if b == nil {
		return eip4844default
	}
	if isPrague && b.Prague != nil {
		return b.Prague.Target
	}
	if b.Cancun != nil {
		return b.Cancun.Target
	}
	return eip4844default
}

func (b *BlobSchedule) MaxBlobsPerBlock(isPrague bool) uint64 {
	eip4844default := uint64(6)
	if b == nil {
		return eip4844default
	}
	if isPrague && b.Prague != nil {
		return b.Prague.Max
	}
	if b.Cancun != nil {
		return b.Cancun.Max
	}
	return eip4844default
}

func (b *BlobSchedule) BaseFeeUpdateFraction(isPrague bool) uint64 {
	eip4844default := uint64(3338477)
	if b == nil {
		return eip4844default
	}
	if isPrague && b.Prague != nil {
		return b.Prague.BaseFeeUpdateFraction
	}
	if b.Cancun != nil {
		return b.Cancun.BaseFeeUpdateFraction
	}
	return eip4844default
}
