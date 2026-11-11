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

// Package seqsig verifies the heads the sequencer signs for every change of
// its canonical L2 head, carries them on the overlay wire, keeps the latest
// head of each signing source, and chooses the unsafe head among them.
package seqsig

import (
	"cmp"
	"encoding/binary"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
)

var (
	// DomainTypeHash is the EIP-712 type hash of the signing domain.
	DomainTypeHash = crypto.Keccak256Hash([]byte("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"))
	// HeadTypeHash is the EIP-712 type hash of SignedHead.
	HeadTypeHash = crypto.Keccak256Hash([]byte("SignedHead(uint64 rollupId,uint64 number,bytes32 blockHash,bytes32 parentHash,uint64 epochMs,uint64 counter)"))

	domainNameHash    = crypto.Keccak256Hash([]byte("EEZ Sequencer"))
	domainVersionHash = crypto.Keccak256Hash([]byte("0"))
)

// Domain binds a signature to one L2 chain, registry and rollup.
type Domain struct {
	ChainID  uint64
	Registry common.Address
	// RollupID is not part of the EIP-712 domain; a head names its rollup.
	RollupID uint64
}

// Separator returns the EIP-712 domain separator: name "EEZ Sequencer",
// version "0", the L2 chain id and the registry as verifying contract.
func (d Domain) Separator() common.Hash {
	var buf [5 * 32]byte
	copy(buf[0:], DomainTypeHash[:])
	copy(buf[32:], domainNameHash[:])
	copy(buf[64:], domainVersionHash[:])
	binary.BigEndian.PutUint64(buf[3*32+24:], d.ChainID)
	copy(buf[4*32+12:], d.Registry[:])
	return crypto.Keccak256Hash(buf[:])
}

// Head is the EIP-712 SignedHead struct.
type Head struct {
	RollupID   uint64
	Number     uint64
	BlockHash  common.Hash
	ParentHash common.Hash
	// EpochMs is the signing process's start time in milliseconds.
	EpochMs uint64
	// Counter starts at 1 and grows by one per head the process signs.
	Counter uint64
}

// Version orders heads: a later epoch, a restarted or newer signing process,
// outranks every counter of an earlier one.
type Version struct {
	EpochMs uint64
	Counter uint64
}

// Compare returns -1, 0 or +1 as v is lower than, equal to or higher than o.
func (v Version) Compare(o Version) int {
	if c := cmp.Compare(v.EpochMs, o.EpochMs); c != 0 {
		return c
	}
	return cmp.Compare(v.Counter, o.Counter)
}

// Version returns the head's (epochMs, counter).
func (h Head) Version() Version { return Version{EpochMs: h.EpochMs, Counter: h.Counter} }

// StructHash returns the EIP-712 hashStruct of h.
func (h Head) StructHash() common.Hash {
	var buf [7 * 32]byte
	copy(buf[0:], HeadTypeHash[:])
	binary.BigEndian.PutUint64(buf[1*32+24:], h.RollupID)
	binary.BigEndian.PutUint64(buf[2*32+24:], h.Number)
	copy(buf[3*32:], h.BlockHash[:])
	copy(buf[4*32:], h.ParentHash[:])
	binary.BigEndian.PutUint64(buf[5*32+24:], h.EpochMs)
	binary.BigEndian.PutUint64(buf[6*32+24:], h.Counter)
	return crypto.Keccak256Hash(buf[:])
}

// SigningHash returns the digest the sequencer signs,
// keccak256(0x1901 ‖ domainSeparator ‖ hashStruct(h)).
func (h Head) SigningHash(d Domain) common.Hash {
	sep, sh := d.Separator(), h.StructHash()
	var buf [2 + 2*32]byte
	buf[0], buf[1] = 0x19, 0x01
	copy(buf[2:], sep[:])
	copy(buf[34:], sh[:])
	return crypto.Keccak256Hash(buf[:])
}
