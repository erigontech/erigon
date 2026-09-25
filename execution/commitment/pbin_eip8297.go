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
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/commitment/eip8297"

	"github.com/erigontech/erigon/common"
)

const (
	pbinMaxPathBits = eip8297.MaxPathBits
	pbinPathWords   = eip8297.PathWords

	pbinBasicDataLeafKey    = eip8297.BasicDataLeafKey
	pbinCodeHashLeafKey     = eip8297.CodeHashLeafKey
	pbinDelegationLeafKey   = eip8297.DelegationLeafKey
	pbinHeaderStorageOffset = eip8297.HeaderStorageOffset
	pbinHeaderStorageSlots  = eip8297.HeaderStorageSlots
	pbinStemSubtreeWidth    = eip8297.StemSubtreeWidth

	pbinAccountZone = eip8297.AccountZone
	pbinCodeZone    = eip8297.CodeZone
	pbinStorageZone = eip8297.StorageZone

	pbinAccountKeyLength = eip8297.AccountKeyLength
	pbinCodeKeyLength    = eip8297.CodeKeyLength
	pbinStorageKeyLength = eip8297.StorageKeyLength

	pbinValueLength = eip8297.ValueLength

	pbinBasicDataCodeSizeOffset = eip8297.BasicDataCodeSizeOffset
	pbinBasicDataNonceOffset    = eip8297.BasicDataNonceOffset
	pbinBasicDataBalanceOffset  = eip8297.BasicDataBalanceOffset
	pbinDelegationCodeLength    = eip8297.DelegationCodeLength

	pbinChunkDataLen = eip8297.ChunkDataLen
	pbinPushOffset   = eip8297.PushOffset
	pbinPush1        = eip8297.Push1
	pbinPush32       = eip8297.Push32

	pbinLeafTag   = eip8297.LeafTag
	pbinBranchTag = eip8297.BranchTag
)

type pbinBitpath = eip8297.Bitpath
type pbinHashFn = eip8297.HashFn
type pbinDigestCache = eip8297.DigestCache
type pbinChunkScratch = eip8297.ChunkScratch

var (
	errPBinNonCanonicalPad  = eip8297.ErrNonCanonicalPad
	errPBinBalanceOverflow  = eip8297.ErrBalanceOverflow
	errPBinCodeSizeOverflow = eip8297.ErrCodeSizeOverflow
	errPBinLeafValue        = eip8297.ErrLeafValue
	pbinEmptyTreeHash       = eip8297.EmptyTreeHash
	pbinSelectedSum         pbinHashFn
)

func pbinPathFromBytes(b []byte) pbinBitpath              { return eip8297.PathFromBytes(b) }
func pbinPathFromBits(b []byte, bitLen int16) pbinBitpath { return eip8297.PathFromBits(b, bitLen) }
func pbinCommonPrefixBitsAt(key *pbinBitpath, from int16, prefix *pbinBitpath) int16 {
	return eip8297.CommonPrefixBitsAt(key, from, prefix)
}
func pbinAppendBitPath(dst []byte, p *pbinBitpath) []byte { return eip8297.AppendBitPath(dst, p) }
func pbinEncodeBitPath(p *pbinBitpath) []byte             { return eip8297.EncodeBitPath(p) }
func pbinDecodeBitPath(buf []byte) (pbinBitpath, error)   { return eip8297.DecodeBitPath(buf) }

func pbinZoneKeyLength(zone byte) (int, bool) { return eip8297.ZoneKeyLength(zone) }
func pbinLeafSuffixBits(zone byte, depth int) (int, error) {
	return eip8297.LeafSuffixBits(zone, depth)
}
func pbinRightAlign32(b []byte) [32]byte { return eip8297.RightAlign32(b) }
func pbinTreeKey(zone byte, position []byte, subIndex byte) []byte {
	return eip8297.TreeKey(zone, position, subIndex)
}
func pbinTreeKeyAccount(addr []byte, subIndex byte) []byte {
	return eip8297.TreeKeyAccount(addr, subIndex)
}
func pbinTreeKeyStorage(addr, slot []byte) []byte {
	return eip8297.TreeKeyStorage(addr, slot)
}
func pbinTreeKeyCodeChunk(codeHash common.Hash, chunkID int) []byte {
	return eip8297.TreeKeyCodeChunk(codeHash, chunkID)
}
func pbinSlotInHeader(slot *[32]byte) bool { return eip8297.SlotInHeader(slot) }

func pbinKeyHasher() keyHasher                   { return keyHasher(eip8297.KeyHasher()) }
func pbinKeyHasherWith(sum pbinHashFn) keyHasher { return keyHasher(eip8297.KeyHasherWith(sum)) }

func pbinEncodeBasicData(nonce uint64, balance *uint256.Int, codeSize uint64) ([pbinValueLength]byte, error) {
	return eip8297.EncodeBasicData(nonce, balance, codeSize)
}
func pbinCodeHashValue(codeHash common.Hash) [pbinValueLength]byte {
	return eip8297.CodeHashValue(codeHash)
}
func pbinIsEmptyCodeHash(codeHash common.Hash) bool          { return eip8297.IsEmptyCodeHash(codeHash) }
func pbinIsDelegation(code []byte) bool                      { return eip8297.IsDelegation(code) }
func pbinEncodeDelegation(code []byte) [pbinValueLength]byte { return eip8297.EncodeDelegation(code) }
func pbinEncodeStorageValue(value []byte) [pbinValueLength]byte {
	return eip8297.EncodeStorageValue(value)
}
func pbinEncodeLeafValue(treeKey []byte, val *[pbinValueLength]byte) ([]byte, error) {
	return eip8297.EncodeLeafValue(treeKey, val)
}
func pbinDecodeLeafValue(treeKey []byte, enc []byte) ([pbinValueLength]byte, error) {
	return eip8297.DecodeLeafValue(treeKey, enc)
}

func pbinChunkifyCode(code []byte) [][pbinValueLength]byte { return eip8297.ChunkifyCode(code) }

func pbinRecordLeafValue(u *Update) ([pbinValueLength]byte, error) {
	if u.StorageLen != pbinValueLength {
		return [pbinValueLength]byte{}, fmt.Errorf("%w: record-resident leaf holds %d value bytes, want %d",
			errPBinCellHash, u.StorageLen, pbinValueLength)
	}
	return u.Storage, nil
}

func SetPBinHashSuite(name string) error {
	if err := eip8297.SetHashSuite(name); err != nil {
		return err
	}
	pbinSelectedSum = eip8297.SelectedHash()
	return nil
}

func PBinHashSuiteName() string { return eip8297.HashSuiteName() }

const (
	PBinHashKeccak = eip8297.HashKeccak
	PBinHashBlake3 = eip8297.HashBlake3
)
