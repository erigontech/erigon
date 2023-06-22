// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package forkid

import (
	"bytes"
	"math"
	"testing"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
)

// TestCreation tests that different genesis and fork rule combinations result in
// the correct fork ID.
// Forks before Shanghai are triggered by the block number,
// while Shanghai and later forks are triggered by the block time.
func TestCreation(t *testing.T) {
	type testcase struct {
		head uint64
		time uint64
		want ID
	}
	tests := []struct {
		config  *chain.Config
		genesis libcommon.Hash
		cases   []testcase
	}{
		// Mainnet test cases
		{
			params.MainnetChainConfig,
			params.MainnetGenesisHash,
			[]testcase{
				{0, 0, ID{Hash: checksumToBytes(0xfc64ec04), Next: 1150000}},                    // Unsynced
				{1149999, 1457981342, ID{Hash: checksumToBytes(0xfc64ec04), Next: 1150000}},     // Last Frontier block
				{1150000, 1457981393, ID{Hash: checksumToBytes(0x97c2c34c), Next: 1920000}},     // First Homestead block
				{1919999, 1469020838, ID{Hash: checksumToBytes(0x97c2c34c), Next: 1920000}},     // Last Homestead block
				{1920000, 1469020840, ID{Hash: checksumToBytes(0x91d1f948), Next: 2463000}},     // First DAO block
				{2462999, 1476796747, ID{Hash: checksumToBytes(0x91d1f948), Next: 2463000}},     // Last DAO block
				{2463000, 1476796771, ID{Hash: checksumToBytes(0x7a64da13), Next: 2675000}},     // First Tangerine block
				{2674999, 1479831337, ID{Hash: checksumToBytes(0x7a64da13), Next: 2675000}},     // Last Tangerine block
				{2675000, 1479831344, ID{Hash: checksumToBytes(0x3edd5b10), Next: 4370000}},     // First Spurious block
				{4369999, 1508131303, ID{Hash: checksumToBytes(0x3edd5b10), Next: 4370000}},     // Last Spurious block
				{4370000, 1508131331, ID{Hash: checksumToBytes(0xa00bc324), Next: 7280000}},     // First Byzantium block
				{7279999, 1551383501, ID{Hash: checksumToBytes(0xa00bc324), Next: 7280000}},     // Last Byzantium block
				{7280000, 1551383524, ID{Hash: checksumToBytes(0x668db0af), Next: 9069000}},     // First and last Constantinople, first Petersburg block
				{9068999, 1575764708, ID{Hash: checksumToBytes(0x668db0af), Next: 9069000}},     // Last Petersburg block
				{9069000, 1575764709, ID{Hash: checksumToBytes(0x879d6e30), Next: 9200000}},     // First Istanbul block
				{9199999, 1577953806, ID{Hash: checksumToBytes(0x879d6e30), Next: 9200000}},     // Last Istanbul block
				{9200000, 1577953849, ID{Hash: checksumToBytes(0xe029e991), Next: 12244000}},    // First Muir Glacier block
				{12243999, 1618481214, ID{Hash: checksumToBytes(0xe029e991), Next: 12244000}},   // Last Muir Glacier block
				{12244000, 1618481223, ID{Hash: checksumToBytes(0x0eb440f6), Next: 12965000}},   // First Berlin block
				{12964999, 1628166812, ID{Hash: checksumToBytes(0x0eb440f6), Next: 12965000}},   // Last Berlin block
				{12965000, 1628166822, ID{Hash: checksumToBytes(0xb715077d), Next: 13773000}},   // First London block
				{13772999, 1639079715, ID{Hash: checksumToBytes(0xb715077d), Next: 13773000}},   // Last London block
				{13773000, 1639079723, ID{Hash: checksumToBytes(0x20c327fc), Next: 15050000}},   // First Arrow Glacier block
				{15049999, 1656586434, ID{Hash: checksumToBytes(0x20c327fc), Next: 15050000}},   // Last Arrow Glacier block
				{15050000, 1656586444, ID{Hash: checksumToBytes(0xf0afd0e3), Next: 1681338455}}, // First Gray Glacier block
				{17034869, 1681338443, ID{Hash: checksumToBytes(0xf0afd0e3), Next: 1681338455}}, // Last pre-Shanghai block
				{17034870, 1681338479, ID{Hash: checksumToBytes(0xdce96c2d), Next: 0}},          // First Shanghai block
				{19000000, 1700000000, ID{Hash: checksumToBytes(0xdce96c2d), Next: 0}},          // Future Shanghai block (mock)
			},
		},
		// Goerli test cases
		{
			params.GoerliChainConfig,
			params.GoerliGenesisHash,
			[]testcase{
				{0, 1548854791, ID{Hash: checksumToBytes(0xa3f5ab08), Next: 1561651}},          // Unsynced, last Frontier, Homestead, Tangerine, Spurious, Byzantium, Constantinople and first Petersburg block
				{1561650, 1572443570, ID{Hash: checksumToBytes(0xa3f5ab08), Next: 1561651}},    // Last Petersburg block
				{1561651, 1572443585, ID{Hash: checksumToBytes(0xc25efa5c), Next: 4460644}},    // First Istanbul block
				{4460643, 1616045376, ID{Hash: checksumToBytes(0xc25efa5c), Next: 4460644}},    // Last Istanbul block
				{4460644, 1616045391, ID{Hash: checksumToBytes(0x757a1c47), Next: 5062605}},    // First Berlin block
				{5062604, 1625109564, ID{Hash: checksumToBytes(0x757a1c47), Next: 5062605}},    // Last Berlin block
				{5062605, 1625109579, ID{Hash: checksumToBytes(0xB8C6299D), Next: 1678832736}}, // First London block
				{8656122, 1678832724, ID{Hash: checksumToBytes(0xB8C6299D), Next: 1678832736}}, // Last pre-Shanghai block
				{8656123, 1678832784, ID{Hash: checksumToBytes(0xf9843abf), Next: 0}},          // First Shanghai block
				{9900000, 1700000000, ID{Hash: checksumToBytes(0xf9843abf), Next: 0}},          // Future Shanghai block (mock)
			},
		},
		// Sepolia test cases
		{
			params.SepoliaChainConfig,
			params.SepoliaGenesisHash,
			[]testcase{
				{0, 1633267481, ID{Hash: checksumToBytes(0xfe3366e7), Next: 1735371}},          // Unsynced, last Frontier, Homestead, Tangerine, Spurious, Byzantium, Constantinople, Petersburg, Istanbul, Berlin and first London block
				{1735370, 1661130096, ID{Hash: checksumToBytes(0xfe3366e7), Next: 1735371}},    // Last pre-MergeNetsplit block
				{1735371, 1661130108, ID{Hash: checksumToBytes(0xb96cbd13), Next: 1677557088}}, // First MergeNetsplit block
				{2990907, 1677557076, ID{Hash: checksumToBytes(0xb96cbd13), Next: 1677557088}}, // Last pre-Shanghai block
				{2990908, 1677557088, ID{Hash: checksumToBytes(0xf7f9bc08), Next: 0}},          // First Shanghai block
				{5000000, 1700000000, ID{Hash: checksumToBytes(0xf7f9bc08), Next: 0}},          // Future Shanghai block (mock)
			},
		},
		// Gnosis test cases
		{
			params.GnosisChainConfig,
			params.GnosisGenesisHash,
			[]testcase{
				{0, 0, ID{Hash: checksumToBytes(0xf64909b1), Next: 1604400}},                    // Unsynced, last Frontier, Homestead, Tangerine, Spurious, Byzantium
				{1604399, 1547205885, ID{Hash: checksumToBytes(0xf64909b1), Next: 1604400}},     // Last Byzantium block
				{1604400, 1547205890, ID{Hash: checksumToBytes(0xfde2d083), Next: 2508800}},     // First Constantinople block
				{2508799, 1551879340, ID{Hash: checksumToBytes(0xfde2d083), Next: 2508800}},     // Last Constantinople block
				{2508800, 1551879345, ID{Hash: checksumToBytes(0xfc1d8f2f), Next: 7298030}},     // First Petersburg block
				{7298029, 1576134775, ID{Hash: checksumToBytes(0xfc1d8f2f), Next: 7298030}},     // Last Petersburg block
				{7298030, 1576134780, ID{Hash: checksumToBytes(0x54d05e6c), Next: 9186425}},     // First Istanbul block
				{9186424, 1585729685, ID{Hash: checksumToBytes(0x54d05e6c), Next: 9186425}},     // Last Istanbul block
				{9186425, 1585729690, ID{Hash: checksumToBytes(0xb6e6cd81), Next: 16101500}},    // First POSDAO Activation block
				{16101499, 1621258420, ID{Hash: checksumToBytes(0xb6e6cd81), Next: 16101500}},   // Last POSDAO Activation block
				{16101500, 1621258425, ID{Hash: checksumToBytes(0x069a83d9), Next: 19040000}},   // First Berlin block
				{19039999, 1636753575, ID{Hash: checksumToBytes(0x069a83d9), Next: 19040000}},   // Last Berlin block
				{19040000, 1636753580, ID{Hash: checksumToBytes(0x018479d3), Next: 1690889660}}, // First London block
				{21735000, 1650443255, ID{Hash: checksumToBytes(0x018479d3), Next: 1690889660}}, // First GIP-31 block
				{29272666, 1690889655, ID{Hash: checksumToBytes(0x018479d3), Next: 1690889660}}, // Last pre-Shanghai block (approx)
				{29272667, 1690889660, ID{Hash: checksumToBytes(0x2efe91ba), Next: 0}},          // First Shanghai block (approx)
			},
		},
		// Chiado test cases
		{
			params.ChiadoChainConfig,
			params.ChiadoGenesisHash,
			[]testcase{
				{0, 0, ID{Hash: checksumToBytes(0x50d39d7b), Next: 1684934220}},
				{4100418, 1684934215, ID{Hash: checksumToBytes(0x50d39d7b), Next: 1684934220}}, // Last pre-Shanghai block
				{4100419, 1684934220, ID{Hash: checksumToBytes(0xa15a4252), Next: 0}},          // First Shanghai block
			},
		},
	}
	for i, tt := range tests {
		for j, ttt := range tt.cases {
			if have := NewID(tt.config, tt.genesis, ttt.head, ttt.time); have != ttt.want {
				t.Errorf("test %d, case %d: fork ID mismatch: have %x, want %x", i, j, have, ttt.want)
			}
		}
	}
}

// TestValidation tests that a local peer correctly validates and accepts a remote
// fork ID.
func TestValidation(t *testing.T) {
	tests := []struct {
		head uint64
		id   ID
		err  error
	}{
		// Local is mainnet Petersburg, remote announces the same. No future fork is announced.
		{7987396, ID{Hash: checksumToBytes(0x668db0af), Next: 0}, nil},

		// Local is mainnet Petersburg, remote announces the same. Remote also announces a next fork
		// at block 0xffffffff, but that is uncertain.
		{7987396, ID{Hash: checksumToBytes(0x668db0af), Next: math.MaxUint64}, nil},

		// Local is mainnet currently in Byzantium only (so it's aware of Petersburg), remote announces
		// also Byzantium, but it's not yet aware of Petersburg (e.g. non updated node before the fork).
		// In this case we don't know if Petersburg passed yet or not.
		{7279999, ID{Hash: checksumToBytes(0xa00bc324), Next: 0}, nil},

		// Local is mainnet currently in Byzantium only (so it's aware of Petersburg), remote announces
		// also Byzantium, and it's also aware of Petersburg (e.g. updated node before the fork). We
		// don't know if Petersburg passed yet (will pass) or not.
		{7279999, ID{Hash: checksumToBytes(0xa00bc324), Next: 7280000}, nil},

		// Local is mainnet currently in Byzantium only (so it's aware of Petersburg), remote announces
		// also Byzantium, and it's also aware of some random fork (e.g. misconfigured Petersburg). As
		// neither forks passed at neither nodes, they may mismatch, but we still connect for now.
		{7279999, ID{Hash: checksumToBytes(0xa00bc324), Next: math.MaxUint64}, nil},

		// Local is mainnet Petersburg, remote announces Byzantium + knowledge about Petersburg. Remote
		// is simply out of sync, accept.
		{7987396, ID{Hash: checksumToBytes(0xa00bc324), Next: 7280000}, nil},

		// Local is mainnet Petersburg, remote announces Spurious + knowledge about Byzantium. Remote
		// is definitely out of sync. It may or may not need the Petersburg update, we don't know yet.
		{7987396, ID{Hash: checksumToBytes(0x3edd5b10), Next: 4370000}, nil},

		// Local is mainnet Byzantium, remote announces Petersburg. Local is out of sync, accept.
		{7279999, ID{Hash: checksumToBytes(0x668db0af), Next: 0}, nil},

		// Local is mainnet Spurious, remote announces Byzantium, but is not aware of Petersburg. Local
		// out of sync. Local also knows about a future fork, but that is uncertain yet.
		{4369999, ID{Hash: checksumToBytes(0xa00bc324), Next: 0}, nil},

		// Local is mainnet Petersburg. remote announces Byzantium but is not aware of further forks.
		// Remote needs software update.
		{7987396, ID{Hash: checksumToBytes(0xa00bc324), Next: 0}, ErrRemoteStale},

		// Local is mainnet Petersburg, and isn't aware of more forks. Remote announces Petersburg +
		// 0xffffffff. Local needs software update, reject.
		{7987396, ID{Hash: checksumToBytes(0x5cddc0e1), Next: 0}, ErrLocalIncompatibleOrStale},

		// Local is mainnet Byzantium, and is aware of Petersburg. Remote announces Petersburg +
		// 0xffffffff. Local needs software update, reject.
		{7279999, ID{Hash: checksumToBytes(0x5cddc0e1), Next: 0}, ErrLocalIncompatibleOrStale},

		// Local is mainnet Petersburg, remote is Rinkeby Petersburg.
		{7987396, ID{Hash: checksumToBytes(0xafec6b27), Next: 0}, ErrLocalIncompatibleOrStale},

		// Local is mainnet Gray Glacier, far in the future. Remote announces Gopherium (non existing fork)
		// at some future block 88888888, for itself, but past block for local. Local is incompatible.
		//
		// This case detects non-upgraded nodes with majority hash power (typical Ropsten mess).
		{88888888, ID{Hash: checksumToBytes(0xf0afd0e3), Next: 88888888}, ErrLocalIncompatibleOrStale},

		// Local is mainnet Byzantium. Remote is also in Byzantium, but announces Gopherium (non existing
		// fork) at block 7279999, before Petersburg. Local is incompatible.
		{7279999, ID{Hash: checksumToBytes(0xa00bc324), Next: 7279999}, ErrLocalIncompatibleOrStale},
	}
	heightForks, timeForks := GatherForks(params.MainnetChainConfig)
	for i, tt := range tests {
		filter := newFilter(heightForks, timeForks, params.MainnetGenesisHash, tt.head, 0)
		if err := filter(tt.id); err != tt.err {
			t.Errorf("test %d: validation error mismatch: have %v, want %v", i, err, tt.err)
		}
	}
}

// Tests that IDs are properly RLP encoded (specifically important because we
// use uint32 to store the hash, but we need to encode it as [4]byte).
func TestEncoding(t *testing.T) {
	tests := []struct {
		id   ID
		want []byte
	}{
		{ID{Hash: checksumToBytes(0), Next: 0}, common.Hex2Bytes("c6840000000080")},
		{ID{Hash: checksumToBytes(0xdeadbeef), Next: 0xBADDCAFE}, common.Hex2Bytes("ca84deadbeef84baddcafe,")},
		{ID{Hash: checksumToBytes(math.MaxUint32), Next: math.MaxUint64}, common.Hex2Bytes("ce84ffffffff88ffffffffffffffff")},
	}
	for i, tt := range tests {
		have, err := rlp.EncodeToBytes(tt.id)
		if err != nil {
			t.Errorf("test %d: failed to encode forkid: %v", i, err)
			continue
		}
		if !bytes.Equal(have, tt.want) {
			t.Errorf("test %d: RLP mismatch: have %x, want %x", i, have, tt.want)
		}
	}
}
