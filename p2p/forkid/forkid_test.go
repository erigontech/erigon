// Copyright 2019 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package forkid

import (
	"bytes"
	"math"
	"testing"

	"github.com/erigontech/erigon-lib/common"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/rlp"
	polychain "github.com/erigontech/erigon/polygon/chain"
)

// TestCreation tests that different genesis and fork rule combinations result in
// the correct fork ID.
// Forks before Shanghai are triggered by the block number,
// while Shanghai and later forks are triggered by the block time.
func TestCreation(t *testing.T) {
	t.Parallel()
	type testcase struct {
		head uint64
		time uint64
		want ID
	}
	tests := []struct {
		spec  chainspec.Spec
		cases []testcase
	}{
		{
			chainspec.Mainnet,
			[]testcase{
				{0, 0, ID{Hash: ChecksumToBytes(0xfc64ec04), Activation: 0, Next: 1150000}},                             // Unsynced
				{1149999, 1457981342, ID{Hash: ChecksumToBytes(0xfc64ec04), Activation: 0, Next: 1150000}},              // Last Frontier block
				{1150000, 1457981393, ID{Hash: ChecksumToBytes(0x97c2c34c), Activation: 1150000, Next: 1920000}},        // First Homestead block
				{1919999, 1469020838, ID{Hash: ChecksumToBytes(0x97c2c34c), Activation: 1150000, Next: 1920000}},        // Last Homestead block
				{1920000, 1469020840, ID{Hash: ChecksumToBytes(0x91d1f948), Activation: 1920000, Next: 2463000}},        // First DAO block
				{2462999, 1476796747, ID{Hash: ChecksumToBytes(0x91d1f948), Activation: 1920000, Next: 2463000}},        // Last DAO block
				{2463000, 1476796771, ID{Hash: ChecksumToBytes(0x7a64da13), Activation: 2463000, Next: 2675000}},        // First Tangerine block
				{2674999, 1479831337, ID{Hash: ChecksumToBytes(0x7a64da13), Activation: 2463000, Next: 2675000}},        // Last Tangerine block
				{2675000, 1479831344, ID{Hash: ChecksumToBytes(0x3edd5b10), Activation: 2675000, Next: 4370000}},        // First Spurious block
				{4369999, 1508131303, ID{Hash: ChecksumToBytes(0x3edd5b10), Activation: 2675000, Next: 4370000}},        // Last Spurious block
				{4370000, 1508131331, ID{Hash: ChecksumToBytes(0xa00bc324), Activation: 4370000, Next: 7280000}},        // First Byzantium block
				{7279999, 1551383501, ID{Hash: ChecksumToBytes(0xa00bc324), Activation: 4370000, Next: 7280000}},        // Last Byzantium block
				{7280000, 1551383524, ID{Hash: ChecksumToBytes(0x668db0af), Activation: 7280000, Next: 9069000}},        // First and last Constantinople, first Petersburg block
				{9068999, 1575764708, ID{Hash: ChecksumToBytes(0x668db0af), Activation: 7280000, Next: 9069000}},        // Last Petersburg block
				{9069000, 1575764709, ID{Hash: ChecksumToBytes(0x879d6e30), Activation: 9069000, Next: 9200000}},        // First Istanbul block
				{9199999, 1577953806, ID{Hash: ChecksumToBytes(0x879d6e30), Activation: 9069000, Next: 9200000}},        // Last Istanbul block
				{9200000, 1577953849, ID{Hash: ChecksumToBytes(0xe029e991), Activation: 9200000, Next: 12244000}},       // First Muir Glacier block
				{12243999, 1618481214, ID{Hash: ChecksumToBytes(0xe029e991), Activation: 9200000, Next: 12244000}},      // Last Muir Glacier block
				{12244000, 1618481223, ID{Hash: ChecksumToBytes(0x0eb440f6), Activation: 12244000, Next: 12965000}},     // First Berlin block
				{12964999, 1628166812, ID{Hash: ChecksumToBytes(0x0eb440f6), Activation: 12244000, Next: 12965000}},     // Last Berlin block
				{12965000, 1628166822, ID{Hash: ChecksumToBytes(0xb715077d), Activation: 12965000, Next: 13773000}},     // First London block
				{13772999, 1639079715, ID{Hash: ChecksumToBytes(0xb715077d), Activation: 12965000, Next: 13773000}},     // Last London block
				{13773000, 1639079723, ID{Hash: ChecksumToBytes(0x20c327fc), Activation: 13773000, Next: 15050000}},     // First Arrow Glacier block
				{15049999, 1656586434, ID{Hash: ChecksumToBytes(0x20c327fc), Activation: 13773000, Next: 15050000}},     // Last Arrow Glacier block
				{15050000, 1656586444, ID{Hash: ChecksumToBytes(0xf0afd0e3), Activation: 15050000, Next: 1681338455}},   // First Gray Glacier block
				{17034869, 1681338443, ID{Hash: ChecksumToBytes(0xf0afd0e3), Activation: 15050000, Next: 1681338455}},   // Last pre-Shanghai block
				{17034870, 1681338479, ID{Hash: ChecksumToBytes(0xdce96c2d), Activation: 1681338455, Next: 1710338135}}, // First Shanghai block
				{19426586, 1710338123, ID{Hash: ChecksumToBytes(0xdce96c2d), Activation: 1681338455, Next: 1710338135}}, // Last Shanghai block
				{19426587, 1710338135, ID{Hash: ChecksumToBytes(0x9f3d2254), Activation: 1710338135, Next: 1746612311}}, // First Cancun block
				{22432453, 1746612299, ID{Hash: ChecksumToBytes(0x9f3d2254), Activation: 1710338135, Next: 1746612311}}, // Last Cancun block (approx.)
				{22432454, 1746612311, ID{Hash: ChecksumToBytes(0xc376cf8b), Activation: 1746612311, Next: 0}},          // First Prague block (approx.)
				{30000000, 1900000000, ID{Hash: ChecksumToBytes(0xc376cf8b), Activation: 1746612311, Next: 0}},          // Future Prague block (mock)
			},
		},
		{
			chainspec.Sepolia,
			[]testcase{
				{0, 1633267481, ID{Hash: ChecksumToBytes(0xfe3366e7), Activation: 0, Next: 1735371}},                   // Unsynced, last Frontier, Homestead, Tangerine, Spurious, Byzantium, Constantinople, Petersburg, Istanbul, Berlin and first London block
				{1735370, 1661130096, ID{Hash: ChecksumToBytes(0xfe3366e7), Activation: 0, Next: 1735371}},             // Last pre-MergeNetsplit block
				{1735371, 1661130108, ID{Hash: ChecksumToBytes(0xb96cbd13), Activation: 1735371, Next: 1677557088}},    // First MergeNetsplit block
				{2990907, 1677557076, ID{Hash: ChecksumToBytes(0xb96cbd13), Activation: 1735371, Next: 1677557088}},    // Last pre-Shanghai block
				{2990908, 1677557088, ID{Hash: ChecksumToBytes(0xf7f9bc08), Activation: 1677557088, Next: 1706655072}}, // First Shanghai block
				{5187022, 1706655060, ID{Hash: ChecksumToBytes(0xf7f9bc08), Activation: 1677557088, Next: 1706655072}}, // Last Shanghai block
				{5187023, 1706655072, ID{Hash: ChecksumToBytes(0x88cf81d9), Activation: 1706655072, Next: 1741159776}}, // First Cancun block
				{7844466, 1741159764, ID{Hash: ChecksumToBytes(0x88cf81d9), Activation: 1706655072, Next: 1741159776}}, // Last Cancun block (approx)
				{7844467, 1741159776, ID{Hash: ChecksumToBytes(0xed88b5fd), Activation: 1741159776, Next: 0}},          // First Prague block (approx)
				{12000000, 1800000000, ID{Hash: ChecksumToBytes(0xed88b5fd), Activation: 1741159776, Next: 0}},         // Future Prague block (mock)
			},
		},
		{
			chainspec.Holesky,
			[]testcase{
				{0, 1696000704, ID{Hash: ChecksumToBytes(0xfd4f016b), Activation: 1696000704, Next: 1707305664}},       // First Shanghai block
				{0, 1707305652, ID{Hash: ChecksumToBytes(0xfd4f016b), Activation: 1696000704, Next: 1707305664}},       // Last Shanghai block
				{894733, 1707305676, ID{Hash: ChecksumToBytes(0x9b192ad0), Activation: 1707305664, Next: 1740434112}},  // First Cancun block
				{3655435, 1740434100, ID{Hash: ChecksumToBytes(0x9b192ad0), Activation: 1707305664, Next: 1740434112}}, // Last Cancun block (approx)
				{3655436, 1740434112, ID{Hash: ChecksumToBytes(0xdfbd9bed), Activation: 1740434112, Next: 0}},          // First Prague block (approx)
				{8000000, 1800000000, ID{Hash: ChecksumToBytes(0xdfbd9bed), Activation: 1740434112, Next: 0}},          // Future Prague block (mock)
			},
		},
		{
			chainspec.Hoodi,
			[]testcase{
				{0, 174221200, ID{Hash: ChecksumToBytes(0xbef71d30), Activation: 0, Next: 1742999832}},        // First Cancun block
				{50000, 1742999820, ID{Hash: ChecksumToBytes(0xbef71d30), Activation: 0, Next: 1742999832}},   // Last Cancun block (approx)
				{50001, 1742999832, ID{Hash: ChecksumToBytes(0x0929e24e), Activation: 1742999832, Next: 0}},   // First Prague block (approx)
				{8000000, 1800000000, ID{Hash: ChecksumToBytes(0x0929e24e), Activation: 1742999832, Next: 0}}, // Future Prague block (mock)
			},
		},
		{
			chainspec.Gnosis,
			[]testcase{
				{0, 0, ID{Hash: ChecksumToBytes(0xf64909b1), Activation: 0, Next: 1604400}},                             // Unsynced, last Frontier, Homestead, Tangerine, Spurious, Byzantium
				{1604399, 1547205885, ID{Hash: ChecksumToBytes(0xf64909b1), Activation: 0, Next: 1604400}},              // Last Byzantium block
				{1604400, 1547205890, ID{Hash: ChecksumToBytes(0xfde2d083), Activation: 1604400, Next: 2508800}},        // First Constantinople block
				{2508799, 1551879340, ID{Hash: ChecksumToBytes(0xfde2d083), Activation: 1604400, Next: 2508800}},        // Last Constantinople block
				{2508800, 1551879345, ID{Hash: ChecksumToBytes(0xfc1d8f2f), Activation: 2508800, Next: 7298030}},        // First Petersburg block
				{7298029, 1576134775, ID{Hash: ChecksumToBytes(0xfc1d8f2f), Activation: 2508800, Next: 7298030}},        // Last Petersburg block
				{7298030, 1576134780, ID{Hash: ChecksumToBytes(0x54d05e6c), Activation: 7298030, Next: 9186425}},        // First Istanbul block
				{9186424, 1585729685, ID{Hash: ChecksumToBytes(0x54d05e6c), Activation: 7298030, Next: 9186425}},        // Last Istanbul block
				{9186425, 1585729690, ID{Hash: ChecksumToBytes(0xb6e6cd81), Activation: 9186425, Next: 16101500}},       // First POSDAO Activation block
				{16101499, 1621258420, ID{Hash: ChecksumToBytes(0xb6e6cd81), Activation: 9186425, Next: 16101500}},      // Last POSDAO Activation block
				{16101500, 1621258425, ID{Hash: ChecksumToBytes(0x069a83d9), Activation: 16101500, Next: 19040000}},     // First Berlin block
				{19039999, 1636753575, ID{Hash: ChecksumToBytes(0x069a83d9), Activation: 16101500, Next: 19040000}},     // Last Berlin block
				{19040000, 1636753580, ID{Hash: ChecksumToBytes(0x018479d3), Activation: 19040000, Next: 1690889660}},   // First London block
				{21735000, 1650443255, ID{Hash: ChecksumToBytes(0x018479d3), Activation: 19040000, Next: 1690889660}},   // First GIP-31 block
				{29242931, 1690889650, ID{Hash: ChecksumToBytes(0x018479d3), Activation: 19040000, Next: 1690889660}},   // Last pre-Shanghai block
				{29242932, 1690889660, ID{Hash: ChecksumToBytes(0x2efe91ba), Activation: 1690889660, Next: 1710181820}}, // First Shanghai block
				{32880679, 1710181810, ID{Hash: ChecksumToBytes(0x2efe91ba), Activation: 1690889660, Next: 1710181820}}, // Last Shanghai block
				{32880680, 1710181820, ID{Hash: ChecksumToBytes(0x1384dfc1), Activation: 1710181820, Next: 1746021820}}, // First Cancun block
				{39834364, 1746021815, ID{Hash: ChecksumToBytes(0x1384dfc1), Activation: 1710181820, Next: 1746021820}}, // Last Cancun block (approx)
				{39834365, 1746021820, ID{Hash: ChecksumToBytes(0x2f095d4a), Activation: 1746021820, Next: 0}},          // First Prague block (approx)
				{50000000, 1800000000, ID{Hash: ChecksumToBytes(0x2f095d4a), Activation: 1746021820, Next: 0}},          // Future Prague block (mock)
			},
		},
		{
			chainspec.Chiado,
			[]testcase{
				{0, 0, ID{Hash: ChecksumToBytes(0x50d39d7b), Activation: 0, Next: 1684934220}},
				{4100418, 1684934215, ID{Hash: ChecksumToBytes(0x50d39d7b), Activation: 0, Next: 1684934220}},           // Last pre-Shanghai block
				{4100419, 1684934220, ID{Hash: ChecksumToBytes(0xa15a4252), Activation: 1684934220, Next: 1706724940}},  // First Shanghai block
				{8021277, 1706724930, ID{Hash: ChecksumToBytes(0xa15a4252), Activation: 1684934220, Next: 1706724940}},  // Last Shanghai block
				{8021278, 1706724940, ID{Hash: ChecksumToBytes(0x5fbc16bc), Activation: 1706724940, Next: 1741254220}},  // First Cancun block
				{14655798, 1741254215, ID{Hash: ChecksumToBytes(0x5fbc16bc), Activation: 1706724940, Next: 1741254220}}, // Last Cancun block (approx)
				{14655799, 1741254220, ID{Hash: ChecksumToBytes(0x8ba51786), Activation: 1741254220, Next: 0}},          // First Prague block (approx)
				{20000000, 1800000000, ID{Hash: ChecksumToBytes(0x8ba51786), Activation: 1741254220, Next: 0}},          // Future Prague block (mock)
			},
		},
		{
			polychain.Amoy,
			[]testcase{
				{0, 0, ID{Hash: ChecksumToBytes(0xbe06a477), Activation: 0, Next: 73100}},
				{73100, 0, ID{Hash: ChecksumToBytes(0x135d2cd5), Activation: 73100, Next: 5423600}}, // First London, Jaipur, Delhi, Indore, Agra
			},
		},
		{
			polychain.BorMainnet,
			[]testcase{
				{0, 0, ID{Hash: ChecksumToBytes(0x0e07e722), Activation: 0, Next: 3395000}},
				{3395000, 0, ID{Hash: ChecksumToBytes(0x27806576), Activation: 3395000, Next: 14750000}},   // First Istanbul block
				{14750000, 0, ID{Hash: ChecksumToBytes(0x66e26adb), Activation: 14750000, Next: 23850000}}, // First Berlin block
				{23850000, 0, ID{Hash: ChecksumToBytes(0x4f2f71cc), Activation: 23850000, Next: 50523000}}, // First London block
				{50523000, 0, ID{Hash: ChecksumToBytes(0xdc08865c), Activation: 50523000, Next: 54876000}}, // First Agra block
				{54876000, 0, ID{Hash: ChecksumToBytes(0xf097bc13), Activation: 54876000, Next: 73440256}}, // First Napoli block
			},
		},
	}
	for i, tt := range tests {
		for j, ttt := range tt.cases {
			heightForks, timeForks := GatherForks(tt.spec.Config, 0 /* genesisTime */)
			if have := NewIDFromForks(heightForks, timeForks, tt.spec.GenesisHash, ttt.head, ttt.time); have != ttt.want {
				t.Errorf("test %d, case %d: fork ID mismatch: have %x, want %x", i, j, have, ttt.want)
			}
		}
	}
}

// TestValidation tests that a local peer correctly validates and accepts a remote
// fork ID.
func TestValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		head uint64
		id   ID
		err  error
	}{
		// Local is mainnet Petersburg, remote announces the same. No future fork is announced.
		{7987396, ID{Hash: ChecksumToBytes(0x668db0af), Next: 0}, nil},

		// Local is mainnet Petersburg, remote announces the same. Remote also announces a next fork
		// at block 0xffffffff, but that is uncertain.
		{7987396, ID{Hash: ChecksumToBytes(0x668db0af), Next: math.MaxUint64}, nil},

		// Local is mainnet currently in Byzantium only (so it's aware of Petersburg), remote announces
		// also Byzantium, but it's not yet aware of Petersburg (e.g. non updated node before the fork).
		// In this case we don't know if Petersburg passed yet or not.
		{7279999, ID{Hash: ChecksumToBytes(0xa00bc324), Next: 0}, nil},

		// Local is mainnet currently in Byzantium only (so it's aware of Petersburg), remote announces
		// also Byzantium, and it's also aware of Petersburg (e.g. updated node before the fork). We
		// don't know if Petersburg passed yet (will pass) or not.
		{7279999, ID{Hash: ChecksumToBytes(0xa00bc324), Next: 7280000}, nil},

		// Local is mainnet currently in Byzantium only (so it's aware of Petersburg), remote announces
		// also Byzantium, and it's also aware of some random fork (e.g. misconfigured Petersburg). As
		// neither forks passed at neither nodes, they may mismatch, but we still connect for now.
		{7279999, ID{Hash: ChecksumToBytes(0xa00bc324), Next: math.MaxUint64}, nil},

		// Local is mainnet Petersburg, remote announces Byzantium + knowledge about Petersburg. Remote
		// is simply out of sync, accept.
		{7987396, ID{Hash: ChecksumToBytes(0xa00bc324), Next: 7280000}, nil},

		// Local is mainnet Petersburg, remote announces Spurious + knowledge about Byzantium. Remote
		// is definitely out of sync. It may or may not need the Petersburg update, we don't know yet.
		{7987396, ID{Hash: ChecksumToBytes(0x3edd5b10), Next: 4370000}, nil},

		// Local is mainnet Byzantium, remote announces Petersburg. Local is out of sync, accept.
		{7279999, ID{Hash: ChecksumToBytes(0x668db0af), Next: 0}, nil},

		// Local is mainnet Spurious, remote announces Byzantium, but is not aware of Petersburg. Local
		// out of sync. Local also knows about a future fork, but that is uncertain yet.
		{4369999, ID{Hash: ChecksumToBytes(0xa00bc324), Next: 0}, nil},

		// Local is mainnet Petersburg. remote announces Byzantium but is not aware of further forks.
		// Remote needs software update.
		{7987396, ID{Hash: ChecksumToBytes(0xa00bc324), Next: 0}, ErrRemoteStale},

		// Local is mainnet Petersburg, and isn't aware of more forks. Remote announces Petersburg +
		// 0xffffffff. Local needs software update, reject.
		{7987396, ID{Hash: ChecksumToBytes(0x5cddc0e1), Next: 0}, ErrLocalIncompatibleOrStale},

		// Local is mainnet Byzantium, and is aware of Petersburg. Remote announces Petersburg +
		// 0xffffffff. Local needs software update, reject.
		{7279999, ID{Hash: ChecksumToBytes(0x5cddc0e1), Next: 0}, ErrLocalIncompatibleOrStale},

		// Local is mainnet Petersburg, remote is Rinkeby Petersburg.
		{7987396, ID{Hash: ChecksumToBytes(0xafec6b27), Next: 0}, ErrLocalIncompatibleOrStale},

		// Local is mainnet Gray Glacier, far in the future. Remote announces Gopherium (non existing fork)
		// at some future block 88888888, for itself, but past block for local. Local is incompatible.
		//
		// This case detects non-upgraded nodes with majority hash power (typical Ropsten mess).
		{88888888, ID{Hash: ChecksumToBytes(0xf0afd0e3), Next: 88888888}, ErrLocalIncompatibleOrStale},

		// Local is mainnet Byzantium. Remote is also in Byzantium, but announces Gopherium (non existing
		// fork) at block 7279999, before Petersburg. Local is incompatible.
		{7279999, ID{Hash: ChecksumToBytes(0xa00bc324), Next: 7279999}, ErrLocalIncompatibleOrStale},
	}
	heightForks, timeForks := GatherForks(chainspec.Mainnet.Config, 0 /* genesisTime */)
	for i, tt := range tests {
		filter := newFilter(heightForks, timeForks, chainspec.Mainnet.GenesisHash, tt.head, 0)
		if err := filter(tt.id); err != tt.err {
			t.Errorf("test %d: validation error mismatch: have %v, want %v", i, err, tt.err)
		}
	}
}

// Tests that IDs are properly RLP encoded (specifically important because we
// use uint32 to store the hash, but we need to encode it as [4]byte).
func TestEncoding(t *testing.T) {
	t.Parallel()
	tests := []struct {
		id   ID
		want []byte
	}{
		{ID{Hash: ChecksumToBytes(0), Next: 0}, common.Hex2Bytes("c6840000000080")},
		{ID{Hash: ChecksumToBytes(0xdeadbeef), Next: 0xBADDCAFE}, common.Hex2Bytes("ca84deadbeef84baddcafe,")},
		{ID{Hash: ChecksumToBytes(math.MaxUint32), Next: math.MaxUint64}, common.Hex2Bytes("ce84ffffffff88ffffffffffffffff")},
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
