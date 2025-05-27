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
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon-p2p/testutil"
)

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
	heightForks, timeForks := GatherForks(testutil.PoWMainnetChainConfig, 0 /* genesisTime */)
	for i, tt := range tests {
		filter := newFilter(heightForks, timeForks, testutil.MainnetGenesisHash, tt.head, 0)
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
