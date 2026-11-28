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

package leansphincs

import (
	"testing"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
)

func BenchmarkVerify(b *testing.B) {
	sig := katSig(b)
	for b.Loop() {
		if !Verify(katPub, katMsg, sig) {
			b.Fatal("verify error")
		}
	}
}

func BenchmarkSign(b *testing.B) {
	for b.Loop() {
		if _, err := Sign(katSeed, katMsg); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkKeyGen(b *testing.B) {
	for b.Loop() {
		if _, err := KeyGen(katSeed); err != nil {
			b.Fatal(err)
		}
	}
}

// ecrecover at 3000 gas is the baseline leanSPHINCS verification is priced against.

func BenchmarkEcrecover(b *testing.B) {
	msg := hexutil.MustDecode("0xce0677bb30baa8cf067c88db9811f4333d131bf8bcf12fe7065d211dce971008")
	sig := hexutil.MustDecode("0x90f27b8b488db00b00606796d2987f6a5f59ae62ea05effe84fef5b8b0e549984a691139ad57a3f0b906637673aa2f63d1f55cb1a69199d4009eea23ceaddc9301")
	for b.Loop() {
		if _, err := crypto.Ecrecover(msg, sig); err != nil {
			b.Fatal(err)
		}
	}
}
