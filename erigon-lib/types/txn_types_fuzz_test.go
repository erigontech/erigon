// Copyright 2024 The Erigon Authors
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

//go:build !nofuzz

package types

import (
	"testing"

	"github.com/erigontech/erigon-lib/common/u256"
)

// golang.org/s/draft-fuzzing-design
//go doc testing
//go doc testing.F
//go doc testing.F.AddRemoteTxs
//go doc testing.F.Fuzz

// go test -trimpath -v -fuzz=Fuzz -fuzztime=10s ./txpool

//func init() {
//	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
//}

func FuzzParseTx(f *testing.F) {
	f.Add([]byte{1}, 0)
	f.Fuzz(func(t *testing.T, in []byte, pos int) {
		t.Parallel()
		ctx := NewTxParseContext(*u256.N1)
		txn := &TxSlot{}
		sender := make([]byte, 20)
		_, _ = ctx.ParseTransaction(in, pos, txn, sender, false /* hasEnvelope */, true /* wrappedWithBlobs */, nil)
	})
}
