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

package protocol

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
)

const noncelessTxType = 0x7b

func init() {
	types.RegisterTxType(noncelessTxType, types.TxTypeSpec{New: func() types.Transaction { return nil }, Nonceless: true})
}

// TestNonceless: a nonceless type neither increments nor checks its sender's
// nonce, even when the message asks for the check, as ApplyTransaction's do.
func TestNonceless(t *testing.T) {
	for _, tc := range []struct {
		name       string
		txType     byte
		checkNonce bool
		nonce      uint64
		err        error
	}{
		{name: "nonceless", txType: noncelessTxType, nonce: 5},
		{name: "nonceless, nonce checked", txType: noncelessTxType, checkNonce: true, nonce: 5},
		{name: "other", nonce: 6},
		{name: "other, nonce checked", checkNonce: true, nonce: 5, err: ErrNonceTooLow},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ibs := state.New(state.NewNoopReader())
			defer ibs.Close()
			require.NoError(t, ibs.SetNonce(minter, 5, tracing.NonceChangeUnspecified))

			msg := mintingMsg(new(uint256.Int), tc.txType)
			msg.SetCheckNonce(tc.checkNonce)
			res, err := applyMinting(t, ibs, msg)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
			} else {
				require.NoError(t, err)
				require.False(t, res.Failed())
			}
			nonce, err := ibs.GetNonce(minter)
			require.NoError(t, err)
			require.Equal(t, tc.nonce, nonce)
		})
	}
}
