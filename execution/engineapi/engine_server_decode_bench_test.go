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

package engineapi

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/types"
)

func BenchmarkDecodePayloadTransactions(b *testing.B) {
	config := preAmsterdamChainConfig()
	key, err := crypto.GenerateKey()
	require.NoError(b, err)
	signer := types.MakeSigner(config, 1, 0)
	to := common.Address{0xb}
	txns := make(types.Transactions, 10_000)
	for i := range txns {
		txns[i], err = types.SignTx(&types.LegacyTx{CommonTx: types.CommonTx{Nonce: uint64(i), GasLimit: 21_000, To: &to}, GasPrice: *uint256.NewInt(7)}, *signer, key)
		require.NoError(b, err)
	}
	bins, err := types.MarshalTransactionsBinary(txns)
	require.NoError(b, err)
	raw := make([]hexutil.Bytes, len(bins))
	for i := range bins {
		raw[i] = bins[i]
	}
	srv := NewEngineServer(log.New(), config, &stubExecutionModule{}, nil, false, false, false, true, nil, nil, 0, 0)
	b.ReportAllocs()
	for b.Loop() {
		if _, _, _, status := srv.decodePayloadTransactions(raw, 150_000_000, false, false, nil); status != nil {
			b.Fatal(status.ValidationError.Error())
		}
	}
}
