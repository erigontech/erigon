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

package misc

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

func validDepositLogData(t *testing.T) []byte {
	t.Helper()
	pubkey := bytes.Repeat([]byte{0x01}, BLSPubKeyLen)
	wc := bytes.Repeat([]byte{0x02}, WithdrawalCredentialsLen)
	amount := bytes.Repeat([]byte{0x03}, 8)
	sig := bytes.Repeat([]byte{0x04}, BLSSigLen)
	index := bytes.Repeat([]byte{0x05}, 8)

	data, err := depositEvent.Inputs.Pack(pubkey, wc, amount, sig, index)
	require.NoError(t, err)
	require.Len(t, data, DepositLogLen)
	return data
}

func TestValidateDepositLog(t *testing.T) {
	t.Parallel()
	data := validDepositLogData(t)
	require.NoError(t, validateDepositLog(data))
}

func TestValidateDepositLog_Errors(t *testing.T) {
	t.Parallel()

	wrongOffset := bytes.Clone(validDepositLogData(t))
	wrongOffset[31] = 0x00 // pubkeyOffset no longer == 160

	wrongSize := bytes.Clone(validDepositLogData(t))
	wrongSize[191] = 0x00 // pubkeySize no longer == 48

	tests := map[string][]byte{
		"too short":    validDepositLogData(t)[:DepositLogLen-1],
		"wrong offset": wrongOffset,
		"wrong size":   wrongSize,
	}
	for name, data := range tests {
		data := data
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.ErrorIs(t, validateDepositLog(data), InvalidDepositLogErr)
		})
	}
}

func TestUnpackDepositLog(t *testing.T) {
	t.Parallel()
	data := validDepositLogData(t)

	out, err := unpackDepositLog(data)
	require.NoError(t, err)

	want := bytes.Join([][]byte{
		bytes.Repeat([]byte{0x01}, BLSPubKeyLen),
		bytes.Repeat([]byte{0x02}, WithdrawalCredentialsLen),
		bytes.Repeat([]byte{0x03}, 8),
		bytes.Repeat([]byte{0x04}, BLSSigLen),
		bytes.Repeat([]byte{0x05}, 8),
	}, nil)
	require.Equal(t, want, out)
}

func TestUnpackDepositLog_Invalid(t *testing.T) {
	t.Parallel()
	_, err := unpackDepositLog([]byte{0x00})
	require.ErrorIs(t, err, InvalidDepositLogErr)
}

func TestParseDepositLogs(t *testing.T) {
	t.Parallel()
	contract := common.HexToAddress("0x00000000219ab540356cbb839cbe05303d7705fa")
	data := validDepositLogData(t)

	matching := &types.Log{Address: contract, Topics: []common.Hash{depositTopic}, Data: data}

	req, err := ParseDepositLogs([]*types.Log{matching}, contract)
	require.NoError(t, err)
	require.NotNil(t, req)
	require.Equal(t, types.DepositRequestType, req.Type)
	require.NotEmpty(t, req.RequestData)
}

func TestParseDepositLogs_NoMatch(t *testing.T) {
	t.Parallel()
	contract := common.HexToAddress("0x00000000219ab540356cbb839cbe05303d7705fa")
	other := common.HexToAddress("0xdead")

	// Right topic, wrong address -> skipped.
	wrongAddr := &types.Log{Address: other, Topics: []common.Hash{depositTopic}, Data: validDepositLogData(t)}
	req, err := ParseDepositLogs([]*types.Log{wrongAddr}, contract)
	require.NoError(t, err)
	require.Nil(t, req)

	// No logs at all.
	req, err = ParseDepositLogs(nil, contract)
	require.NoError(t, err)
	require.Nil(t, req)
}

func TestParseDepositLogs_BadData(t *testing.T) {
	t.Parallel()
	contract := common.HexToAddress("0x00000000219ab540356cbb839cbe05303d7705fa")
	bad := &types.Log{Address: contract, Topics: []common.Hash{depositTopic}, Data: []byte{0x01, 0x02}}

	_, err := ParseDepositLogs([]*types.Log{bad}, contract)
	require.Error(t, err)
}
