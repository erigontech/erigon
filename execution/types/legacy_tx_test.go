package types

import (
	"bytes"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_LegacyTx_Timeboosted(t *testing.T) {
	two := uint256.NewInt(2)
	ltx := NewTransaction(4, common.HexToAddress("0x2"), two, 21000, two, []byte("data"))
	ltx.Timeboosted = true

	buf := bytes.NewBuffer(nil)
	err := ltx.EncodeRLP(buf)
	require.NoError(t, err)

	var ltx2 LegacyTx
	stream := rlp.NewStream(bytes.NewReader(buf.Bytes()), uint64(buf.Len()))
	err = ltx2.DecodeRLP(stream)
	require.NoError(t, err)

	require.EqualValues(t, ltx.Timeboosted, ltx2.Timeboosted)
	require.EqualValues(t, ltx.GasLimit, ltx2.GasLimit)
	require.EqualValues(t, ltx.GasPrice.Bytes(), ltx2.GasPrice.Bytes())
	require.EqualValues(t, ltx.Value.Bytes(), ltx2.Value.Bytes())
	require.EqualValues(t, ltx.Data, ltx2.Data)
	require.EqualValues(t, ltx.To, ltx2.To)

	require.True(t, ltx2.IsTimeBoosted())
}
