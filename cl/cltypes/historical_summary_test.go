package cltypes_test

import (
	"testing"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/common"
	"github.com/stretchr/testify/require"
)

var (
	serializedHistoricalSummarySnappy = common.Hex2Bytes("40f03f6c6eee828430632dd18c6b608ea98806380fe7711b75ed235551bc95dacfc04c158258ebdb1c95ff566d6e458fc220d2f345bfc063fe717dd26e0c161f70d7ce")
	historicalSummaryRoot             = common.Hex2Bytes("7be1818e82411b397340c97f61d2c785397f5c41747e6c94cef3cba91262db22")
)

func TestHistoricalSummary(t *testing.T) {
	decompressed, _ := utils.DecompressSnappy(serializedHistoricalSummarySnappy)
	obj := &cltypes.HistoricalSummary{}
	require.NoError(t, obj.DecodeSSZ(decompressed))
	root, err := obj.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, root[:], historicalSummaryRoot)
	reencoded, err := obj.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, reencoded, decompressed)
}
