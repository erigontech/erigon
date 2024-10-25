package types

import (
	"encoding/json"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/require"
)

func TestTxnTrace_MarshalJSON(t *testing.T) {
	balance := uint256.NewInt(32)
	nonce := uint256.NewInt(64)
	storageRead := []common.Hash{common.HexToHash("0x1")}
	selfDestructed := false
	codeRead := common.HexToHash("0x6")

	trace := TxnTrace{
		Balance:     balance,
		Nonce:       nonce,
		StorageRead: storageRead,
		StorageWritten: map[common.Hash]*uint256.Int{
			common.HexToHash("0x7"): uint256.NewInt(7),
			common.HexToHash("0x8"): uint256.NewInt(8),
			common.HexToHash("0x9"): uint256.NewInt(9),
		},
		CodeUsage: &ContractCodeUsage{
			Read: &codeRead,
		},
		SelfDestructed: &selfDestructed,
	}

	traceJSON, err := trace.MarshalJSON()
	require.NoError(t, err)

	// Parse the JSON output into a generic map and assert balance and nonce are hex encoded
	var genericTraceResult map[string]interface{}
	err = json.Unmarshal(traceJSON, &genericTraceResult)
	require.NoError(t, err)

	require.Equal(t, balance.Hex(), genericTraceResult["balance"])
	require.Equal(t, nonce.Hex(), genericTraceResult["nonce"])

	var resultTrace TxnTrace
	err = json.Unmarshal(traceJSON, &resultTrace)
	require.NoError(t, err)
	require.Equal(t, trace, resultTrace)
}
