package types

import (
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/require"
)

func Test_L1InfoTreeMarshallUnmarshall(t *testing.T) {
	ger := libcommon.HexToHash("0x1")
	mainnet := libcommon.HexToHash("0x2")
	rollup := libcommon.HexToHash("0x3")
	parent := libcommon.HexToHash("0x4")
	timestamp := uint64(1000)
	in := L1InfoTreeUpdate{
		Index:           1,
		GER:             ger,
		MainnetExitRoot: mainnet,
		RollupExitRoot:  rollup,
		ParentHash:      parent,
		Timestamp:       timestamp,
	}
	marshalled := in.Marshall()

	result := L1InfoTreeUpdate{}
	result.Unmarshall(marshalled)

	require.Equal(t, in, result)
}

func Test_L1InjectedBatchMarshallUnmarshall(t *testing.T) {
	input := &L1InjectedBatch{
		L1BlockNumber:      1,
		Timestamp:          1000,
		L1BlockHash:        libcommon.HexToHash("0x1"),
		L1ParentHash:       libcommon.HexToHash("0x2"),
		LastGlobalExitRoot: libcommon.HexToHash("0x3"),
		Sequencer:          libcommon.HexToAddress("0x4"),
		Transaction:        []byte{100},
	}

	marshalled := input.Marshall()

	result := &L1InjectedBatch{}
	err := result.Unmarshall(marshalled)
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, input, result)
}
