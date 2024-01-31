package types

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"testing"
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

	if result.Index != in.Index {
		t.Errorf("index mismatch")
	}
	if result.GER != in.GER {
		t.Errorf("ger mismatch")
	}
	if result.MainnetExitRoot != in.MainnetExitRoot {
		t.Errorf("mainnet exit root mismatch")
	}
	if result.RollupExitRoot != in.RollupExitRoot {
		t.Errorf("rollup exit root mismatch")
	}
	if result.ParentHash != in.ParentHash {
		t.Errorf("parent hash mismatch")
	}
	if result.Timestamp != in.Timestamp {
		t.Errorf("timestamp mismatch")
	}
}
