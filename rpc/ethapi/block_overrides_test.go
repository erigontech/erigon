package ethapi

import (
	"testing"

	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

func TestBlockOverridesOverrideGasLimit(t *testing.T) {
	originalTime := uint64(42)
	originalGasLimit := uint64(1_000_000)

	blockCtx := evmtypes.BlockContext{
		Time:     originalTime,
		GasLimit: originalGasLimit,
	}

	newGasLimit := hexutil.Uint64(30_000_000)
	overrides := BlockOverrides{GasLimit: &newGasLimit}

	if err := overrides.Override(&blockCtx); err != nil {
		t.Fatalf("override returned error: %v", err)
	}

	if blockCtx.GasLimit != uint64(newGasLimit) {
		t.Fatalf("unexpected gas limit override: have %d want %d", blockCtx.GasLimit, uint64(newGasLimit))
	}

	if blockCtx.Time != originalTime {
		t.Fatalf("timestamp changed by gas limit override: have %d want %d", blockCtx.Time, originalTime)
	}
}
