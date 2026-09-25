package protocol_test

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type baselineEngine struct {
	rules.EngineReader
	blockNum  uint64
	txIndex   int
	baselines []rules.StorageBaseline
}

func (e baselineEngine) StorageBaselines(blockNum uint64, txIndex int) []rules.StorageBaseline {
	if blockNum != e.blockNum || txIndex != e.txIndex {
		return nil
	}
	return e.baselines
}

// TestSetTxContextInstallsStorageBaselines covers the replay paths: they set the
// tx context and then call ApplyMessage, so the engine's overrides have to ride
// along with the tx context or they are silently skipped.
func TestSetTxContextInstallsStorageBaselines(t *testing.T) {
	t.Parallel()

	contract := accounts.InternAddress(common.HexToAddress("0x89791428868131eb109e42340ad01eb8987526b2"))
	key := accounts.InternKey(common.HexToHash("0xf1e9242398de526b8dd9c25d38e65fbb01926b8940377762d7884b8b0dcdc3b0"))
	value := uint256.MustFromHex("0xf6a7831804efd2cd0a")

	engine := baselineEngine{
		blockNum:  35547779,
		txIndex:   196,
		baselines: []rules.StorageBaseline{{Address: contract, Key: key, Value: *value}},
	}

	dirs := datadir.New(t.TempDir())
	tx, err := temporaltest.NewTestDB(t, dirs).BeginTemporalRw(t.Context()) //nolint:gocritic
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(t, err)
	t.Cleanup(sd.Close)

	ibs := state.New(state.NewReaderV3(sd.AsStateGetter(tx, execctxapi.StateGetterOptions{})))
	defer ibs.Close()

	protocol.SetTxContext(ibs, engine, 35547779, 196)
	committed, err := ibs.GetCommittedState(contract, key)
	require.NoError(t, err)
	require.Equal(t, value.String(), committed.String())

	protocol.SetTxContext(ibs, engine, 35547779, 197)
	committed, err = ibs.GetCommittedState(contract, key)
	require.NoError(t, err)
	require.True(t, committed.IsZero())

	// An engine without baselines is left alone.
	protocol.SetTxContext(ibs, struct{ rules.EngineReader }{}, 35547779, 196)
	committed, err = ibs.GetCommittedState(contract, key)
	require.NoError(t, err)
	require.True(t, committed.IsZero())
}
