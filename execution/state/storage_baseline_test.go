package state_test

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestStorageBaseline(t *testing.T) {
	t.Parallel()

	contract := accounts.InternAddress(common.HexToAddress("0x89791428868131eb109e42340ad01eb8987526b2"))
	key := accounts.InternKey(common.HexToHash("0xf1e9242398de526b8dd9c25d38e65fbb01926b8940377762d7884b8b0dcdc3b0"))
	baseline := uint256.MustFromHex("0xf6a7831804efd2cd0a")

	_, tx, sd := state.NewTestRwTx(t)
	ibs := state.New(state.NewReaderV3(sd.AsStateGetter(tx, execctxapi.StateGetterOptions{})))
	defer ibs.Close()

	ibs.SetTxContext(35547779, 196)
	committed, err := ibs.GetCommittedState(contract, key)
	require.NoError(t, err)
	require.True(t, committed.IsZero())

	ibs.SetStorageBaseline(contract, key, *baseline)

	value, err := ibs.GetState(contract, key)
	require.NoError(t, err)
	require.Equal(t, baseline.String(), value.String())
	committed, err = ibs.GetCommittedState(contract, key)
	require.NoError(t, err)
	require.Equal(t, baseline.String(), committed.String())

	// The transaction's own write wins over the baseline, while the baseline stays
	// the committed original that SSTORE prices against.
	written := new(uint256.Int).AddUint64(baseline, 1234)
	require.NoError(t, ibs.SetState(contract, key, *written))
	value, err = ibs.GetState(contract, key)
	require.NoError(t, err)
	require.Equal(t, written.String(), value.String())
	committed, err = ibs.GetCommittedState(contract, key)
	require.NoError(t, err)
	require.Equal(t, baseline.String(), committed.String())

	// A baseline belongs to one transaction only.
	ibs.SetTxContext(35547779, 197)
	committed, err = ibs.GetCommittedState(contract, key)
	require.NoError(t, err)
	require.True(t, committed.IsZero())
}
