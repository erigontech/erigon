package execmodule_test

import (
	"github.com/holiman/uint256"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/types"
)

// ISOLATED (CL re-execution bug): ValidateChain (Caplin's newPayload path) must ACCEPT a block THIS node
// already sealed — recorded in sealedByHash by the marker-driven seal — by pure LOOKUP, returning Success +
// the sealed root WITHOUT re-executing. Re-executing on a fresh SD parented to lagging canonical state lacks
// the frontier predecessor's state/txNum and computes a wrong root or fails (nonce-too-low / can't-find-header),
// falsely invalidating our own valid block (the tip stall). Proof of "no re-exec": the sealed block is NEVER
// inserted, so any execution attempt would fail to find it; a Success return can ONLY come from accept-by-hash.
func TestValidateChain_AcceptsLocallySealedWithoutReExec(t *testing.T) {
	ctx := t.Context()
	m := execmoduletester.New(t, execmoduletester.WithChainConfig(chain.AllProtocolChanges))
	exec := m.ExecModule

	root := common.HexToHash("0x00000000000000000000000000000000000000000000000000000000c0ffee01")
	sealed := &types.Header{Number: *uint256.NewInt(2), Root: root}
	h := sealed.Hash()

	// Record it as if the driver's marker seal ran (ingestSealedFlashblockLocked) — but insert NO block.
	exec.RecordSealedForTest(sealed)

	res, err := exec.ValidateChain(ctx, h, 2)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, res.ValidationStatus, "must ACCEPT our own sealed block (no re-exec)")
	require.Equal(t, root, res.ComputedRoot, "must return the sealed root by lookup, not by re-execution")
	require.Equal(t, h, res.LatestValidHash)
}
