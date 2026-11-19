package blockreplay_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFixtureHasAuthoritativeOutputs pins that a captured sample carries the
// block's authoritative post-state (read from the canonical chain at capture
// time, not re-derived by any executor), including the fee-recipient credit.
// This is the reference a parallel replay's data is checked against.
func TestFixtureHasAuthoritativeOutputs(t *testing.T) {
	fx := loadFixture(t, "25604144")
	require.NotNil(t, fx.Outputs, "fixture must carry captured outputs")
	require.NotEmpty(t, fx.Outputs.Accounts, "block must write at least one account")

	block, err := fx.Block()
	require.NoError(t, err)
	coinbase := block.HeaderNoCopy().Coinbase
	_, ok := fx.Outputs.Accounts[[20]byte(coinbase)]
	require.True(t, ok, "fee recipient %x must appear in captured post-state", coinbase)

	// Diff is reflexive: a captured set compared against itself is clean.
	require.Empty(t, fx.Outputs.Diff(fx.Outputs))
}
