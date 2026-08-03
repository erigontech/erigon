package network

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShouldBanIncompleteBlockResponse(t *testing.T) {
	require.False(t, shouldBanIncompleteBlockResponse("peer", 0, 0))
	require.True(t, shouldBanIncompleteBlockResponse("peer", 1, 0))
	require.False(t, shouldBanIncompleteBlockResponse("peer", 2, 1))
	require.False(t, shouldBanIncompleteBlockResponse("", 1, 0))
	require.False(t, shouldBanIncompleteBlockResponse("http-fallback", 1, 0))
}
