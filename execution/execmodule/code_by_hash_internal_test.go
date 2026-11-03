package execmodule

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCacheCodeByHashIsNilWithoutStateCache(t *testing.T) {
	require.True(t, (&Cache{}).CodeByHash() == nil)
	require.True(t, (&Cache{execModule: &ExecModule{}}).CodeByHash() == nil, "a nil *StateCache must not become a non-nil CodeByHash")
}
