package debug

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// GOMEMLIMIT=off is a deliberate choice, but the runtime reports it exactly like
// an unset variable, so only the environment can tell the two apart.
func TestGoMemLimitInForce(t *testing.T) {
	t.Run("unset", func(t *testing.T) {
		require.False(t, goMemLimitInForce(math.MaxInt64))
	})
	t.Run("off", func(t *testing.T) {
		t.Setenv("GOMEMLIMIT", "off")
		require.True(t, goMemLimitInForce(math.MaxInt64))
	})
	t.Run("set in process", func(t *testing.T) {
		require.True(t, goMemLimitInForce(3<<30))
	})
}
