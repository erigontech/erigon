//go:build linux

package state

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPBTRSSAnonLinuxValue(t *testing.T) {
	value, ok := pbtRSSAnon().(uint64)
	require.True(t, ok)
	require.Positive(t, value)
}
