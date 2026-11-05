//go:build linux

package estimate

import (
	"testing"

	"github.com/containerd/cgroups/v3"
	"github.com/stretchr/testify/require"
)

// In hybrid mode the memory controller is mounted as cgroup v1.
func TestCgroupsMemoryLimitForMode(t *testing.T) {
	v1 := func() (uint64, error) { return 1, nil }
	v2 := func() (uint64, error) { return 2, nil }
	for _, tc := range []struct {
		name string
		mode cgroups.CGMode
		want uint64
	}{
		{"legacy", cgroups.Legacy, 1},
		{"hybrid", cgroups.Hybrid, 1},
		{"unified", cgroups.Unified, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := cgroupsMemoryLimitForMode(tc.mode, v1, v2)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
