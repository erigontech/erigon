package spectest

import (
	"io/fs"
	"path/filepath"
	"testing"

	"github.com/erigontech/erigon/cl/transition/machine"

	"github.com/stretchr/testify/require"
)

func RunCases(t *testing.T, app Appendix, machineImpl machine.Interface, root fs.FS) {
	cases, err := ReadTestCases(root)
	require.NoError(t, err, "reading cases")
	runLevel(t, cases, 0, func(t *testing.T, value TestCase) {
		if value.ForkPhaseName == "whisk" || value.ForkPhaseName == "eip7594" || value.ForkPhaseName == "heze" {
			t.Skipf("skipping %s", value.ForkPhaseName)
			return
		}
		t.Run(value.CaseName, func(t *testing.T) {
			require.NotPanics(t, func() {
				t.Parallel()
				runner, ok := app[value.RunnerName]
				if !ok {
					t.Skipf("runner not found: %s", value.RunnerName)
					return
				}
				handler, err := runner.GetHandler(value.HandlerName)
				if err != nil {
					t.Skipf("handler not found: %s/%s", value.RunnerName, value.HandlerName)
					return
				}
				path := value.path()
				subfs, err := fs.Sub(root, filepath.Join(path[:]...))
				value.Machine = machineImpl
				require.NoError(t, err)
				err = handler.Run(t, subfs, value)
				require.NoError(t, err)
			})
		})
	})
}

// cases must be grouped by path, as fs.WalkDir returns them.
func runLevel(t *testing.T, cases []TestCase, depth int, runCase func(*testing.T, TestCase)) {
	if depth == 5 {
		for _, c := range cases {
			runCase(t, c)
		}
		return
	}
	for len(cases) > 0 {
		name := cases[0].path()[depth]
		n := 1
		for n < len(cases) && cases[n].path()[depth] == name {
			n++
		}
		group := cases[:n]
		cases = cases[n:]
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			runLevel(t, group, depth+1, runCase)
		})
	}
}
