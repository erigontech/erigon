package spectest

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
)

func TestRunCasesFailsOnUnregisteredRunnerOrHandler(t *testing.T) {
	if os.Getenv("ERIGON_SPECTEST_UNREGISTERED_RUNNER_OR_HANDLER_CHILD") == "1" {
		fsys := fstest.MapFS{
			filepath.Join("mainnet", "gloas", "networking", "gossip_unregistered", "pyspec_tests", "case"): {},
			filepath.Join("mainnet", "gloas", "unregistered_runner", "handler", "pyspec_tests", "case"):    {},
		}
		RunCases(t, Appendix{"networking": NewFormat()}, nil, fsys)
		return
	}

	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestRunCasesFailsOnUnregisteredRunnerOrHandler$")
	cmd.Env = append(os.Environ(), "ERIGON_SPECTEST_UNREGISTERED_RUNNER_OR_HANDLER_CHILD=1")
	output, err := cmd.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "handler not found: networking/gossip_unregistered")
	require.Contains(t, string(output), "runner not found: unregistered_runner")
}
