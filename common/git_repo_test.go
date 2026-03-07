package common

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNoLargeInRecentGitHistory ensures no large files do not get into git history
func TestNoLargeFilesInRecentGitHistory(t *testing.T) {
	const gitCommand = `git rev-list --objects --since="1 month ago" HEAD |
		git cat-file --batch-check="%(objecttype) %(objectsize) %(rest)" | grep blob |
		grep -v testdata | grep -v test_data | grep -v execution-spec-tests | grep -v engineapi-performance-tests | grep -v 'tests/files' |
		grep -v initial_state |
		grep -v 'docs/lmdb' | grep -v 'docs/gitbook' |
		grep -v 'cl/phase1/core/state/tests' |
		grep -v 'signer/fourbyte/4byte.json' |
		awk '$2 > 1*1024*1024 {printf "%s MB: %s\n", $2/(1*1024*1024), $3}'`
	cmd := exec.Command("bash", "-c", gitCommand)
	// Prevent git from lazy-fetching missing blobs in partial (blob:none) clones.
	// Without this, git cat-file tries to fetch every historical blob from the
	// remote, causing the test to hang. Blobs not present locally are reported
	// as "missing" and filtered out by grep; blobs from the current checkout ARE
	// present and will still be checked.
	cmd.Env = append(os.Environ(), "GIT_NO_LAZY_FETCH=1")
	output, err := cmd.Output()
	require.NoError(t, err)
	outStr := strings.TrimSpace(string(output))
	if outStr != "" {
		t.Errorf("Found large blobs in git history:\n%s", outStr)
	}

	//TODO:
	// - purge `popppp/`
	// - purge `Godeps/`
	// - purge `vendor`
	// - purge `deploy`
	// - purge `cmd/clef`
	// - purge `cmd/swarm`
	// - purge `coverage-test.out`
	// - purge `ethdb/mdbx/dist`
	// - purge `cmd/*/compiled binaries`
	// - to decide about `docs/lmdb` (this folder used by github's wiki)
}
