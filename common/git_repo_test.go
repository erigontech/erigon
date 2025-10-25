package common

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNoLargeBlobsInGitHistory ensures no large files do not get into git history
func TestNoLargeBlobsInGitHistory(t *testing.T) {
	const gitCommand = `git rev-list --objects --since="1 month ago" HEAD |
		git cat-file --batch-check="%(objecttype) %(objectsize) %(rest)" |
		grep blob |
		grep -v testdata | grep -v test_data | grep -v execution-spec-tests | grep -v 'tests/files' |
		grep -v initial_state |
		grep -v 'docs/lmdb' |
		grep -v 'cl/phase1/core/state/tests' |
		grep -v 'signer/fourbyte/4byte.json' |
		grep -v 'tests/files' |
		awk '$2 > 1*1024*1024 {printf "%s MB: %s\n", $2/(1*1024*1024), $3}'`
	cmd := exec.Command("bash", "-c", gitCommand)
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
	// - purge `cmd/*/compiled binaries`
	// - purge `cmd/*/compiled binaries`
	// - purge `cmd/*/compiled binaries`
	// - purge `coverage-test.out`
	// - purge `ethdb/mdbx/dist`
	// - to decide about `docs/lmdb` (this folder used by github's wiki)
}
