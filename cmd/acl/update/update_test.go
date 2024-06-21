package update

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/zk/txpool"
	"github.com/stretchr/testify/require"
)

// newTestState creates new instance of state used by tests.
func newTestACLDB(tb testing.TB) kv.RwDB {
	tb.Helper()

	dir := fmt.Sprintf("/tmp/acl-db-temp_%v", time.Now().UTC().Format(time.RFC3339Nano))
	err := os.Mkdir(dir, 0775)

	if err != nil {
		tb.Fatal(err)
	}

	state, err := txpool.OpenACLDB(context.Background(), dir)
	if err != nil {
		tb.Fatal(err)
	}

	tb.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			tb.Fatal(err)
		}
	})

	return state
}

func TestUpdatePolicies(t *testing.T) {
	// Define the test cases
	testCases := []struct {
		name        string
		csvFile     string
		aclType     string
		expectedErr string
	}{
		{
			name:        "Update allowlist",
			csvFile:     "tests/acls_update.csv",
			aclType:     "allowlist",
			expectedErr: "",
		},
		{
			name:        "Update blocklist",
			csvFile:     "tests/acls_update.csv",
			aclType:     "blocklist",
			expectedErr: "",
		},
		{
			name:        "Remove allowlist policy",
			csvFile:     "tests/acls_remove.csv",
			aclType:     "allowlist",
			expectedErr: "",
		},
		{
			name:        "Remove blocklist policy",
			csvFile:     "tests/acls_remove.csv",
			aclType:     "blocklist",
			expectedErr: "",
		},
		{
			name:        "Unknown policy",
			csvFile:     "tests/acls_invalid_policy.csv",
			aclType:     "allowlist",
			expectedErr: "unknown policy",
		},
		{
			name:        "Unknown policy",
			csvFile:     "tests/acls_invalid_policy.csv",
			aclType:     "blocklist",
			expectedErr: "unknown policy",
		},
	}

	// Run the test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// create acls db
			aclDB := newTestACLDB(t)
			defer aclDB.Close()

			// prepare some data first
			require.NoError(t, updatePolicies(context.Background(), "tests/acls_prepare.csv", tc.aclType, aclDB))

			// Update the policies based on the CSV file
			err := updatePolicies(context.Background(), tc.csvFile, tc.aclType, aclDB)
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
