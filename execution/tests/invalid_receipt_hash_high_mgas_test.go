package executiontests

import (
	"path"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
)

func TestInvalidReceiptHashHighMgas(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlCrit)
	testDir := path.Join(cornersDir, "invalid-receipt-hash-high-mgas")
	preAllocsDir := path.Join(testDir, "pre_alloc")
	payloadsDir := path.Join(testDir, "payloads")
	engineXRunner, err := NewEngineXTestRunner(t, logger, preAllocsDir)
	require.NoError(t, err)
	tm := testMatcher{}
	tm.walk(t, payloadsDir, func(t *testing.T, name string, test EngineXTestDefinition) {
		err := engineXRunner.Run(ctx, test)
		require.NoError(t, err)
	})
}
