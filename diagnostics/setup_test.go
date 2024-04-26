package diagnostics_test

import (
	"testing"

	"github.com/ledgerwatch/erigon/diagnostics"
)

func TestSetupEndpoints(t *testing.T) {
	diagnostics.SetupLogsAccess(nil, nil)
	diagnostics.SetupDbAccess(nil, nil)
	diagnostics.SetupCmdLineAccess(nil)
	diagnostics.SetupFlagsAccess(nil, nil)
	diagnostics.SetupVersionAccess(nil)
	diagnostics.SetupBlockBodyDownload(nil)
	diagnostics.SetupHeaderDownloadStats(nil)
	diagnostics.SetupNodeInfoAccess(nil, nil)
	diagnostics.SetupPeersAccess(nil, nil, nil, nil)
	diagnostics.SetupBootnodesAccess(nil, nil)
	diagnostics.SetupStagesAccess(nil, nil)
	diagnostics.SetupMemAccess(nil)
	diagnostics.SetupHeadersAccess(nil, nil)
	diagnostics.SetupBodiesAccess(nil, nil)

}
