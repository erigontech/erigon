package heimdall

import (
	"testing"

	"github.com/ledgerwatch/erigon/polygon/heimdall/heimdalltest"
)

func TestCheckpointJsonMarshall(t *testing.T) {
	heimdalltest.AssertJsonMarshalUnmarshal(t, makeCheckpoint(10, 100))
}
