package heimdall

import (
	"testing"

	"github.com/ledgerwatch/erigon/polygon/heimdall/heimdalltest"
)

func TestMilestoneJsonMarshall(t *testing.T) {
	heimdalltest.AssertJsonMarshalUnmarshal(t, makeMilestone(10, 100))
}
