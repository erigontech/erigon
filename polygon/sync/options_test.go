package sync_test

import (
	"testing"

	"github.com/erigontech/erigon/erigon-lib/log/v3"
	"github.com/erigontech/erigon/polygon/sync"
)

func TestEventChannelOptions(t *testing.T) {
	sync.NewEventChannel[sync.Event](10, sync.WithEventChannelLogging(log.New(), log.LvlTrace, sync.EventTopicHeimdall.String()))
}
