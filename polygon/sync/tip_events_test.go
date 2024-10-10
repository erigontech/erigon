package sync

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/polygon/p2p"
	"github.com/erigontech/erigon/turbo/testlog"
)

func TestBlockEventsSpamGuard(t *testing.T) {
	logger := testlog.Logger(t, log.LvlCrit)
	sg := newBlockEventsSpamGuard(logger)
	peerId1 := p2p.PeerIdFromUint64(1)
	peerId2 := p2p.PeerIdFromUint64(2)
	require.False(t, sg.Spam(peerId1, common.HexToHash("0x0"), 1))
	require.True(t, sg.Spam(peerId1, common.HexToHash("0x0"), 1))
	require.True(t, sg.Spam(peerId1, common.HexToHash("0x0"), 1))
	require.False(t, sg.Spam(peerId2, common.HexToHash("0x0"), 1))
	require.True(t, sg.Spam(peerId2, common.HexToHash("0x0"), 1))
	require.False(t, sg.Spam(peerId2, common.HexToHash("0x1"), 1))
	require.False(t, sg.Spam(peerId2, common.HexToHash("0x0"), 2))
	require.False(t, sg.Spam(peerId2, common.HexToHash("0x2"), 2))
	require.False(t, sg.Spam(peerId2, common.HexToHash("0x3"), 3))
}
