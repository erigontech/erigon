package txpool

import (
	"context"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/u256"
	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon-lib/kv/temporal/temporaltest"
	"github.com/ledgerwatch/erigon-lib/txpool"
	"github.com/ledgerwatch/erigon-lib/txpool/txpoolcfg"
	types "github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

var peerID types.PeerID = gointerfaces.ConvertHashToH512([64]byte{0x12, 0x34, 0x50}) // "12345"

// DecodeHex converts a hex string to a byte array.
func decodeHex(in string) []byte {
	payload, err := hex.DecodeString(in)
	if err != nil {
		panic(err)
	}
	return payload
}

func TestFetch(t *testing.T) {
	assert, require := assert.New(t), require.New(t)
	ch := make(chan types.Announcements, 100)
	_, coreDB, _ := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	defer coreDB.Close()
	db := memdb.NewTestPoolDB(t)
	path := fmt.Sprintf("/tmp/db-test-%v", time.Now().UTC().Format(time.RFC3339Nano))
	txPoolDB := newTestTxPoolDB(t, path)
	defer txPoolDB.Close()
	aclsDB := newTestACLDB(t, path)
	defer aclsDB.Close()

	// Check if the dbs are created.
	require.NotNil(t, db)
	require.NotNil(t, txPoolDB)
	require.NotNil(t, aclsDB)

	cfg := txpoolcfg.DefaultConfig
	ethCfg := &ethconfig.Defaults
	sendersCache := kvcache.New(kvcache.DefaultCoherentConfig)
	pool, err := New(ch, coreDB, cfg, ethCfg, sendersCache, *u256.N1, nil, nil, aclsDB)
	assert.NoError(err)
	require.True(pool != nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctrl := gomock.NewController(t)
	remoteKvClient := remote.NewMockKVClient(ctrl)
	sentryServer := sentry.NewMockSentryServer(ctrl)

	m := txpool.NewMockSentry(ctx, sentryServer)
	sentryClient := direct.NewSentryClientDirect(direct.ETH66, m)
	fetch := NewFetch(ctx, []direct.SentryClient{sentryClient}, pool, remoteKvClient, nil, nil, *u256.N1)
	var wg sync.WaitGroup
	fetch.SetWaitGroup(&wg)
	// The corresponding WaitGroup.Done() will be called by the Sentry.
	// First will be called by (txpool.MockSentry).Messages
	// Second will be called by (txpool.MockSentry).PeerEvents
	m.StreamWg.Add(2)
	fetch.ConnectSentries()
	m.StreamWg.Wait()

	// Send one transaction id with ETH66 protocol.
	// The corresponding WaitGroup.Done() will be called by the fetch.receiveMessage()
	wg.Add(1)
	errs := m.Send(&sentry.InboundMessage{
		Id:     sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_66,
		Data:   decodeHex("e1a0595e27a835cd79729ff1eeacec3120eeb6ed1464a04ec727aaca734ead961328"),
		PeerId: peerID,
	})
	for i, err := range errs {
		if err != nil {
			t.Errorf("sending new pool txn hashes 66 (%d): %v", i, err)
		}
	}
	wg.Wait()
}
