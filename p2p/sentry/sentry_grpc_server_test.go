package sentry

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	grpcmetadata "google.golang.org/grpc/metadata"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/forkid"
	"github.com/erigontech/erigon/p2p/protocols/eth"
	"github.com/erigontech/erigon/p2p/protocols/wit"
)

// Handles RLP encoding/decoding for p2p.Msg
type MockMsgReadWriter struct {
	readBuf  *bytes.Buffer
	writeBuf *bytes.Buffer
	readMu   sync.Mutex
	writeMu  sync.Mutex
}

func NewMockMsgReadWriter() *MockMsgReadWriter {
	return &MockMsgReadWriter{
		readBuf:  bytes.NewBuffer(nil),
		writeBuf: bytes.NewBuffer(nil),
	}
}

func (m *MockMsgReadWriter) ReadMsg() (p2p.Msg, error) {
	m.readMu.Lock()
	defer m.readMu.Unlock()

	s := rlp.NewStream(m.readBuf, 0)

	kind, _, err := s.Kind()
	if err != nil {
		return p2p.Msg{}, fmt.Errorf("failed to read RLP kind: %w", err)
	}
	if kind != rlp.List {
		return p2p.Msg{}, fmt.Errorf("expected RLP list, got %s", kind)
	}
	_, err = s.List()
	if err != nil {
		return p2p.Msg{}, fmt.Errorf("failed to read RLP list: %w", err)
	}

	code, err := s.Uint64()
	if err != nil {
		return p2p.Msg{}, fmt.Errorf("failed to read message code: %w", err)
	}

	payloadBytes, err := s.Bytes()
	if err != nil {
		return p2p.Msg{}, fmt.Errorf("failed to read payload bytes: %w", err)
	}

	if err := s.ListEnd(); err != nil {
		return p2p.Msg{}, fmt.Errorf("failed to end RLP list: %w", err)
	}

	return p2p.Msg{
		Code:    code,
		Size:    uint32(len(payloadBytes)),
		Payload: io.NopCloser(bytes.NewReader(payloadBytes)),
	}, nil
}

func (m *MockMsgReadWriter) WriteMsg(msg p2p.Msg) error {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()

	// RLP encode the message code and payload as a list.
	var payloadBytes []byte
	if msg.Payload != nil {
		var err error
		payloadBytes, err = io.ReadAll(msg.Payload)
		if err != nil {
			return err
		}
	}

	buf := new(bytes.Buffer)
	err := rlp.Encode(buf, []any{msg.Code, payloadBytes}) // Encode as a list [code, payload]
	if err != nil {
		return fmt.Errorf("failed to RLP encode message: %w", err)
	}

	m.writeBuf.Write(buf.Bytes())
	return nil
}

func (m *MockMsgReadWriter) ReadAllWritten() []byte {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	b := m.writeBuf.Bytes()
	m.writeBuf.Reset()
	return b
}

func (m *MockMsgReadWriter) WriteToReadBuffer(data []byte) {
	m.readMu.Lock()
	defer m.readMu.Unlock()
	m.readBuf.Write(data)
}

// MockPeer implements p2p.Peer for testing purposes
type MockPeer struct {
	pubkey [64]byte
	name   string
}

func NewMockPeer(pubkey [64]byte, name string) *MockPeer {
	return &MockPeer{pubkey: pubkey, name: name}
}

func (m *MockPeer) Pubkey() [64]byte {
	return m.pubkey
}

func (m *MockPeer) Name() string {
	return m.name
}

func (m *MockPeer) Fullname() string {
	return fmt.Sprintf("%s/%x", m.name, m.pubkey[:4])
}

func (m *MockPeer) ID() enode.ID {
	var id enode.ID
	copy(id[:], m.pubkey[:])
	return id
}

func (m *MockPeer) Info() *p2p.PeerInfo {
	// Simplified mock, as NetworkInfo is not directly accessible or needed for this test
	return &p2p.PeerInfo{
		ID:   m.ID().String(),
		Name: m.Name(),
		// Removed Network field entirely to avoid p2p.NetworkInfo dependency
	}
}

func (m *MockPeer) Disconnect(reason *p2p.PeerError) {
	// No-op for mock
}

func createDummyStatusData(networkID uint64, bestHash common.Hash, totalDifficulty *big.Int, genesisHash common.Hash, minimumBlockHeight uint64, maxBlockHeight uint64) *sentryproto.StatusData {
	return &sentryproto.StatusData{
		NetworkId:       networkID,
		TotalDifficulty: gointerfaces.ConvertUint256IntToH256(uint256.MustFromBig(totalDifficulty)),
		BestHash:        gointerfaces.ConvertHashToH256(bestHash),
		ForkData: &sentryproto.Forks{
			Genesis:     gointerfaces.ConvertHashToH256(genesisHash),
			HeightForks: []uint64{},
			TimeForks:   []uint64{},
		},
		MaxBlockHeight:     maxBlockHeight,
		MaxBlockTime:       0,
		MinimumBlockHeight: minimumBlockHeight,
	}
}

func TestHandShake69_ETH69ToETH69(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	require := require.New(t)

	ctx := t.Context()

	// Sentry 1 (initiator)
	sentry1RW := NewMockMsgReadWriter()
	sentry1Status := createDummyStatusData(
		1, common.HexToHash("0x111"), big.NewInt(100), common.HexToHash("0xabc"), 1, 100,
	)

	// Sentry 2 (responder)
	sentry2RW := NewMockMsgReadWriter()
	sentry2Status := createDummyStatusData(
		1, common.HexToHash("0x222"), big.NewInt(100), common.HexToHash("0xabc"), 1, 100,
	)

	// Simulate Sentry 2 sending its status to Sentry 1
	sentry2EthStatus := &eth.StatusPacket69{
		ProtocolVersion: direct.ETH69,
		NetworkID:       sentry2Status.NetworkId,
		Genesis:         gointerfaces.ConvertH256ToHash(sentry2Status.ForkData.Genesis),
		ForkID:          forkid.NewIDFromForks(sentry2Status.ForkData.HeightForks, sentry2Status.ForkData.TimeForks, gointerfaces.ConvertH256ToHash(sentry2Status.ForkData.Genesis), sentry2Status.MaxBlockHeight, sentry2Status.MaxBlockTime),
		MinimumBlock:    sentry2Status.MinimumBlockHeight,
		LatestBlock:     sentry2Status.MaxBlockHeight,
		LatestBlockHash: gointerfaces.ConvertH256ToHash(sentry2Status.BestHash),
	}
	sentry2StatusBytes, err := rlp.EncodeToBytes(sentry2EthStatus)
	require.NoError(err)
	err = sentry2RW.WriteMsg(p2p.Msg{Code: eth.StatusMsg, Size: uint32(len(sentry2StatusBytes)), Payload: bytes.NewReader(sentry2StatusBytes)})
	require.NoError(err)
	sentry1RW.WriteToReadBuffer(sentry2RW.ReadAllWritten())

	// Simulate Sentry 1 sending its status to Sentry 2
	sentry1EthStatus := &eth.StatusPacket69{
		ProtocolVersion: direct.ETH69,
		NetworkID:       sentry1Status.NetworkId,
		Genesis:         gointerfaces.ConvertH256ToHash(sentry1Status.ForkData.Genesis),
		ForkID:          forkid.NewIDFromForks(sentry1Status.ForkData.HeightForks, sentry1Status.ForkData.TimeForks, gointerfaces.ConvertH256ToHash(sentry1Status.ForkData.Genesis), sentry1Status.MaxBlockHeight, sentry1Status.MaxBlockTime),
		MinimumBlock:    sentry1Status.MinimumBlockHeight,
		LatestBlock:     sentry1Status.MaxBlockHeight,
		LatestBlockHash: gointerfaces.ConvertH256ToHash(sentry1Status.BestHash),
	}
	sentry1StatusBytes, err := rlp.EncodeToBytes(sentry1EthStatus)
	require.NoError(err)
	err = sentry1RW.WriteMsg(p2p.Msg{Code: eth.StatusMsg, Size: uint32(len(sentry1StatusBytes)), Payload: bytes.NewReader(sentry1StatusBytes)})
	require.NoError(err)
	sentry2RW.WriteToReadBuffer(sentry1RW.ReadAllWritten())

	// Run ETH69 handshake for Sentry 1 in a goroutine
	var reply69_1 *eth.StatusPacket69
	var peerErr1 *p2p.PeerError
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		reply69_1, peerErr1 = handShake[eth.StatusPacket69](ctx, sentry1Status, sentry1RW, direct.ETH69, direct.ETH69, encodeStatusPacket69, compatStatusPacket69, handshakeTimeout)
	}()

	// Run ETH69 handshake for Sentry 2 in a goroutine
	var reply69_2 *eth.StatusPacket69
	var peerErr2 *p2p.PeerError
	wg.Add(1)
	go func() {
		defer wg.Done()
		reply69_2, peerErr2 = handShake[eth.StatusPacket69](ctx, sentry2Status, sentry2RW, direct.ETH69, direct.ETH69, encodeStatusPacket69, compatStatusPacket69, handshakeTimeout)
	}()

	wg.Wait()

	assert.Nil(peerErr1)
	if assert.NotNil(reply69_1) {
		assert.Equal(sentry2Status.BestHash, gointerfaces.ConvertHashToH256(reply69_1.LatestBlockHash))
	}

	assert.Nil(peerErr2)
	if assert.NotNil(reply69_2) {
		assert.Equal(sentry1Status.BestHash, gointerfaces.ConvertHashToH256(reply69_2.LatestBlockHash))
	}

	// Verify that Sentry 1 sent its status
	sentBytes1 := sentry1RW.ReadAllWritten()
	assert.NotEmpty(sentBytes1)
	// Verify that Sentry 2 sent its status
	sentBytes2 := sentry2RW.ReadAllWritten()
	assert.NotEmpty(sentBytes2)
}

func TestHandShake69_ETH69ToETH68(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	require := require.New(t)

	ctx := t.Context()

	// Sentry 1 (ETH69 initiator)
	sentry1RW := NewMockMsgReadWriter()
	sentry1Status := createDummyStatusData(
		1, common.HexToHash("0x111"), big.NewInt(100), common.HexToHash("0xabc"), 1, 100,
	)

	// Sentry 2 (ETH68 responder)
	sentry2RW := NewMockMsgReadWriter()
	sentry2Status := createDummyStatusData(
		1, common.HexToHash("0x222"), big.NewInt(90), common.HexToHash("0xabc"), 0, 90,
	)

	// Simulate Sentry 2 (ETH68) sending its status to Sentry 1 (ETH69)
	sentry2EthStatus := &eth.StatusPacket{
		ProtocolVersion: direct.ETH68,
		NetworkID:       sentry2Status.NetworkId,
		TD:              gointerfaces.ConvertH256ToUint256Int(sentry2Status.TotalDifficulty).ToBig(),
		Head:            gointerfaces.ConvertH256ToHash(sentry2Status.BestHash),
		Genesis:         gointerfaces.ConvertH256ToHash(sentry2Status.ForkData.Genesis),
		ForkID:          forkid.NewIDFromForks(sentry2Status.ForkData.HeightForks, sentry2Status.ForkData.TimeForks, gointerfaces.ConvertH256ToHash(sentry2Status.ForkData.Genesis), sentry2Status.MaxBlockHeight, sentry2Status.MaxBlockTime),
	}
	sentry2StatusBytes, err := rlp.EncodeToBytes(sentry2EthStatus)
	require.NoError(err)
	err = sentry2RW.WriteMsg(p2p.Msg{Code: eth.StatusMsg, Size: uint32(len(sentry2StatusBytes)), Payload: bytes.NewReader(sentry2StatusBytes)})
	require.NoError(err)
	sentry1RW.WriteToReadBuffer(sentry2RW.ReadAllWritten())

	// Simulate Sentry 1 (ETH69) sending its status to Sentry 2 (ETH68)
	sentry1EthStatus := &eth.StatusPacket69{
		ProtocolVersion: direct.ETH69,
		NetworkID:       sentry1Status.NetworkId,
		Genesis:         gointerfaces.ConvertH256ToHash(sentry1Status.ForkData.Genesis),
		ForkID:          forkid.NewIDFromForks(sentry1Status.ForkData.HeightForks, sentry1Status.ForkData.TimeForks, gointerfaces.ConvertH256ToHash(sentry1Status.ForkData.Genesis), sentry1Status.MaxBlockHeight, sentry1Status.MaxBlockTime),
		MinimumBlock:    sentry1Status.MinimumBlockHeight,
		LatestBlock:     sentry1Status.MaxBlockHeight,
		LatestBlockHash: gointerfaces.ConvertH256ToHash(sentry1Status.BestHash),
	}
	sentry1StatusBytes, err := rlp.EncodeToBytes(sentry1EthStatus)
	require.NoError(err)
	err = sentry1RW.WriteMsg(p2p.Msg{Code: eth.StatusMsg, Size: uint32(len(sentry1StatusBytes)), Payload: bytes.NewReader(sentry1StatusBytes)})
	require.NoError(err)
	sentry2RW.WriteToReadBuffer(sentry1RW.ReadAllWritten())

	// Run ETH69/ETH68 handshakes on both sides
	wg := sync.WaitGroup{}
	wg.Add(2)
	var peerErr1 *p2p.PeerError
	var peerErr2 *p2p.PeerError

	go func() {
		defer wg.Done()
		_, peerErr1 = handShake[eth.StatusPacket69](ctx, sentry1Status, sentry1RW, direct.ETH69, direct.ETH68, encodeStatusPacket69, compatStatusPacket69, handshakeTimeout)
	}()

	go func() {
		defer wg.Done()
		_, peerErr2 = handShake[eth.StatusPacket](ctx, sentry2Status, sentry2RW, direct.ETH68, direct.ETH68, encodeStatusPacket, compatStatusPacket, handshakeTimeout)
	}()

	wg.Wait()

	// fails because it expects ETH69 status but receives ETH68
	assert.NotNil(peerErr1)
	assert.NotNil(peerErr2)
}

// RLPReadWriter is a more robust mock for p2p.MsgReadWriter that uses channels for communication.
type RLPReadWriter struct {
	readCh  chan p2p.Msg
	writeCh chan p2p.Msg
	quit    chan struct{}
	// Added to collect written messages for assertion
	writtenMessagesMu sync.Mutex
	writtenMessages   []byte
}

func NewRLPReadWriter() *RLPReadWriter {
	return &RLPReadWriter{
		readCh:          make(chan p2p.Msg, 10),
		writeCh:         make(chan p2p.Msg, 10),
		quit:            make(chan struct{}),
		writtenMessages: make([]byte, 0),
	}
}

func (rw *RLPReadWriter) ReadMsg() (p2p.Msg, error) {
	select {
	case msg := <-rw.readCh:
		return msg, nil
	case <-rw.quit:
		return p2p.Msg{}, io.EOF
	}
}

func (rw *RLPReadWriter) WriteMsg(msg p2p.Msg) error {
	select {
	case rw.writeCh <- msg:
		// Store the written message for later assertion
		rw.writtenMessagesMu.Lock()
		defer rw.writtenMessagesMu.Unlock()
		// RLP encode the message code and payload for storage
		buf := new(bytes.Buffer)
		err := rlp.Encode(buf, []any{msg.Code, msg.Payload})
		if err != nil {
			return fmt.Errorf("failed to RLP encode message for storage: %w", err)
		}
		rw.writtenMessages = append(rw.writtenMessages, buf.Bytes()...)
		return nil
	case <-rw.quit:
		return io.EOF
	}
}

func (rw *RLPReadWriter) Close() {
	close(rw.quit)
}

// ReadAllWritten collects all messages written to this RLPReadWriter.
func (rw *RLPReadWriter) ReadAllWritten() []byte {
	rw.writtenMessagesMu.Lock()
	defer rw.writtenMessagesMu.Unlock()
	b := rw.writtenMessages
	rw.writtenMessages = nil // Clear after reading
	return b
}

func TestHandShake69_ETH69ToETH69_WithRLP(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	ctx := t.Context()

	// Sentry 1 (initiator)
	sentry1RW := NewRLPReadWriter()
	sentry1Status := createDummyStatusData(
		1, common.HexToHash("0x111"), big.NewInt(100), common.HexToHash("0xabc"), 1, 100,
	)

	// Sentry 2 (responder)
	sentry2RW := NewRLPReadWriter()
	sentry2Status := createDummyStatusData(
		1, common.HexToHash("0x222"), big.NewInt(100), common.HexToHash("0xabc"), 1, 100,
	)

	// Simulate the connection: Sentry1 writes to Sentry2's read channel, and vice versa
	var wg sync.WaitGroup
	wg.Add(2)

	var reply1 *eth.StatusPacket69
	var peerErr1 *p2p.PeerError
	go func() {
		defer wg.Done()
		reply1, peerErr1 = handShake[eth.StatusPacket69](ctx, sentry1Status, sentry1RW, direct.ETH69, direct.ETH69, encodeStatusPacket69, compatStatusPacket69, handshakeTimeout)
	}()

	var reply2 *eth.StatusPacket69
	var peerErr2 *p2p.PeerError
	go func() {
		defer wg.Done()
		reply2, peerErr2 = handShake[eth.StatusPacket69](ctx, sentry2Status, sentry2RW, direct.ETH69, direct.ETH69, encodeStatusPacket69, compatStatusPacket69, handshakeTimeout)
	}()

	// Exchange messages between the two RLPReadWriters
	// This simulates the underlying network communication
	go func() {
		for {
			select {
			case msg := <-sentry1RW.writeCh:
				sentry2RW.readCh <- msg
			case msg := <-sentry2RW.writeCh:
				sentry1RW.readCh <- msg
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Wait()

	assert.Nil(peerErr1)
	if assert.NotNil(reply1) {
		assert.Equal(sentry2Status.BestHash, gointerfaces.ConvertHashToH256(reply1.LatestBlockHash))
	}

	assert.Nil(peerErr2)
	if assert.NotNil(reply2) {
		assert.Equal(sentry1Status.BestHash, gointerfaces.ConvertHashToH256(reply2.LatestBlockHash))
	}

	// Verify that Sentry 1 sent its status
	sentBytes1 := sentry1RW.ReadAllWritten()
	assert.NotEmpty(sentBytes1)
	// Verify that Sentry 2 sent its status
	sentBytes2 := sentry2RW.ReadAllWritten()
	assert.NotEmpty(sentBytes2)
}

func TestHandShake_ETH69ToETH68_WithRLP(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	ctx := t.Context()

	// Sentry 1 (ETH69 initiator)
	sentry1RW := NewRLPReadWriter()
	sentry1Status := createDummyStatusData(
		1, common.HexToHash("0x111"), big.NewInt(100), common.HexToHash("0xabc"), 1, 100,
	)

	// Sentry 2 (ETH68 responder)
	sentry2RW := NewRLPReadWriter()
	sentry2Status := createDummyStatusData(
		1, common.HexToHash("0x222"), big.NewInt(90), common.HexToHash("0xabc"), 0, 90,
	)

	var wg sync.WaitGroup
	wg.Add(2)
	var peerErr1 *p2p.PeerError
	var peerErr2 *p2p.PeerError

	go func() {
		defer wg.Done()
		_, peerErr1 = handShake[eth.StatusPacket69](ctx, sentry1Status, sentry1RW, direct.ETH69, direct.ETH68, encodeStatusPacket69, compatStatusPacket69, handshakeTimeout)
	}()

	go func() {
		defer wg.Done()
		_, peerErr2 = handShake[eth.StatusPacket](ctx, sentry2Status, sentry2RW, direct.ETH68, direct.ETH68, encodeStatusPacket, compatStatusPacket, handshakeTimeout)
	}()

	// Exchange messages between the two RLPReadWriters
	go func() {
		for {
			select {
			case msg := <-sentry1RW.writeCh:
				sentry2RW.readCh <- msg
			case msg := <-sentry2RW.writeCh:
				sentry1RW.readCh <- msg
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Wait()

	assert.NotNil(peerErr1)
	assert.NotNil(peerErr2)
}

func testSentryServer(db kv.Getter, genesis *types.Genesis, genesisHash common.Hash) *GrpcServer {
	s := &GrpcServer{
		ctx: context.Background(),
	}

	head := rawdb.ReadCurrentHeader(db)
	headTd, err := rawdb.ReadTd(db, head.Hash(), head.Number.Uint64())
	if err != nil {
		panic(err)
	}
	if headTd == nil {
		headTd = new(uint256.Int)
	}

	heightForks, timeForks := forkid.GatherForks(genesis.Config, genesis.Timestamp)
	s.statusData = &sentryproto.StatusData{
		NetworkId:       1,
		TotalDifficulty: gointerfaces.ConvertUint256IntToH256(headTd),
		BestHash:        gointerfaces.ConvertHashToH256(head.Hash()),
		MaxBlockHeight:  head.Number.Uint64(),
		MaxBlockTime:    head.Time,
		ForkData: &sentryproto.Forks{
			Genesis:     gointerfaces.ConvertHashToH256(genesisHash),
			HeightForks: heightForks,
			TimeForks:   timeForks,
		},
	}
	return s

}

func startHandshake(
	ctx context.Context,
	status *sentryproto.StatusData,
	pipe *p2p.MsgPipeRW,
	protocolVersion uint,
	errChan chan *p2p.PeerError,
) {
	go func() {
		_, err := handShake[eth.StatusPacket](ctx, status, pipe, protocolVersion, protocolVersion, encodeStatusPacket, compatStatusPacket, handshakeTimeout)
		errChan <- err
	}()
}

// Tests that peers are correctly accepted (or rejected) based on the advertised
// fork IDs in the protocol handshake.
func TestForkIDSplit68(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	testForkIDSplit(t, direct.ETH68)
}

func testForkIDSplit(t *testing.T, protocol uint) {
	var (
		ctx           = context.Background()
		configNoFork  = &chain.Config{HomesteadBlock: common.NewUint64(1), ChainID: uint256.NewInt(1)}
		configProFork = &chain.Config{
			ChainID:               uint256.NewInt(1),
			HomesteadBlock:        common.NewUint64(1),
			TangerineWhistleBlock: common.NewUint64(2),
			SpuriousDragonBlock:   common.NewUint64(2),
			ByzantiumBlock:        common.NewUint64(3),
		}
		dbNoFork  = temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
		dbProFork = temporaltest.NewTestDB(t, datadir.New(t.TempDir()))

		gspecNoFork  = &types.Genesis{Config: configNoFork}
		gspecProFork = &types.Genesis{Config: configProFork}

		genesisNoFork  = genesiswrite.MustCommitGenesis(gspecNoFork, dbNoFork, datadir.New(t.TempDir()), log.Root())
		genesisProFork = genesiswrite.MustCommitGenesis(gspecProFork, dbProFork, datadir.New(t.TempDir()), log.Root())
	)

	var s1, s2 *GrpcServer

	err := dbNoFork.Update(context.Background(), func(tx kv.RwTx) error {
		s1 = testSentryServer(tx, gspecNoFork, genesisNoFork.Hash())
		return nil
	})
	require.NoError(t, err)
	err = dbProFork.Update(context.Background(), func(tx kv.RwTx) error {
		s2 = testSentryServer(tx, gspecProFork, genesisProFork.Hash())
		return nil
	})
	require.NoError(t, err)

	// Both nodes should allow the other to connect (same genesis, next fork is the same)
	p2pNoFork, p2pProFork := p2p.MsgPipe()
	defer p2pNoFork.Close()
	defer p2pProFork.Close()

	errc := make(chan *p2p.PeerError, 2)
	startHandshake(ctx, s1.GetStatus(), p2pNoFork, protocol, errc)
	startHandshake(ctx, s2.GetStatus(), p2pProFork, protocol, errc)

	for i := 0; i < 2; i++ {
		select {
		case err := <-errc:
			if err != nil {
				t.Fatalf("frontier nofork <-> profork failed: %v", err)
			}
		case <-time.After(250 * time.Millisecond):
			t.Fatalf("frontier nofork <-> profork handler timeout")
		}
	}

	// Progress into Homestead. Fork's match, so we don't care what the future holds
	s1.statusData.MaxBlockHeight = 1
	s2.statusData.MaxBlockHeight = 1

	startHandshake(ctx, s1.GetStatus(), p2pNoFork, protocol, errc)
	startHandshake(ctx, s2.GetStatus(), p2pProFork, protocol, errc)

	for i := 0; i < 2; i++ {
		select {
		case err := <-errc:
			if err != nil {
				t.Fatalf("homestead nofork <-> profork failed: %v", err)
			}
		case <-time.After(250 * time.Millisecond):
			t.Fatalf("frontier nofork <-> profork handler timeout")
		}
	}

	// Progress into Spurious. Forks mismatch, signalling differing chains, reject
	s1.statusData.MaxBlockHeight = 2
	s2.statusData.MaxBlockHeight = 2

	// Both nodes should allow the other to connect (same genesis, next fork is the same)
	startHandshake(ctx, s1.GetStatus(), p2pNoFork, protocol, errc)
	startHandshake(ctx, s2.GetStatus(), p2pProFork, protocol, errc)

	var successes int
	for i := 0; i < 2; i++ {
		select {
		case err := <-errc:
			if err == nil {
				successes++
				if successes == 2 { // Only one side disconnects
					t.Fatalf("fork ID rejection didn't happen")
				}
			}
		case <-time.After(250 * time.Millisecond):
			t.Fatalf("split peers not rejected")
		}
	}
}

func TestSentryServerImpl_SetStatusInitPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic during server initialization")
		}
	}()

	configNoFork := &chain.Config{HomesteadBlock: common.NewUint64(1), ChainID: uint256.NewInt(1)}
	dbNoFork := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	gspecNoFork := &types.Genesis{Config: configNoFork}
	genesisNoFork := genesiswrite.MustCommitGenesis(gspecNoFork, dbNoFork, datadir.New(t.TempDir()), log.Root())
	ss := &GrpcServer{p2p: &p2p.Config{}}

	_, err := ss.SetStatus(context.Background(), &sentryproto.StatusData{
		ForkData: &sentryproto.Forks{Genesis: gointerfaces.ConvertHashToH256(genesisNoFork.Hash())},
	})
	if err == nil {
		t.Fatalf("error expected")
	}

	// Should not panic here.
	_, err = ss.SetStatus(context.Background(), &sentryproto.StatusData{
		ForkData: &sentryproto.Forks{Genesis: gointerfaces.ConvertHashToH256(genesisNoFork.Hash())},
	})
	if err == nil {
		t.Fatalf("error expected")
	}
}

// newTestPeerInfoWithEth creates a PeerInfo backed by a *p2p.Peer that has
// the eth protocol in its running map (so WaitForEth won't fail) and marks
// the eth handshake as already completed.
func newTestPeerInfoWithEth(t *testing.T) (*PeerInfo, [64]byte) {
	t.Helper()
	var pubkey [64]byte
	pubkey[0] = 0x01
	id := enode.ID{}
	copy(id[:], pubkey[:])

	caps := []p2p.Cap{
		{Name: eth.ProtocolName, Version: direct.ETH68},
	}
	protocols := []p2p.Protocol{
		{Name: eth.ProtocolName, Version: direct.ETH68, Length: eth.ProtocolLengths[direct.ETH68]},
	}
	peer := p2p.NewPeerWithProtocols(id, pubkey, "test-peer", caps, protocols, false)

	rw := NewRLPReadWriter()
	t.Cleanup(rw.Close)

	pi := NewPeerInfo(peer)
	pi.SetEthRw(rw)
	// Mark eth handshake as done so WaitForEth returns immediately.
	pi.SetEthProtocol(direct.ETH68)

	return pi, pubkey
}

// TestRunWitPeer_MalformedNewWitnessMsg verifies that a malformed
// NewWitnessMsg causes a PeerError (peer disconnect) rather than a
// nil-pointer panic. This is the regression test for the DoS
// vulnerability where a missing 'continue' after RLP decode failure
// led to query.Witness.Header().Hash() panicking on a nil Witness.
func TestRunWitPeer_MalformedNewWitnessMsg(t *testing.T) {
	t.Parallel()

	peerInfo, peerID := newTestPeerInfoWithEth(t)

	rw := NewRLPReadWriter()
	t.Cleanup(rw.Close)

	logger := log.Root()

	send := func(msgId sentryproto.MessageId, peerID [64]byte, b []byte) {}
	hasSubscribers := func(msgId sentryproto.MessageId) bool { return true }
	getWitnessRequest := func(hash common.Hash, peerID [64]byte) bool { return false }

	// Feed a NewWitnessMsg with garbage RLP payload.
	garbage := []byte{0xff, 0xfe, 0xfd}
	rw.readCh <- p2p.Msg{
		Code:    wit.NewWitnessMsg,
		Size:    uint32(len(garbage)),
		Payload: io.NopCloser(bytes.NewReader(garbage)),
	}

	errCh := make(chan *p2p.PeerError, 1)
	go func() {
		errCh <- runWitPeer(t.Context(), peerID, rw, peerInfo, send, hasSubscribers, getWitnessRequest, logger)
	}()

	select {
	case peerErr := <-errCh:
		require.NotNil(t, peerErr, "expected a PeerError for malformed message")
		assert.Equal(t, p2p.PeerErrorInvalidMessage, peerErr.Code)
	case <-time.After(5 * time.Second):
		t.Fatal("runWitPeer did not return within timeout")
	}
}

// TestRunWitPeer_MalformedNewWitnessHashesMsg verifies the same
// protection for NewWitnessHashesMsg.
func TestRunWitPeer_MalformedNewWitnessHashesMsg(t *testing.T) {
	t.Parallel()

	peerInfo, peerID := newTestPeerInfoWithEth(t)

	rw := NewRLPReadWriter()
	t.Cleanup(rw.Close)

	logger := log.Root()

	send := func(msgId sentryproto.MessageId, peerID [64]byte, b []byte) {}
	hasSubscribers := func(msgId sentryproto.MessageId) bool { return true }
	getWitnessRequest := func(hash common.Hash, peerID [64]byte) bool { return false }

	// Feed a NewWitnessHashesMsg with garbage RLP payload.
	garbage := []byte{0xff, 0xfe, 0xfd}
	rw.readCh <- p2p.Msg{
		Code:    wit.NewWitnessHashesMsg,
		Size:    uint32(len(garbage)),
		Payload: io.NopCloser(bytes.NewReader(garbage)),
	}

	errCh := make(chan *p2p.PeerError, 1)
	go func() {
		errCh <- runWitPeer(t.Context(), peerID, rw, peerInfo, send, hasSubscribers, getWitnessRequest, logger)
	}()

	select {
	case peerErr := <-errCh:
		require.NotNil(t, peerErr, "expected a PeerError for malformed message")
		assert.Equal(t, p2p.PeerErrorInvalidMessage, peerErr.Code)
	case <-time.After(5 * time.Second):
		t.Fatal("runWitPeer did not return within timeout")
	}
}

func freshNewBlockHashesMsg(t *testing.T, entries int) p2p.Msg {
	t.Helper()
	pkt := make(eth.NewBlockHashesPacket, entries)
	for i := range pkt {
		pkt[i].Number = uint64(i)
		pkt[i].Hash[0] = byte(i)
	}
	b, err := rlp.EncodeToBytes(pkt)
	require.NoError(t, err)
	return p2p.Msg{
		Code:    eth.NewBlockHashesMsg,
		Size:    uint32(len(b)),
		Payload: io.NopCloser(bytes.NewReader(b)),
	}
}

// TestRunPeer_OversizedNewBlockHashesKicksPeer verifies the sentry framing
// layer drops an oversized NewBlockHashes packet and disconnects the peer
// before the payload is forwarded to any subscriber.
func TestRunPeer_OversizedNewBlockHashesKicksPeer(t *testing.T) {
	t.Parallel()

	peerInfo, peerID := newTestPeerInfoWithEth(t)
	rw := NewRLPReadWriter()
	t.Cleanup(rw.Close)
	logger := log.Root()

	sendCh := make(chan struct{}, 1)
	send := func(msgId sentryproto.MessageId, peerID [64]byte, b []byte) { sendCh <- struct{}{} }
	hasSubscribers := func(msgId sentryproto.MessageId) bool { return true }

	oversize := make([]byte, maxNewBlockHashesBytes+1)
	rw.readCh <- p2p.Msg{
		Code:    eth.NewBlockHashesMsg,
		Size:    uint32(len(oversize)),
		Payload: io.NopCloser(bytes.NewReader(oversize)),
	}

	peerCap := p2p.Cap{Name: eth.ProtocolName, Version: direct.ETH68}
	errCh := make(chan *p2p.PeerError, 1)
	go func() {
		errCh <- runPeer(t.Context(), peerID, peerCap, rw, peerInfo, send, hasSubscribers, logger)
	}()

	select {
	case peerErr := <-errCh:
		require.NotNil(t, peerErr, "expected a PeerError for oversized NewBlockHashes")
		assert.Equal(t, p2p.PeerErrorMessageSizeLimit, peerErr.Code)
	case <-time.After(5 * time.Second):
		t.Fatal("runPeer did not return within timeout")
	}

	select {
	case <-sendCh:
		t.Fatal("oversized NewBlockHashes must not be forwarded to subscribers")
	default:
	}
}

// TestRunPeer_TooManyNewBlockHashesEntriesKicksPeer verifies a packet that
// stays under the byte cap but carries more than maxBlockHashesPerMsg entries
// is rejected and the peer disconnected.
func TestRunPeer_TooManyNewBlockHashesEntriesKicksPeer(t *testing.T) {
	t.Parallel()

	peerInfo, peerID := newTestPeerInfoWithEth(t)
	rw := NewRLPReadWriter()
	t.Cleanup(rw.Close)
	logger := log.Root()

	sendCh := make(chan struct{}, 1)
	send := func(msgId sentryproto.MessageId, peerID [64]byte, b []byte) { sendCh <- struct{}{} }
	hasSubscribers := func(msgId sentryproto.MessageId) bool { return true }

	msg := freshNewBlockHashesMsg(t, maxBlockHashesPerMsg+1)
	require.LessOrEqual(t, int(msg.Size), maxNewBlockHashesBytes, "packet must pass the byte cap to exercise the entry cap")
	rw.readCh <- msg

	peerCap := p2p.Cap{Name: eth.ProtocolName, Version: direct.ETH68}
	errCh := make(chan *p2p.PeerError, 1)
	go func() {
		errCh <- runPeer(t.Context(), peerID, peerCap, rw, peerInfo, send, hasSubscribers, logger)
	}()

	select {
	case peerErr := <-errCh:
		require.NotNil(t, peerErr, "expected a PeerError for too many NewBlockHashes entries")
		assert.Equal(t, p2p.PeerErrorMessageSizeLimit, peerErr.Code)
	case <-time.After(5 * time.Second):
		t.Fatal("runPeer did not return within timeout")
	}

	select {
	case <-sendCh:
		t.Fatal("over-cap NewBlockHashes must not be forwarded to subscribers")
	default:
	}
}

// TestRunPeer_NewBlockHashesFloodKicksPeer verifies a peer flooding compliant
// NewBlockHashes packets past the per-peer rate limit is disconnected.
func TestRunPeer_NewBlockHashesFloodKicksPeer(t *testing.T) {
	t.Parallel()

	peerInfo, peerID := newTestPeerInfoWithEth(t)
	rw := NewRLPReadWriter()
	t.Cleanup(rw.Close)
	logger := log.Root()

	send := func(msgId sentryproto.MessageId, peerID [64]byte, b []byte) {}
	hasSubscribers := func(msgId sentryproto.MessageId) bool { return true }

	peerCap := p2p.Cap{Name: eth.ProtocolName, Version: direct.ETH68}
	errCh := make(chan *p2p.PeerError, 1)
	go func() {
		errCh <- runPeer(t.Context(), peerID, peerCap, rw, peerInfo, send, hasSubscribers, logger)
	}()

	assertKicked := func(peerErr *p2p.PeerError) {
		require.NotNil(t, peerErr, "expected a PeerError for NewBlockHashes flood")
		assert.Equal(t, p2p.PeerErrorInvalidMessage, peerErr.Code)
	}

	for i := 0; i < newBlockHashesBurst+10; i++ {
		select {
		case rw.readCh <- freshNewBlockHashesMsg(t, 1):
		case peerErr := <-errCh:
			assertKicked(peerErr)
			return
		case <-time.After(5 * time.Second):
			t.Fatal("timed out feeding NewBlockHashes packets")
		}
	}

	select {
	case peerErr := <-errCh:
		assertKicked(peerErr)
	case <-time.After(5 * time.Second):
		t.Fatal("runPeer did not kick flooding peer within timeout")
	}
}

// TestRunPeer_NormalNewBlockHashesForwarded verifies compliant NewBlockHashes
// traffic is forwarded to subscribers and not penalized.
func TestRunPeer_NormalNewBlockHashesForwarded(t *testing.T) {
	t.Parallel()

	peerInfo, peerID := newTestPeerInfoWithEth(t)
	rw := NewRLPReadWriter()
	t.Cleanup(rw.Close)
	logger := log.Root()

	const want = 5
	sent := make(chan struct{}, want)
	send := func(msgId sentryproto.MessageId, peerID [64]byte, b []byte) { sent <- struct{}{} }
	hasSubscribers := func(msgId sentryproto.MessageId) bool { return true }

	peerCap := p2p.Cap{Name: eth.ProtocolName, Version: direct.ETH68}
	errCh := make(chan *p2p.PeerError, 1)
	go func() {
		errCh <- runPeer(t.Context(), peerID, peerCap, rw, peerInfo, send, hasSubscribers, logger)
	}()

	for i := 0; i < want; i++ {
		rw.readCh <- freshNewBlockHashesMsg(t, 1)
	}

	for i := 0; i < want; i++ {
		select {
		case <-sent:
		case peerErr := <-errCh:
			t.Fatalf("runPeer kicked peer for compliant NewBlockHashes traffic: %v", peerErr)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for forwarded NewBlockHashes")
		}
	}
}

// minimalP2PServer returns an un-networked p2p.Server (no discovery, no
// dial, no listener) suitable for tests that only need a non-nil Server
// to inject into a GrpcServer.
func minimalP2PServer(t *testing.T) *p2p.Server {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	return &p2p.Server{Config: p2p.Config{
		PrivateKey:      key,
		MaxPeers:        1,
		MaxPendingPeers: 1,
		NoDiscovery:     true,
		NoDial:          true,
	}}
}

// minimalP2PServerWithListener returns a p2p.Server bound to an ephemeral
// loopback port. The bound address lets lifecycle tests observe Stop by
// dialling the port: connect succeeds while the listener is up and is
// refused once the Server has shut it down. Avoids relying on Start-after-
// Stop, which p2p.Server documents as unsupported.
func minimalP2PServerWithListener(t *testing.T) *p2p.Server {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	return &p2p.Server{Config: p2p.Config{
		PrivateKey:      key,
		MaxPeers:        1,
		MaxPendingPeers: 1,
		NoDiscovery:     true,
		NoDial:          true,
		ListenAddr:      "127.0.0.1:0",
	}}
}

// listenerReachable returns true if a TCP dial to the given peer-server's
// listener succeeds within 200ms. Used by lifecycle tests to verify Stop
// without depending on Start-after-Stop.
func listenerReachable(t *testing.T, srv *p2p.Server) bool {
	t.Helper()
	addr := srv.NodeInfo().ListenAddr
	c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
	if err != nil {
		return false
	}
	c.Close()
	return true
}

// TestGrpcServer_SetP2PServer_FlagsAndIdempotency verifies the SetP2PServer
// hook flips the external flag correctly and refuses a second injection.
func TestGrpcServer_SetP2PServer_FlagsAndIdempotency(t *testing.T) {
	srv := minimalP2PServer(t)
	ss := &GrpcServer{statusReady: make(chan struct{})}

	require.NoError(t, ss.SetP2PServer(srv))
	require.True(t, ss.external)
	require.Same(t, srv, ss.getP2PServer())

	// A second SetP2PServer must error — ownership is decided up front.
	require.Error(t, ss.SetP2PServer(srv))
}

// TestGrpcServer_PeersFiltersByProtocol covers the per-sentry filter on
// Peers: a GrpcServer only reports peers whose negotiated eth version
// matches its own. Three entries — one matching this sentry's version,
// one ghost (protocol == 0), one on a different eth version — only the
// matching entry comes through.
func TestGrpcServer_PeersFiltersByProtocol(t *testing.T) {
	srv := minimalP2PServer(t)
	ss := &GrpcServer{
		statusReady: make(chan struct{}),
		ethVersion:  direct.ETH68,
		Protocols:   []p2p.Protocol{{Name: eth.ProtocolName, Version: direct.ETH68}},
	}
	ss.peers.Store(NewPeerStore())
	require.NoError(t, ss.SetP2PServer(srv))

	store := ss.peers.Load()

	// Owned-by-this-sentry: protocol matches.
	owned, _ := newTestPeerInfoWithEth(t)
	var ownedKey [64]byte
	ownedKey[0] = 0x42
	store.peers[ownedKey] = owned

	// Ghost: no protocol set anywhere yet (RLPx-only / in-flight handshake).
	ghost, _ := newTestPeerInfoWithEth(t)
	ghost.protocol = 0
	ghost.witProtocol = 0
	var ghostKey [64]byte
	ghostKey[0] = 0x43
	store.peers[ghostKey] = ghost

	// Owned-by-another-sentry: eth Run for a different version populated
	// this PeerInfo (shared PeerStore in shared-Server mode).
	otherVersion, _ := newTestPeerInfoWithEth(t)
	otherVersion.protocol = direct.ETH69
	var otherKey [64]byte
	otherKey[0] = 0x44
	store.peers[otherKey] = otherVersion

	reply, err := ss.Peers(context.Background(), nil)
	require.NoError(t, err)
	require.Len(t, reply.Peers, 1, "only the entry matching this sentry's eth version should be reported")
}

// TestGrpcServer_SharedPeerStore_VisibleToEthAndWit covers the central
// invariant of SetSharedPeerStore: two GrpcServers backing one p2p.Server,
// after the swap, see the same PeerInfo when one of them calls
// getOrCreatePeer. That's what unblocks wit/0's WaitForEth in shared-
// Server mode — eth/* runs on a different sentry but populates the same
// PeerInfo's protocol field.
func TestGrpcServer_SharedPeerStore_VisibleToEthAndWit(t *testing.T) {
	shared := NewPeerStore()
	ethSentry := &GrpcServer{statusReady: make(chan struct{})}
	ethSentry.peers.Store(NewPeerStore())
	witSentry := &GrpcServer{statusReady: make(chan struct{})}
	witSentry.peers.Store(NewPeerStore())
	ethSentry.SetSharedPeerStore(shared)
	witSentry.SetSharedPeerStore(shared)

	pi, peerID := newTestPeerInfoWithEth(t)
	store := ethSentry.peers.Load()
	store.mu.Lock()
	store.peers[peerID] = pi
	store.mu.Unlock()

	// witSentry shares the same store, so the entry placed via ethSentry
	// must be visible through witSentry too.
	got := witSentry.getPeer(peerID)
	require.NotNil(t, got, "shared PeerStore: wit-side sentry must see entry placed by eth-side sentry")
	require.Same(t, pi, got)
}

// TestGrpcServer_ProtocolForMessageID_ResolvesByMessageId guards the
// disambiguation that writePeer relies on: eth and wit reuse the same
// numeric msg codes (e.g. 0x01 is both eth.NewBlockHashesMsg and
// wit.NewWitnessHashesMsg). Routing by numeric code would always pick
// the eth Protocol entry when eth is registered first, sending wit
// frames at the wrong RLPx offset. Looking up by sentryproto.MessageId
// (which is globally unique) avoids the collision.
func TestGrpcServer_ProtocolForMessageID_ResolvesByMessageId(t *testing.T) {
	ss := &GrpcServer{
		Protocols: []p2p.Protocol{
			{
				Name:      eth.ProtocolName,
				Version:   direct.ETH69,
				FromProto: eth.FromProto[direct.ETH69],
			},
			{
				Name:      wit.ProtocolName,
				Version:   wit.ProtocolVersions[0],
				FromProto: wit.FromProto[wit.ProtocolVersions[0]],
			},
		},
	}

	name, version := ss.protocolForMessageID(sentryproto.MessageId_NEW_BLOCK_HASHES_66)
	require.Equal(t, eth.ProtocolName, name)
	require.Equal(t, uint(direct.ETH69), version)

	// wit.NewWitnessHashesMsg shares the same numeric code (0x01) as
	// eth.NewBlockHashesMsg. The MessageId-based lookup must still
	// resolve to wit here.
	name, version = ss.protocolForMessageID(sentryproto.MessageId_NEW_WITNESS_HASHES_W0)
	require.Equal(t, wit.ProtocolName, name)
	require.Equal(t, wit.ProtocolVersions[0], version)
}

// TestGrpcServer_CloseDoesNotStopExternalServer verifies that GrpcServer.Close
// leaves an externally-injected Server running — the coordinator owns its
// lifecycle. We dial the listener before and after Close: it must still
// accept connections after Close because the external Server is alive.
func TestGrpcServer_CloseDoesNotStopExternalServer(t *testing.T) {
	srv := minimalP2PServerWithListener(t)
	require.NoError(t, srv.Start(context.Background(), log.Root()))
	t.Cleanup(srv.Stop)

	ss := &GrpcServer{statusReady: make(chan struct{})}
	require.NoError(t, ss.SetP2PServer(srv))
	require.True(t, listenerReachable(t, srv), "listener should be up before Close")

	ss.Close()

	require.True(t, listenerReachable(t, srv),
		"GrpcServer.Close must not stop an externally-injected p2p.Server")
}

// TestGrpcServer_CloseStopsOwnedServer covers the inverse: when the
// GrpcServer created its own Server (legacy lazy path), Close stops it.
// We dial the listener after Close and expect the connection to be refused.
func TestGrpcServer_CloseStopsOwnedServer(t *testing.T) {
	srv := minimalP2PServerWithListener(t)
	require.NoError(t, srv.Start(context.Background(), log.Root()))

	ss := &GrpcServer{statusReady: make(chan struct{})}
	ss.p2pServerLock.Lock()
	ss.p2pServer = srv
	ss.p2pServerLock.Unlock()
	require.True(t, listenerReachable(t, srv), "listener should be up before Close")

	ss.Close()

	require.False(t, listenerReachable(t, srv),
		"GrpcServer.Close (owned server) must close the listener")
}

// TestGrpcServer_AwaitStatus_ReturnsExistingImmediately covers the fast path
// where a *usable* statusData (statusUsable: NetworkId != 0 && ForkData != nil)
// has already been populated by SetStatus.
func TestGrpcServer_AwaitStatus_ReturnsExistingImmediately(t *testing.T) {
	ss := &GrpcServer{
		ctx:         context.Background(),
		statusReady: make(chan struct{}),
		statusData: &sentryproto.StatusData{
			NetworkId: 42,
			ForkData:  &sentryproto.Forks{},
		},
	}
	got := ss.awaitStatus(2 * time.Second)
	require.NotNil(t, got)
	require.Equal(t, uint64(42), got.NetworkId)
}

// TestGrpcServer_AwaitStatus_IgnoresPartialStatus exercises the validity
// gate shared with SetStatus: a non-nil but partial statusData (missing
// ForkData / zero NetworkId) must NOT be returned to handshakers — they
// would proceed with garbage and fail the eth handshake anyway.
func TestGrpcServer_AwaitStatus_IgnoresPartialStatus(t *testing.T) {
	ss := &GrpcServer{
		ctx:         context.Background(),
		statusReady: make(chan struct{}),
		statusData:  &sentryproto.StatusData{NetworkId: 42}, // ForkData nil
	}
	got := ss.awaitStatus(50 * time.Millisecond)
	require.Nil(t, got, "partial status (no ForkData) must not satisfy awaitStatus")
}

// TestGrpcServer_AwaitStatus_TimesOutWhenUnset verifies that awaitStatus
// returns nil when SetStatus never arrives, so Protocol.Run can bail out
// after the configured window instead of blocking forever.
func TestGrpcServer_AwaitStatus_TimesOutWhenUnset(t *testing.T) {
	ss := &GrpcServer{
		ctx:         context.Background(),
		statusReady: make(chan struct{}),
	}
	start := time.Now()
	got := ss.awaitStatus(50 * time.Millisecond)
	require.Nil(t, got)
	require.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond)
}

// TestGrpcServer_AwaitStatus_UnblocksOnSetStatus verifies that awaitStatus
// returns the freshly-set statusData as soon as the statusReady signal fires
// — the central startup-window fix.
func TestGrpcServer_AwaitStatus_UnblocksOnSetStatus(t *testing.T) {
	ss := &GrpcServer{
		ctx:         context.Background(),
		statusReady: make(chan struct{}),
	}
	go func() {
		time.Sleep(20 * time.Millisecond)
		ss.statusDataLock.Lock()
		ss.statusData = &sentryproto.StatusData{
			NetworkId: 7,
			ForkData:  &sentryproto.Forks{},
		}
		ss.statusDataLock.Unlock()
		close(ss.statusReady)
	}()
	got := ss.awaitStatus(2 * time.Second)
	require.NotNil(t, got)
	require.Equal(t, uint64(7), got.NetworkId)
}

// TestGrpcServer_SetStatus_NilStatusReadyIsSafe guards against panics in
// callers that instantiate GrpcServer directly without using NewGrpcServer
// (some tests do this). SetStatus closes the statusReady channel; close(nil)
// would panic, so the close is nil-guarded.
func TestGrpcServer_SetStatus_NilStatusReadyIsSafe(t *testing.T) {
	srv := minimalP2PServer(t)
	require.NoError(t, srv.Start(context.Background(), log.Root()))
	t.Cleanup(srv.Stop)

	// Build a minimal but valid status payload (genesis + empty forks lists).
	configNoFork := &chain.Config{HomesteadBlock: common.NewUint64(1), ChainID: uint256.NewInt(1)}
	dbNoFork := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	gspecNoFork := &types.Genesis{Config: configNoFork}
	genesisNoFork := genesiswrite.MustCommitGenesis(gspecNoFork, dbNoFork, datadir.New(t.TempDir()), log.Root())

	ss := &GrpcServer{} // zero-value statusReady (nil channel)
	require.NoError(t, ss.SetP2PServer(srv))

	require.NotPanics(t, func() {
		_, _ = ss.SetStatus(context.Background(), &sentryproto.StatusData{
			ForkData: &sentryproto.Forks{Genesis: gointerfaces.ConvertHashToH256(genesisNoFork.Hash())},
		})
	})
}

// TestGrpcServer_SetStatus_RejectsNilForkData covers the gRPC-entrypoint
// hardening: SetStatus is reachable over the wire from cmd/sentry, so a
// malformed StatusData (nil or missing ForkData) must return an error
// instead of nil-panicking on the ForkData dereference.
func TestGrpcServer_SetStatus_RejectsNilForkData(t *testing.T) {
	ss := &GrpcServer{statusReady: make(chan struct{})}

	// Nil StatusData.
	require.NotPanics(t, func() {
		_, err := ss.SetStatus(context.Background(), nil)
		require.Error(t, err)
	})

	// Non-nil StatusData but nil ForkData.
	require.NotPanics(t, func() {
		_, err := ss.SetStatus(context.Background(), &sentryproto.StatusData{NetworkId: 1})
		require.Error(t, err)
	})
}

// TestGrpcServer_SetP2PServer_RejectsNil guards the lifecycle invariant
// flagged by review: a nil server flipping external=true would later
// cause Close() to skip Stop() on whatever Server the lazy SetStatus path
// built — a real listener/goroutine leak. Reject at the door instead.
func TestGrpcServer_SetP2PServer_RejectsNil(t *testing.T) {
	ss := &GrpcServer{statusReady: make(chan struct{})}
	require.Error(t, ss.SetP2PServer(nil))
	require.False(t, ss.external, "external must not flip when SetP2PServer rejects the input")
}

// mockPeerEventsStream is a minimal sentryproto.Sentry_PeerEventsServer
// implementation that collects sent PeerEvents in-memory.
type mockPeerEventsStream struct {
	ctx    context.Context
	mu     sync.Mutex
	events []*sentryproto.PeerEvent
}

func (m *mockPeerEventsStream) Send(e *sentryproto.PeerEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, e)
	return nil
}
func (m *mockPeerEventsStream) Context() context.Context         { return m.ctx }
func (m *mockPeerEventsStream) SetHeader(grpcmetadata.MD) error  { return nil }
func (m *mockPeerEventsStream) SendHeader(grpcmetadata.MD) error { return nil }
func (m *mockPeerEventsStream) SetTrailer(grpcmetadata.MD)       {}
func (m *mockPeerEventsStream) SendMsg(any) error                { return nil }
func (m *mockPeerEventsStream) RecvMsg(any) error                { return nil }

// TestGrpcServer_FindBestPeersWithPermit_FiltersVersion verifies that
// findBestPeersWithPermit only considers peers whose negotiated eth version
// matches the sentry's own ethVersion when the shared PeerStore is in use.
func TestGrpcServer_FindBestPeersWithPermit_FiltersVersion(t *testing.T) {
	ss := &GrpcServer{
		statusReady: make(chan struct{}),
		ethVersion:  direct.ETH68,
	}
	ss.peers.Store(NewPeerStore())
	store := ss.peers.Load()

	// ETH68 peer — should be selected.
	pi68, _ := newTestPeerInfoWithEth(t)
	var key68 [64]byte
	key68[0] = 0x11
	store.peers[key68] = pi68

	// ETH69 peer — must be ignored by this ETH68 sentry.
	pi69, _ := newTestPeerInfoWithEth(t)
	pi69.protocol = direct.ETH69
	var key69 [64]byte
	key69[0] = 0x22
	store.peers[key69] = pi69

	// in-flight (protocol==0) — must also be ignored.
	pi0, _ := newTestPeerInfoWithEth(t)
	pi0.protocol = 0
	var key0 [64]byte
	key0[0] = 0x33
	store.peers[key0] = pi0

	got := ss.findBestPeersWithPermit(10)
	require.Len(t, got, 1, "findBestPeersWithPermit must only return peers matching this sentry's eth version")
}

// TestGrpcServer_FindPeerByMinBlock_FiltersVersion verifies that
// findPeerByMinBlock skips peers from other eth protocol versions in the
// shared PeerStore.
func TestGrpcServer_FindPeerByMinBlock_FiltersVersion(t *testing.T) {
	ss := &GrpcServer{
		statusReady: make(chan struct{}),
		ethVersion:  direct.ETH68,
	}
	ss.peers.Store(NewPeerStore())
	store := ss.peers.Load()

	// ETH69 peer with a known min block — if the filter is absent, the
	// ETH68 sentry would select this peer for GetBlockHeaders and encode
	// the request with ETH68 codes, causing a protocol error on the
	// remote side.
	pi69, _ := newTestPeerInfoWithEth(t)
	pi69.protocol = direct.ETH69
	pi69.minBlock = 0 // minBlock 0 satisfies any findPeerByMinBlock(0) call
	var key69 [64]byte
	key69[0] = 0x11
	store.peers[key69] = pi69

	_, found := ss.findPeerByMinBlock(0)
	require.False(t, found, "findPeerByMinBlock must not return a peer from a different eth version")
}

// TestGrpcServer_PeerEvents_ReplayFiltersByVersion verifies that the
// replay pass inside PeerEvents only emits Connect events for peers
// whose negotiated eth version matches this sentry's ethVersion.
func TestGrpcServer_PeerEvents_ReplayFiltersByVersion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ss := &GrpcServer{
		ctx:          ctx,
		statusReady:  make(chan struct{}),
		ethVersion:   direct.ETH68,
		peersStreams: NewPeersStreams(),
	}
	ss.peers.Store(NewPeerStore())
	store := ss.peers.Load()

	// ETH68 peer — should appear in the replay.
	pi68, _ := newTestPeerInfoWithEth(t)
	var key68 [64]byte
	key68[0] = 0x10
	store.peers[key68] = pi68

	// ETH69 peer — must NOT appear (different version).
	pi69, _ := newTestPeerInfoWithEth(t)
	pi69.protocol = direct.ETH69
	var key69 [64]byte
	key69[0] = 0x20
	store.peers[key69] = pi69

	// in-flight (protocol==0) — must NOT appear.
	pi0, _ := newTestPeerInfoWithEth(t)
	pi0.protocol = 0
	var key0 [64]byte
	key0[0] = 0x30
	store.peers[key0] = pi0

	streamCtx, streamCancel := context.WithCancel(ctx)
	stream := &mockPeerEventsStream{ctx: streamCtx}

	done := make(chan error, 1)
	go func() { done <- ss.PeerEvents(nil, stream) }()

	// Give the replay goroutine time to flush, then cancel the stream.
	time.Sleep(50 * time.Millisecond)
	streamCancel()
	require.NoError(t, <-done)

	stream.mu.Lock()
	defer stream.mu.Unlock()
	require.Len(t, stream.events, 1, "PeerEvents replay must only emit Connect for the ETH68 peer")
	require.Equal(t, sentryproto.PeerEvent_Connect, stream.events[0].EventId)
}
