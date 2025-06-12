package sentry

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-db/rawdb"
	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/direct"
	"github.com/erigontech/erigon-lib/gointerfaces"
	proto_sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/temporal/temporaltest"
	"github.com/erigontech/erigon-lib/types"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/rlp"
	p2p "github.com/erigontech/erigon-p2p"
	"github.com/erigontech/erigon-p2p/enode"
	"github.com/erigontech/erigon-p2p/forkid"
	"github.com/erigontech/erigon-p2p/protocols/eth"
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

	code, err := s.Uint()
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
	err := rlp.Encode(buf, []interface{}{msg.Code, payloadBytes}) // Encode as a list [code, payload]
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

func createDummyStatusData(networkID uint64, bestHash common.Hash, totalDifficulty *big.Int, genesisHash common.Hash, earliestBlockHeight uint64, maxBlockHeight uint64) *proto_sentry.StatusData {
	return &proto_sentry.StatusData{
		NetworkId:       networkID,
		TotalDifficulty: gointerfaces.ConvertUint256IntToH256(uint256.MustFromBig(totalDifficulty)),
		BestHash:        gointerfaces.ConvertHashToH256(bestHash),
		ForkData: &proto_sentry.Forks{
			Genesis:     gointerfaces.ConvertHashToH256(genesisHash),
			HeightForks: []uint64{},
			TimeForks:   []uint64{},
		},
		MaxBlockHeight:      maxBlockHeight,
		MaxBlockTime:        0,
		EarliestBlockHeight: earliestBlockHeight,
	}
}

func TestHandShake69_ETH69ToETH69(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)
	require := require.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
		EarliestBlock:   sentry2Status.EarliestBlockHeight,
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
		EarliestBlock:   sentry1Status.EarliestBlockHeight,
		LatestBlock:     sentry1Status.MaxBlockHeight,
		LatestBlockHash: gointerfaces.ConvertH256ToHash(sentry1Status.BestHash),
	}
	sentry1StatusBytes, err := rlp.EncodeToBytes(sentry1EthStatus)
	require.NoError(err)
	err = sentry1RW.WriteMsg(p2p.Msg{Code: eth.StatusMsg, Size: uint32(len(sentry1StatusBytes)), Payload: bytes.NewReader(sentry1StatusBytes)})
	require.NoError(err)
	sentry2RW.WriteToReadBuffer(sentry1RW.ReadAllWritten())

	// Run handShake69 for Sentry 1 in a goroutine
	var peerBestHash1 *common.Hash
	var peerErr1 *p2p.PeerError
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		peerBestHash1, peerErr1 = handShake69(ctx, sentry1Status, sentry1RW, direct.ETH69, direct.ETH69)
	}()

	// Run handShake69 for Sentry 2 in a goroutine
	var peerBestHash2 *common.Hash
	var peerErr2 *p2p.PeerError
	wg.Add(1)
	go func() {
		defer wg.Done()
		peerBestHash2, peerErr2 = handShake69(ctx, sentry2Status, sentry2RW, direct.ETH69, direct.ETH69)
	}()

	wg.Wait()

	assert.Nil(peerErr1)
	assert.NotNil(peerBestHash1)
	assert.Equal(sentry2Status.BestHash, gointerfaces.ConvertHashToH256(*peerBestHash1))

	assert.Nil(peerErr2)
	assert.NotNil(peerBestHash2)
	assert.Equal(sentry1Status.BestHash, gointerfaces.ConvertHashToH256(*peerBestHash2))

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
		EarliestBlock:   sentry1Status.EarliestBlockHeight,
		LatestBlock:     sentry1Status.MaxBlockHeight,
		LatestBlockHash: gointerfaces.ConvertH256ToHash(sentry1Status.BestHash),
	}
	sentry1StatusBytes, err := rlp.EncodeToBytes(sentry1EthStatus)
	require.NoError(err)
	err = sentry1RW.WriteMsg(p2p.Msg{Code: eth.StatusMsg, Size: uint32(len(sentry1StatusBytes)), Payload: bytes.NewReader(sentry1StatusBytes)})
	require.NoError(err)
	sentry2RW.WriteToReadBuffer(sentry1RW.ReadAllWritten())

	// Run handShake69 for Sentry 1 (ETH69)
	wg := sync.WaitGroup{}
	wg.Add(2)
	var peerErr1 *p2p.PeerError
	var peerErr2 *p2p.PeerError

	go func() {
		defer wg.Done()
		_, peerErr1 = handShake69(ctx, sentry1Status, sentry1RW, direct.ETH69, direct.ETH68)
	}()

	go func() {
		defer wg.Done()
		_, peerErr2 = handShake(ctx, sentry2Status, sentry2RW, direct.ETH68, direct.ETH68)
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
		err := rlp.Encode(buf, []interface{}{msg.Code, msg.Payload})
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

// SimulatePeer simulates a peer's behavior in a handshake.
// It sends its status and then waits to receive the other peer's status.
func SimulatePeer(ctx context.Context, rw *RLPReadWriter, ownStatus *proto_sentry.StatusData, ownProtocol uint, expectedMinProtocol uint, isETH69 bool) (*common.Hash, *p2p.PeerError) {
	// Send own status
	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("Recovered in SimulatePeer send: %v\n", r)
			}
		}()
		var statusMsg interface{}
		if isETH69 {
			statusMsg = &eth.StatusPacket69{
				ProtocolVersion: uint32(ownProtocol),
				NetworkID:       ownStatus.NetworkId,
				Genesis:         gointerfaces.ConvertH256ToHash(ownStatus.ForkData.Genesis),
				ForkID:          forkid.NewIDFromForks(ownStatus.ForkData.HeightForks, ownStatus.ForkData.TimeForks, gointerfaces.ConvertH256ToHash(ownStatus.ForkData.Genesis), ownStatus.MaxBlockHeight, ownStatus.MaxBlockTime),
				EarliestBlock:   ownStatus.EarliestBlockHeight,
				LatestBlock:     ownStatus.MaxBlockHeight,
				LatestBlockHash: gointerfaces.ConvertH256ToHash(ownStatus.BestHash),
			}
		} else {
			statusMsg = &eth.StatusPacket{
				ProtocolVersion: uint32(ownProtocol),
				NetworkID:       ownStatus.NetworkId,
				TD:              gointerfaces.ConvertH256ToUint256Int(ownStatus.TotalDifficulty).ToBig(),
				Head:            gointerfaces.ConvertH256ToHash(ownStatus.BestHash),
				Genesis:         gointerfaces.ConvertH256ToHash(ownStatus.ForkData.Genesis),
				ForkID:          forkid.NewIDFromForks(ownStatus.ForkData.HeightForks, ownStatus.ForkData.TimeForks, gointerfaces.ConvertH256ToHash(ownStatus.ForkData.Genesis), ownStatus.MaxBlockHeight, ownStatus.MaxBlockTime),
			}
		}
		b, err := rlp.EncodeToBytes(statusMsg)
		if err != nil {
			fmt.Printf("SimulatePeer: failed to encode status: %v\n", err)
			return
		}
		err = rw.WriteMsg(p2p.Msg{Code: eth.StatusMsg, Size: uint32(len(b)), Payload: bytes.NewReader(b)})
		if err != nil {
			fmt.Printf("SimulatePeer: failed to write status msg: %v\n", err)
		}
	}()

	// Receive other peer's status
	if isETH69 {
		return handShake69(ctx, ownStatus, rw, ownProtocol, expectedMinProtocol)
	}
	return handShake(ctx, ownStatus, rw, ownProtocol, expectedMinProtocol)
}

func TestHandShake69_ETH69ToETH69_WithRLP(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	var peerBestHash1 *common.Hash
	var peerErr1 *p2p.PeerError
	go func() {
		defer wg.Done()
		peerBestHash1, peerErr1 = SimulatePeer(ctx, sentry1RW, sentry1Status, direct.ETH69, direct.ETH69, true)
	}()

	var peerBestHash2 *common.Hash
	var peerErr2 *p2p.PeerError
	go func() {
		defer wg.Done()
		peerBestHash2, peerErr2 = SimulatePeer(ctx, sentry2RW, sentry2Status, direct.ETH69, direct.ETH69, true)
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
	assert.NotNil(peerBestHash1)
	assert.Equal(sentry2Status.BestHash, gointerfaces.ConvertHashToH256(*peerBestHash1))

	assert.Nil(peerErr2)
	assert.NotNil(peerBestHash2)
	assert.Equal(sentry1Status.BestHash, gointerfaces.ConvertHashToH256(*peerBestHash2))

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
		_, peerErr1 = SimulatePeer(ctx, sentry1RW, sentry1Status, direct.ETH69, direct.ETH68, true)
	}()

	go func() {
		defer wg.Done()
		_, peerErr2 = SimulatePeer(ctx, sentry2RW, sentry2Status, direct.ETH68, direct.ETH68, false)
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

	headTd256 := new(uint256.Int)
	headTd256.SetFromBig(headTd)
	heightForks, timeForks := forkid.GatherForks(genesis.Config, genesis.Timestamp)
	s.statusData = &proto_sentry.StatusData{
		NetworkId:       1,
		TotalDifficulty: gointerfaces.ConvertUint256IntToH256(headTd256),
		BestHash:        gointerfaces.ConvertHashToH256(head.Hash()),
		MaxBlockHeight:  head.Number.Uint64(),
		MaxBlockTime:    head.Time,
		ForkData: &proto_sentry.Forks{
			Genesis:     gointerfaces.ConvertHashToH256(genesisHash),
			HeightForks: heightForks,
			TimeForks:   timeForks,
		},
	}
	return s

}

func startHandshake(
	ctx context.Context,
	status *proto_sentry.StatusData,
	pipe *p2p.MsgPipeRW,
	protocolVersion uint,
	errChan chan *p2p.PeerError,
) {
	go func() {
		_, err := handShake(ctx, status, pipe, protocolVersion, protocolVersion)
		errChan <- err
	}()
}

// Tests that peers are correctly accepted (or rejected) based on the advertised
// fork IDs in the protocol handshake.
func TestForkIDSplit67(t *testing.T) { testForkIDSplit(t, direct.ETH67) }

func testForkIDSplit(t *testing.T, protocol uint) {
	var (
		ctx           = context.Background()
		configNoFork  = &chain.Config{HomesteadBlock: big.NewInt(1), ChainID: big.NewInt(1)}
		configProFork = &chain.Config{
			ChainID:               big.NewInt(1),
			HomesteadBlock:        big.NewInt(1),
			TangerineWhistleBlock: big.NewInt(2),
			SpuriousDragonBlock:   big.NewInt(2),
			ByzantiumBlock:        big.NewInt(3),
		}
		dbNoFork  = temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
		dbProFork = temporaltest.NewTestDB(t, datadir.New(t.TempDir()))

		gspecNoFork  = &types.Genesis{Config: configNoFork}
		gspecProFork = &types.Genesis{Config: configProFork}

		genesisNoFork  = rawdb.MustCommitGenesisWithoutState(gspecNoFork, dbNoFork)
		genesisProFork = rawdb.MustCommitGenesisWithoutState(gspecProFork, dbProFork)
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

func emptyBootnodeURL(genesis common.Hash) []string {
	return []string{}

}

func mainnetDNSNetwork(genesis common.Hash, protocol string) string {
	return "enrtree://AKA3AM6LPBYEUDMVNU3BSVQJ5AD45Y7YPOHJLEF6W26QOE4VTUDPE@" + protocol + ".mainnet.ethdisco.net"
}

func TestSentryServerImpl_SetStatusInitPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic during server initialization")
		}
	}()

	configNoFork := &chain.Config{HomesteadBlock: big.NewInt(1), ChainID: big.NewInt(1)}
	dbNoFork := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	gspecNoFork := &types.Genesis{Config: configNoFork}
	genesisNoFork := rawdb.MustCommitGenesisWithoutState(gspecNoFork, dbNoFork)
	ss := &GrpcServer{p2p: &p2p.Config{LookupBootnodeURLs: emptyBootnodeURL, LookupDNSNetwork: mainnetDNSNetwork}}

	_, err := ss.SetStatus(context.Background(), &proto_sentry.StatusData{
		ForkData: &proto_sentry.Forks{Genesis: gointerfaces.ConvertHashToH256(genesisNoFork.Hash())},
	})
	if err == nil {
		t.Fatalf("error expected")
	}

	// Should not panic here.
	_, err = ss.SetStatus(context.Background(), &proto_sentry.StatusData{
		ForkData: &proto_sentry.Forks{Genesis: gointerfaces.ConvertHashToH256(genesisNoFork.Hash())},
	})
	if err == nil {
		t.Fatalf("error expected")
	}
}
