package helper

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/nat"
	"github.com/erigontech/erigon/p2p/protocols/eth"
	"github.com/erigontech/erigon/p2p/sentry"
)

var (
	txChanSize = 5000
)

type TxMessage struct {
	MessageID sentryproto.MessageId
	Payload   []byte
}

type p2pClient struct {
	adminRPC string
}

// connect adminRPC
// Example: http://127.0.0.1:8545/
func NewP2P(adminRPC string) *p2pClient {
	return &p2pClient{
		adminRPC: adminRPC,
	}
}

type NodeInfo struct {
	Enode      string
	NetworkID  uint64
	Difficulty *uint256.Int
	Genesis    [32]byte
}

// FetchNodeInfo queries admin_nodeInfo and returns the node's identity. It errors
// unless the response is a well-formed, ready erigon node (valid enode and a
// 32-byte genesis hash), so callers can tell a real node apart from an unrelated
// service that merely happens to answer on the same port.
func FetchNodeInfo(adminRPC string) (NodeInfo, error) {
	r, err := http.Post(adminRPC, "application/json", strings.NewReader(
		`{"jsonrpc":"2.0","method":"admin_nodeInfo","params":[],"id":1}`,
	))
	if err != nil {
		return NodeInfo{}, err
	}
	defer r.Body.Close()
	return parseNodeInfo(r.Body)
}

func parseNodeInfo(body io.Reader) (NodeInfo, error) {
	var resp struct {
		Result struct {
			Enode     string `json:"enode"`
			Protocols struct {
				Eth struct {
					Genesis    string `json:"genesis"`
					Network    int    `json:"network"`
					Difficulty int    `json:"difficulty"`
				} `json:"eth"`
			} `json:"protocols"`
		} `json:"result"`
	}
	if err := json.NewDecoder(body).Decode(&resp); err != nil {
		return NodeInfo{}, err
	}

	eth := resp.Result.Protocols.Eth
	genesis, err := hexutil.FromHexWithValidation(eth.Genesis)
	if err != nil {
		return NodeInfo{}, fmt.Errorf("admin_nodeInfo returned invalid genesis %q: %w", eth.Genesis, err)
	}
	if len(genesis) != 32 {
		return NodeInfo{}, fmt.Errorf("admin_nodeInfo returned genesis of %d bytes, want 32", len(genesis))
	}
	if resp.Result.Enode == "" {
		return NodeInfo{}, fmt.Errorf("admin_nodeInfo returned empty enode")
	}

	difficulty := eth.Difficulty
	if difficulty < 0 {
		difficulty = 0
	}
	return NodeInfo{
		Enode:      resp.Result.Enode,
		NetworkID:  uint64(eth.Network),
		Difficulty: uint256.NewInt(uint64(difficulty)),
		Genesis:    [32]byte(genesis),
	}, nil
}

func (p *p2pClient) Connect() (<-chan TxMessage, <-chan error, error) {
	privateKey, err := crypto.GenerateKey()
	if err != nil {
		return nil, nil, err
	}

	cfg := &p2p.Config{
		ListenAddr:      ":30307",
		ProtocolVersion: []uint{direct.ETH70, direct.ETH69, direct.ETH68},
		MaxPeers:        32,
		MaxPendingPeers: 1000,
		NAT:             nat.Any(),
		NoDiscovery:     true,
		Name:            "p2p-mock",
		NodeDatabase:    "dev/nodes/eth68",
		PrivateKey:      privateKey,
	}

	info, err := FetchNodeInfo(p.adminRPC)
	if err != nil {
		return nil, nil, err
	}

	if cfg.StaticNodes, err = enode.ParseNodesFromURLs([]string{info.Enode}); err != nil {
		return nil, nil, err
	}

	ready, err := p.notifyWhenReady()
	if err != nil {
		return nil, nil, err
	}

	grpcServer := sentry.NewGrpcServer(context.TODO(), nil, func() *eth.NodeInfo { return nil }, cfg, direct.ETH68, log.New(), nil, "")
	sentryClient, err := direct.NewSentryClientDirect(direct.ETH69, grpcServer, nil)
	if err != nil {
		return nil, nil, err
	}

	_, err = sentryClient.SetStatus(context.TODO(), &sentryproto.StatusData{
		NetworkId:       info.NetworkID,
		TotalDifficulty: gointerfaces.ConvertUint256IntToH256(info.Difficulty),
		BestHash:        gointerfaces.ConvertHashToH256(info.Genesis),
		ForkData: &sentryproto.Forks{
			Genesis: gointerfaces.ConvertHashToH256(info.Genesis),
		},
	})
	if err != nil {
		return nil, nil, err
	}

	conn, err := sentryClient.Messages(context.TODO(), &sentryproto.MessagesRequest{
		Ids: []sentryproto.MessageId{
			sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_66,
			sentryproto.MessageId_GET_POOLED_TRANSACTIONS_66,
			sentryproto.MessageId_TRANSACTIONS_66,
			sentryproto.MessageId_POOLED_TRANSACTIONS_66,
			sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68,
		},
	})
	if err != nil {
		return nil, nil, err
	}

	gotTxCh := make(chan TxMessage, txChanSize)
	errCh := make(chan error)

	go p.serve(conn, gotTxCh, errCh)
	<-ready

	return gotTxCh, errCh, nil
}

func (p *p2pClient) notifyWhenReady() (<-chan struct{}, error) {
	ready := make(chan struct{})

	r, err := http.Post(p.adminRPC, "application/json", strings.NewReader(
		`{"jsonrpc":"2.0","method":"admin_peers","params":[],"id":1}`,
	))
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()

	var resp struct {
		Result []struct {
			Enode string `json:"enode"`
		} `json:"result"`
	}

	if err := json.NewDecoder(r.Body).Decode(&resp); err != nil {
		return nil, err
	}

	numConn := len(resp.Result)

	go func() {
		for {
			time.Sleep(100 * time.Millisecond)

			r, err := http.Post(p.adminRPC, "application/json", strings.NewReader(
				`{"jsonrpc":"2.0","method":"admin_peers","params":[],"id":1}`,
			))
			if err != nil {
				continue
			}
			defer r.Body.Close()

			if err := json.NewDecoder(r.Body).Decode(&resp); err != nil {
				continue
			}

			if len(resp.Result) > numConn {
				ready <- struct{}{}
				return
			}
		}
	}()

	return ready, nil
}

func (p *p2pClient) serve(conn sentryproto.Sentry_MessagesClient, gotTxCh chan<- TxMessage, errCh chan<- error) {
	for {
		req, err := conn.Recv()
		if err != nil {
			errCh <- err
			continue
		}

		gotTxCh <- TxMessage{
			MessageID: req.Id,
			Payload:   req.Data,
		}
	}
}
