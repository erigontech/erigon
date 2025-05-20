package helper

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/direct"
	"github.com/erigontech/erigon-lib/gointerfaces"
	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/p2p"
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

func (p *p2pClient) Connect() (<-chan TxMessage, <-chan error, error) {
	privateKey, err := crypto.GenerateKey()
	if err != nil {
		return nil, nil, err
	}

	cfg := &p2p.Config{
		ListenAddr:      ":30307",
		AllowedPorts:    []uint{30303, 30304, 30305, 30306, 30307},
		ProtocolVersion: []uint{direct.ETH69, direct.ETH68, direct.ETH67},
		MaxPeers:        32,
		MaxPendingPeers: 1000,
		NAT:             nat.Any(),
		NoDiscovery:     true,
		Name:            "p2p-mock",
		NodeDatabase:    "dev/nodes/eth67",
		PrivateKey:      privateKey,
	}

	r, err := http.Post(p.adminRPC, "application/json", strings.NewReader(
		`{"jsonrpc":"2.0","method":"admin_nodeInfo","params":[],"id":1}`,
	))
	if err != nil {
		return nil, nil, err
	}
	defer r.Body.Close()

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

	if err := json.NewDecoder(r.Body).Decode(&resp); err != nil {
		return nil, nil, err
	}

	if cfg.StaticNodes, err = utils.ParseNodesFromURLs([]string{resp.Result.Enode}); err != nil {
		return nil, nil, err
	}

	ready, err := p.notifyWhenReady()
	if err != nil {
		return nil, nil, err
	}

	grpcServer := sentry.NewGrpcServer(context.TODO(), nil, func() *eth.NodeInfo { return nil }, cfg, direct.ETH68, log.New())
	sentry := direct.NewSentryClientDirect(direct.ETH69, grpcServer)

	_, err = sentry.SetStatus(context.TODO(), &sentryproto.StatusData{
		NetworkId:       uint64(resp.Result.Protocols.Eth.Network),
		TotalDifficulty: gointerfaces.ConvertUint256IntToH256(uint256.MustFromDecimal(strconv.Itoa(resp.Result.Protocols.Eth.Difficulty))),
		BestHash: gointerfaces.ConvertHashToH256(
			[32]byte(common.FromHex(resp.Result.Protocols.Eth.Genesis)),
		),
		ForkData: &sentryproto.Forks{
			Genesis: gointerfaces.ConvertHashToH256(
				[32]byte(common.FromHex(resp.Result.Protocols.Eth.Genesis)),
			),
		},
	})
	if err != nil {
		return nil, nil, err
	}

	conn, err := sentry.Messages(context.TODO(), &sentryproto.MessagesRequest{
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
