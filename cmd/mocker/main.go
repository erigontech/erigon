package main

import (
	"context"
	"fmt"
	"time"

	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/eth/protocols/eth"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/p2p/nat"
	"github.com/erigontech/erigon/p2p/sentry"
	"github.com/holiman/uint256"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/direct"
	"github.com/erigontech/erigon-lib/gointerfaces"
	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
)

var (
	staticPeers = []string{
		"enode://1f2055bb57f0acefad5956c9673d9b586f8a1cefb0494f00ab41c0c628f9de24726f35b4be0be0d2e417bf5268abc61c8f7b493055742026163c124acf37117f@127.0.0.1:30304",
	}
)

func main() {
	privateKey, err := crypto.GenerateKey()
	if err != nil {
		panic(err)
	}

	cfg := &p2p.Config{
		ListenAddr:      ":30305",
		AllowedPorts:    []uint{30303, 30304, 30305, 30306, 30307},
		ProtocolVersion: []uint{direct.ETH68, direct.ETH67},
		MaxPeers:        32,
		MaxPendingPeers: 1000,
		NAT:             nat.Any(),
		NoDiscovery:     true,
		Name:            "p2p-mock",
		NodeDatabase:    "dev/nodes/eth67",
		PrivateKey:      privateKey,
	}

	staticNodes, err := utils.ParseNodesFromURLs(staticPeers)
	if err != nil {
		panic(err)
	}
	cfg.StaticNodes = staticNodes

	grpcServer := sentry.NewGrpcServer(context.TODO(), nil, func() *eth.NodeInfo { return nil }, cfg, direct.ETH67, log.New())
	sentry := direct.NewSentryClientDirect(direct.ETH67, grpcServer)

	_, err = sentry.SetStatus(context.TODO(), &sentryproto.StatusData{
		NetworkId:       1337,
		TotalDifficulty: gointerfaces.ConvertUint256IntToH256(uint256.MustFromDecimal("1")),
		BestHash: gointerfaces.ConvertHashToH256(
			[32]byte(libcommon.FromHex("c493151b4991dd1bf459952509bba9bc3fb2b706299e18e36e6b78781835065d")),
		),
		ForkData: &sentryproto.Forks{
			Genesis: gointerfaces.ConvertHashToH256(
				[32]byte(libcommon.FromHex("c493151b4991dd1bf459952509bba9bc3fb2b706299e18e36e6b78781835065d")),
			),
		},
	})
	if err != nil {
		panic(err)
	}

	stream, err := sentry.Messages(context.TODO(), &sentryproto.MessagesRequest{
		Ids: []sentryproto.MessageId{
			sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_66,
			sentryproto.MessageId_GET_POOLED_TRANSACTIONS_66,
			sentryproto.MessageId_TRANSACTIONS_66,
			sentryproto.MessageId_POOLED_TRANSACTIONS_66,
			sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68,
		},
	})

	if err != nil {
		panic(err)
	}

	var n int

	for {
		req, err := stream.Recv()
		if err != nil {
			panic(err)
		}

		switch req.Id {
		case sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_66:
			fmt.Println("MessageId_NEW_POOLED_TRANSACTION_HASHES_66")
			fmt.Println("n = ", n, time.Now())
			n++
		case sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68:
			fmt.Println("MessageId_NEW_POOLED_TRANSACTION_HASHES_68")
		case sentryproto.MessageId_GET_POOLED_TRANSACTIONS_66:
			fmt.Println("MessageId_GET_POOLED_TRANSACTIONS_66")
		case sentryproto.MessageId_POOLED_TRANSACTIONS_66:
			fmt.Println("MessageId_POOLED_TRANSACTIONS_66")
		case sentryproto.MessageId_TRANSACTIONS_66:
			fmt.Println("MessageId_TRANSACTIONS_66")
		default:
			panic(fmt.Sprintf("unknown id %d", req.Id))
		}
	}
}
