package sentry

import (
	"context"
	"strconv"
	"strings"

	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/types"
	"google.golang.org/protobuf/types/known/emptypb"
)

func Protocols(sentry sentryproto.SentryClient, peer types.PeerID) []byte {
	if peer != nil {
		if reply, err := sentry.PeerById(context.Background(), &sentryproto.PeerByIdRequest{PeerId: peer}); err == nil {
			info := reply.GetPeer()
			var protocols []byte

			for _, cap := range info.Caps {
				parts := strings.Split(cap, "/")
				if len(parts) > 1 && strings.EqualFold(parts[0], "ETH") {
					if version, err := strconv.Atoi(parts[1]); err == nil {
						protocols = append(protocols, byte(version))
					}
				}
			}

			return protocols
		}
	} else {
		switch sentry := sentry.(type) {
		case interface{ Protocol() byte }:
			return []byte{byte(sentry.Protocol())}
		case *sentryMultiplexer:
			if infos, err := sentry.NodeInfos(context.Background()); err == nil {
				var protocols []byte
				var seen map[byte]struct{} = map[byte]struct{}{}
				for _, info := range infos {
					for _, p := range info.Protocols {
						if _, ok := seen[p]; !ok {
							protocols = append(protocols, p)
							seen[p] = struct{}{}
						}
					}
				}
				return protocols
			}
		default:
			if info, err := sentry.NodeInfo(context.Background(), &emptypb.Empty{}); err == nil {
				return info.Protocols
			}
		}
	}

	return nil
}
