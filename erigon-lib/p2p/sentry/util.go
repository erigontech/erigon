package sentry

import (
	"context"
	"strconv"
	"strings"

	"github.com/erigontech/erigon/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/erigon-lib/gointerfaces/typesproto"
	"google.golang.org/protobuf/types/known/emptypb"
)

func PeerProtocols(sentry sentryproto.SentryClient, peer *typesproto.H512) []byte {
	if reply, err := sentry.PeerById(context.Background(), &sentryproto.PeerByIdRequest{PeerId: peer}); err == nil {
		info := reply.GetPeer()
		var protocols []byte

		if info != nil {
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
	}

	return nil
}

func Protocols(sentry sentryproto.SentryClient) []byte {
	switch sentry := sentry.(type) {
	case interface{ Protocol() uint }:
		return []byte{byte(sentry.Protocol())}
	default:
		if infos, err := sentry.Peers(context.Background(), &emptypb.Empty{}); err == nil {
			var protocols []byte
			var seen map[byte]struct{} = map[byte]struct{}{}
			for _, info := range infos.GetPeers() {
				for _, cap := range info.Caps {
					parts := strings.Split(cap, "/")
					if len(parts) > 1 && strings.EqualFold(parts[0], "ETH") {
						if version, err := strconv.Atoi(parts[1]); err == nil {
							p := byte(version)
							if _, ok := seen[p]; !ok {
								protocols = append(protocols, p)
								seen[p] = struct{}{}
							}
						}
					}
				}
			}

			return protocols
		}
	}
	return nil
}
