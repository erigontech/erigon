package sentry

import (
	"context"
	"encoding/json"
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
		case interface{ Protocol() uint }:
			return []byte{byte(sentry.Protocol())}
		case *sentryMultiplexer:
			if infos, err := sentry.NodeInfos(context.Background()); err == nil {
				var protocols []byte
				var seen map[byte]struct{} = map[byte]struct{}{}
				for _, info := range infos {
					var rawProtocols map[string]json.RawMessage
					json.Unmarshal(info.Protocols, &rawProtocols)

					if rawJson, ok := rawProtocols["eth"]; ok {
						protocol := struct {
							Name    string `json:"name"`
							Version uint   `json:"version"`
						}{}

						json.Unmarshal(rawJson, &protocol)

						if protocol.Version > 0 {
							p := byte(protocol.Version)
							if _, ok := seen[p]; !ok {
								protocols = append(protocols, p)
								seen[p] = struct{}{}
							}
						}
					}

				}
				return protocols
			}
		default:
			if info, err := sentry.NodeInfo(context.Background(), &emptypb.Empty{}); err == nil {
				var protocols map[string]json.RawMessage
				json.Unmarshal(info.Protocols, &protocols)

				if rawJson, ok := protocols["eth"]; ok {
					protocol := struct {
						Name    string `json:"name"`
						Version uint   `json:"version"`
					}{}

					json.Unmarshal(rawJson, &protocol)

					if protocol.Version > 0 {
						return []byte{byte(protocol.Version)}
					}
				}
			}
		}
	}

	return nil
}
