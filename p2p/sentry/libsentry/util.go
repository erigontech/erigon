// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package libsentry

import (
	"context"
	"strconv"
	"strings"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/gointerfaces/typesproto"
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
