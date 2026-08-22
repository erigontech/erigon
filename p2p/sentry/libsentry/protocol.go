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
	"fmt"
	"maps"
	"slices"

	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/p2p/protocols/eth"
	"github.com/erigontech/erigon/p2p/protocols/wit"
)

const noProtocol sentryproto.Protocol = -1

var (
	UintToProtocolMap = map[uint]sentryproto.Protocol{
		eth.ETH68: sentryproto.Protocol_ETH68,
		eth.ETH69: sentryproto.Protocol_ETH69,
		eth.ETH70: sentryproto.Protocol_ETH70,
		eth.ETH71: sentryproto.Protocol_ETH71,
	}
	ProtocolToUintMap = inverseProtocolMap(UintToProtocolMap)

	UintToSideProtocolMap = map[uint]sentryproto.Protocol{
		wit.WIT1: sentryproto.Protocol_WIT0,
	}
	SupportedSideProtocols = map[sentryproto.Protocol]struct{}{
		sentryproto.Protocol_WIT0: {},
	}
)

func inverseProtocolMap(uintToProtocol map[uint]sentryproto.Protocol) map[sentryproto.Protocol]uint {
	inv := make(map[sentryproto.Protocol]uint, len(uintToProtocol))
	for version, protocol := range uintToProtocol {
		if _, ok := inv[protocol]; ok {
			panic(fmt.Sprintf("protocol %s mapped from multiple versions", protocol))
		}
		inv[protocol] = version
	}
	return inv
}

// ethProtocolsByVersion lists ETH protocols in ascending version order.
// Used by MinProtocol to find the lowest version supporting a message.
var ethProtocolsByVersion = func() []sentryproto.Protocol {
	versions := slices.Sorted(maps.Keys(UintToProtocolMap))
	protocols := make([]sentryproto.Protocol, len(versions))
	for i, version := range versions {
		protocols[i] = UintToProtocolMap[version]
	}
	return protocols
}()

func MinProtocol(m sentryproto.MessageId) sentryproto.Protocol {
	for _, p := range ethProtocolsByVersion {
		if ids, ok := ProtoIds[p]; ok {
			if _, ok := ids[m]; ok {
				return p
			}
		}
	}

	return noProtocol
}

var ProtoIds = func() map[sentryproto.Protocol]map[sentryproto.MessageId]struct{} {
	ids := make(map[sentryproto.Protocol]map[sentryproto.MessageId]struct{}, len(eth.FromProto)+len(wit.FromProto))
	for version, fromProto := range eth.FromProto {
		ids[protocolOf(UintToProtocolMap, version)] = messageIdSet(fromProto)
	}
	for version, fromProto := range wit.FromProto {
		ids[protocolOf(UintToSideProtocolMap, version)] = messageIdSet(fromProto)
	}
	return ids
}()

func protocolOf(uintToProtocol map[uint]sentryproto.Protocol, version uint) sentryproto.Protocol {
	protocol, ok := uintToProtocol[version]
	if !ok {
		panic(fmt.Sprintf("no sentry protocol registered for version %d", version))
	}
	return protocol
}

func messageIdSet(fromProto map[sentryproto.MessageId]uint64) map[sentryproto.MessageId]struct{} {
	set := make(map[sentryproto.MessageId]struct{}, len(fromProto))
	for id := range fromProto {
		set[id] = struct{}{}
	}
	return set
}
