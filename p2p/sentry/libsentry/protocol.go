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
	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
)

func MinProtocol(m sentryproto.MessageId) sentryproto.Protocol {
	for p := sentryproto.Protocol_ETH67; p <= sentryproto.Protocol_ETH68; p++ {
		if ids, ok := ProtoIds[p]; ok {
			if _, ok := ids[m]; ok {
				return p
			}
		}
	}

	return -1
}

func ProtocolVersion(p sentryproto.Protocol) uint {
	return uint(p + 65)
}

var ProtoIds = map[sentryproto.Protocol]map[sentryproto.MessageId]struct{}{
	sentryproto.Protocol_ETH67: {
		sentryproto.MessageId_GET_BLOCK_HEADERS_66:             struct{}{},
		sentryproto.MessageId_BLOCK_HEADERS_66:                 struct{}{},
		sentryproto.MessageId_GET_BLOCK_BODIES_66:              struct{}{},
		sentryproto.MessageId_BLOCK_BODIES_66:                  struct{}{},
		sentryproto.MessageId_GET_RECEIPTS_66:                  struct{}{},
		sentryproto.MessageId_RECEIPTS_66:                      struct{}{},
		sentryproto.MessageId_NEW_BLOCK_HASHES_66:              struct{}{},
		sentryproto.MessageId_NEW_BLOCK_66:                     struct{}{},
		sentryproto.MessageId_TRANSACTIONS_66:                  struct{}{},
		sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_66: struct{}{},
		sentryproto.MessageId_GET_POOLED_TRANSACTIONS_66:       struct{}{},
		sentryproto.MessageId_POOLED_TRANSACTIONS_66:           struct{}{},
	},
	sentryproto.Protocol_ETH68: {
		sentryproto.MessageId_GET_BLOCK_HEADERS_66:             struct{}{},
		sentryproto.MessageId_BLOCK_HEADERS_66:                 struct{}{},
		sentryproto.MessageId_GET_BLOCK_BODIES_66:              struct{}{},
		sentryproto.MessageId_BLOCK_BODIES_66:                  struct{}{},
		sentryproto.MessageId_GET_RECEIPTS_66:                  struct{}{},
		sentryproto.MessageId_RECEIPTS_66:                      struct{}{},
		sentryproto.MessageId_NEW_BLOCK_HASHES_66:              struct{}{},
		sentryproto.MessageId_NEW_BLOCK_66:                     struct{}{},
		sentryproto.MessageId_TRANSACTIONS_66:                  struct{}{},
		sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68: struct{}{},
		sentryproto.MessageId_GET_POOLED_TRANSACTIONS_66:       struct{}{},
		sentryproto.MessageId_POOLED_TRANSACTIONS_66:           struct{}{},
	},
	sentryproto.Protocol_WIT0: {
		sentryproto.MessageId_GET_BLOCK_WITNESS_W0:  struct{}{},
		sentryproto.MessageId_BLOCK_WITNESS_W0:      struct{}{},
		sentryproto.MessageId_NEW_WITNESS_W0:        struct{}{},
		sentryproto.MessageId_NEW_WITNESS_HASHES_W0: struct{}{},
	},
}
