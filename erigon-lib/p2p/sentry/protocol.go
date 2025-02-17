package sentry

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
}
