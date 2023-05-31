// Copyright 2020 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package eth

import (
	"context"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/services"
)

func AnswerGetBlockHeadersQuery(db kv.Tx, query *GetBlockHeadersPacket, blockReader services.HeaderAndCanonicalReader) ([]*types.Header, error) {
	hashMode := query.Origin.Hash != (libcommon.Hash{})
	first := true
	maxNonCanonical := uint64(100)

	// Gather headers until the fetch or network limits is reached
	var (
		bytes   common.StorageSize
		headers []*types.Header
		unknown bool
		err     error
		lookups int
	)

	for !unknown && len(headers) < int(query.Amount) && bytes < softResponseLimit &&
		len(headers) < MaxHeadersServe && lookups < 2*MaxHeadersServe {
		lookups++
		// Retrieve the next header satisfying the query
		var origin *types.Header
		if hashMode {
			if first {
				first = false
				origin, err = blockReader.HeaderByHash(context.Background(), db, query.Origin.Hash)
				if err != nil {
					return nil, err
				}
				if origin != nil {
					query.Origin.Number = origin.Number.Uint64()
				}
			} else {
				origin, err = blockReader.Header(context.Background(), db, query.Origin.Hash, query.Origin.Number)
				if err != nil {
					return nil, err
				}
			}
		} else {
			origin, err = blockReader.HeaderByNumber(context.Background(), db, query.Origin.Number)
			if err != nil {
				return nil, err
			}
		}
		if origin == nil {
			break
		}
		headers = append(headers, origin)
		bytes += estHeaderSize

		// Advance to the next header of the query
		switch {
		case hashMode && query.Reverse:
			// Hash based traversal towards the genesis block
			ancestor := query.Skip + 1
			if ancestor == 0 {
				unknown = true
			} else {
				query.Origin.Hash, query.Origin.Number = rawdb.ReadAncestor(db, query.Origin.Hash, query.Origin.Number, ancestor, &maxNonCanonical, blockReader)
				unknown = query.Origin.Hash == libcommon.Hash{}
			}
		case hashMode && !query.Reverse:
			// Hash based traversal towards the leaf block
			var (
				current = origin.Number.Uint64()
				next    = current + query.Skip + 1
			)
			if next <= current {
				//infos, _ := json.MarshalIndent(peer.Peer.Info(), "", "  ")
				//log.Warn("GetBlockHeaders skip overflow attack", "current", current, "skip", query.Skip, "next", next, "attacker", infos)
				log.Warn("GetBlockHeaders skip overflow attack", "current", current, "skip", query.Skip, "next", next)
				unknown = true
			} else {
				header, err := blockReader.HeaderByNumber(context.Background(), db, query.Origin.Number)
				if err != nil {
					return nil, err
				}
				if header != nil {
					nextHash := header.Hash()
					expOldHash, _ := rawdb.ReadAncestor(db, nextHash, next, query.Skip+1, &maxNonCanonical, blockReader)
					if expOldHash == query.Origin.Hash {
						query.Origin.Hash, query.Origin.Number = nextHash, next
					} else {
						unknown = true
					}
				} else {
					unknown = true
				}
			}
		case query.Reverse:
			// Number based traversal towards the genesis block
			if query.Origin.Number >= query.Skip+1 {
				query.Origin.Number -= query.Skip + 1
			} else {
				unknown = true
			}

		case !query.Reverse:
			// Number based traversal towards the leaf block
			query.Origin.Number += query.Skip + 1
		}
	}
	return headers, nil
}

func AnswerGetBlockBodiesQuery(db kv.Tx, query GetBlockBodiesPacket, blockReader services.FullBlockReader) []rlp.RawValue { //nolint:unparam
	// Gather blocks until the fetch or network limits is reached
	var bytes int
	bodies := make([]rlp.RawValue, 0, len(query))

	for lookups, hash := range query {
		if bytes >= softResponseLimit || len(bodies) >= MaxBodiesServe ||
			lookups >= 2*MaxBodiesServe {
			break
		}
		number := rawdb.ReadHeaderNumber(db, hash)
		if number == nil {
			continue
		}
		bodyRLP, _ := blockReader.BodyRlp(context.Background(), db, hash, *number)
		if len(bodyRLP) == 0 {
			continue
		}
		bodies = append(bodies, bodyRLP)
		bytes += len(bodyRLP)
	}
	return bodies
}

func AnswerGetReceiptsQuery(db kv.Tx, query GetReceiptsPacket) ([]rlp.RawValue, error) { //nolint:unparam
	// Gather state data until the fetch or network limits is reached
	var (
		bytes    int
		receipts []rlp.RawValue
	)
	for lookups, hash := range query {
		if bytes >= softResponseLimit || len(receipts) >= maxReceiptsServe ||
			lookups >= 2*maxReceiptsServe {
			break
		}
		// Retrieve the requested block's receipts
		number := rawdb.ReadHeaderNumber(db, hash)
		if number == nil {
			return nil, nil
		}
		block, senders, err := rawdb.ReadBlockWithSenders(db, hash, *number)
		if err != nil {
			return nil, err
		}
		results := rawdb.ReadReceipts(db, block, senders)
		if results == nil {
			header, err := rawdb.ReadHeaderByHash(db, hash)
			if err != nil {
				return nil, err
			}
			if header == nil || header.ReceiptHash != types.EmptyRootHash {
				continue
			}
		}
		// If known, encode and queue for response packet
		if encoded, err := rlp.EncodeToBytes(results); err != nil {
			return nil, fmt.Errorf("failed to encode receipt: %w", err)
		} else {
			receipts = append(receipts, encoded)
			bytes += len(encoded)
		}
	}
	return receipts, nil
}
