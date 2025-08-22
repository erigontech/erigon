// Copyright 2020 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package eth

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/empty"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/turbo/services"
)

func AnswerGetBlockHeadersQuery(db kv.Tx, query *GetBlockHeadersPacket, blockReader services.HeaderReader) ([]*types.Header, error) {
	hashMode := query.Origin.Hash != (common.Hash{})
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
				query.Origin.Hash, query.Origin.Number = blockReader.ReadAncestor(db, query.Origin.Hash, query.Origin.Number, ancestor, &maxNonCanonical)
				unknown = query.Origin.Hash == common.Hash{}
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
					expOldHash, _ := blockReader.ReadAncestor(db, nextHash, next, query.Skip+1, &maxNonCanonical)
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
			current := query.Origin.Number
			ancestor := current - (query.Skip + 1)
			if ancestor >= current { // check for underflow
				unknown = true
			} else {
				query.Origin.Number = ancestor
			}

		case !query.Reverse:
			current := query.Origin.Number
			next := current + query.Skip + 1
			if next <= current { // check for overflow
				unknown = true
			} else {
				query.Origin.Number = next
			}
		}
	}
	return headers, nil
}

func AnswerGetBlockBodiesQuery(db kv.Tx, query GetBlockBodiesPacket, blockReader services.HeaderAndBodyReader) []rlp.RawValue { //nolint:unparam
	// Gather blocks until the fetch or network limits is reached
	var bytes int
	bodies := make([]rlp.RawValue, 0, len(query))

	for lookups, hash := range query {
		if bytes >= softResponseLimit || len(bodies) >= MaxBodiesServe ||
			lookups >= 2*MaxBodiesServe {
			break
		}
		number, _ := blockReader.HeaderNumber(context.Background(), db, hash)
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

type ReceiptsGetter interface {
	GetReceipts(ctx context.Context, cfg *chain.Config, tx kv.TemporalTx, block *types.Block) (types.Receipts, error)
	GetCachedReceipts(ctx context.Context, blockHash common.Hash) (types.Receipts, bool)
}

type cachedReceipts struct {
	EncodedReceipts []rlp.RawValue
	Bytes           int // total size of the encoded receipts
	PendingIndex    int // index of the first not-found receipt in the query
}

func AnswerGetReceiptsQueryCacheOnly(ctx context.Context, receiptsGetter ReceiptsGetter, query GetReceiptsPacket) (*cachedReceipts, bool, error) {
	var (
		bytes        int
		receiptsList []rlp.RawValue
		pendingIndex int
		needMore     = true
	)

	for lookups, hash := range query {
		if bytes >= softResponseLimit || len(receiptsList) >= maxReceiptsServe ||
			lookups >= 2*maxReceiptsServe {
			needMore = false
			break
		}
		if receipts, ok := receiptsGetter.GetCachedReceipts(ctx, hash); ok {
			if encoded, err := rlp.EncodeToBytes(receipts); err != nil {
				return nil, needMore, fmt.Errorf("failed to encode receipt: %w", err)
			} else {
				receiptsList = append(receiptsList, encoded)
				bytes += len(encoded)
				pendingIndex = lookups + 1
			}
		} else {
			break
		}
	}
	if pendingIndex == len(query) {
		needMore = false
	}
	return &cachedReceipts{
		EncodedReceipts: receiptsList,
		Bytes:           bytes,
		PendingIndex:    pendingIndex,
	}, needMore, nil
}

func AnswerGetReceiptsQuery(ctx context.Context, cfg *chain.Config, receiptsGetter ReceiptsGetter, br services.HeaderAndBodyReader, db kv.TemporalTx, query GetReceiptsPacket, cachedReceipts *cachedReceipts) ([]rlp.RawValue, error) { //nolint:unparam
	// Gather state data until the fetch or network limits is reached
	var (
		bytes        int
		receipts     []rlp.RawValue
		pendingIndex int
	)

	if cachedReceipts != nil {
		bytes = cachedReceipts.Bytes
		receipts = cachedReceipts.EncodedReceipts
		pendingIndex = cachedReceipts.PendingIndex
	}

	for lookups := pendingIndex; lookups < len(query); lookups++ {
		hash := query[lookups]
		if bytes >= softResponseLimit || len(receipts) >= maxReceiptsServe ||
			lookups >= 2*maxReceiptsServe {
			break
		}
		number, _ := br.HeaderNumber(context.Background(), db, hash)
		if number == nil {
			return nil, nil
		}
		// Retrieve the requested block's receipts
		b, _, err := br.BlockWithSenders(context.Background(), db, hash, *number)
		if err != nil {
			return nil, err
		}
		if b == nil {
			return nil, nil
		}

		results, err := receiptsGetter.GetReceipts(ctx, cfg, db, b)
		if err != nil {
			return nil, err
		}

		if results == nil {
			header, err := rawdb.ReadHeaderByHash(db, hash)
			if err != nil {
				return nil, err
			}
			if header == nil || header.ReceiptHash != empty.RootHash {
				continue
			}
		}
		// For debug
		//println("receipts:")
		//for _, result := range results {
		//	println(result.String())
		//}

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
