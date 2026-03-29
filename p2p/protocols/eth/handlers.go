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
	"bytes"
	"context"
	"fmt"
	"math"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
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
				log.Warn("[p2p] GetBlockHeaders skip overflow attack", "current", current, "skip", query.Skip, "next", next)
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

type CachedReceipts struct {
	EncodedReceipts     []rlp.RawValue
	Bytes               int  // total size of the encoded receipts
	PendingIndex        int  // index of the first not-found receipt in the query
	LastBlockIncomplete bool // true if the final receipt list is partial (eth/70)
}

// encodeBlockReceipts69WithLimit encodes block receipts in eth/69 format (no Bloom),
// stopping when adding another receipt would push totalBytes over sizeLimit.
// Returns the encoded receipt list, its byte length, and whether all receipts were included.
func encodeBlockReceipts69WithLimit(receipts types.Receipts, totalBytes, sizeLimit int) (rlp.RawValue, int, bool, error) {
	if len(receipts) == 0 {
		encoded, _ := rlp.EncodeToBytes([]rlp.RawValue{})
		return encoded, len(encoded), true, nil
	}

	var perReceipt []rlp.RawValue
	contentSize := 0
	complete := true

	for _, r := range receipts {
		buf := &bytes.Buffer{}
		if err := r.EncodeRLP69(buf); err != nil {
			return nil, 0, false, err
		}
		encoded := buf.Bytes()

		// Calculate what the RLP list would be with this receipt added
		newContentSize := contentSize + len(encoded)
		newListSize := rlp.ListPrefixLen(newContentSize) + newContentSize

		if totalBytes+newListSize > sizeLimit && len(perReceipt) > 0 {
			complete = false
			break
		}

		perReceipt = append(perReceipt, encoded)
		contentSize = newContentSize
	}

	// Build the RLP list from per-receipt encodings
	encoded, _ := rlp.EncodeToBytes(perReceipt)
	return encoded, len(encoded), complete, nil
}

// NoSizeLimit disables per-receipt truncation (used for eth/68 and eth/69).
const NoSizeLimit = math.MaxInt

// ReceiptQueryOpts controls the behavior differences between eth protocol versions
// when answering GetReceipts queries.
type ReceiptQueryOpts struct {
	// EthVersion is the protocol version (e.g. direct.ETH68, direct.ETH69, direct.ETH70).
	// eth/69+ uses encoding without the Bloom field.
	EthVersion uint
	// FirstBlockReceiptIndex skips this many receipts from the first block (eth/70).
	FirstBlockReceiptIndex uint64
	// SizeLimit is the maximum encoded response size for per-receipt truncation.
	// Use NoSizeLimit to disable truncation (eth/68, eth/69).
	SizeLimit int
}

func AnswerGetReceiptsQueryCacheOnly(ctx context.Context, receiptsGetter ReceiptsGetter, query GetReceiptsPacket, opts ReceiptQueryOpts) (*CachedReceipts, bool, error) {
	var (
		numBytes     int
		pendingIndex int
		needMore     = true
	)
	receiptsList := make([]rlp.RawValue, 0, len(query))

	for lookups, hash := range query {
		if numBytes >= softResponseLimit || len(receiptsList) >= maxReceiptsServe ||
			lookups >= 2*maxReceiptsServe {
			needMore = false
			break
		}

		receipts, ok := receiptsGetter.GetCachedReceipts(ctx, hash)
		if !ok {
			break
		}

		// For the first block, skip receipts before firstBlockReceiptIndex (eth/70)
		if lookups == 0 && opts.FirstBlockReceiptIndex > 0 {
			if opts.FirstBlockReceiptIndex >= uint64(len(receipts)) {
				receipts = nil
			} else {
				receipts = receipts[opts.FirstBlockReceiptIndex:]
			}
		}

		encoded, encodedLen, complete, err := encodeBlockReceiptsWithLimit(receipts, numBytes, opts)
		if err != nil {
			return nil, false, err
		}
		receiptsList = append(receiptsList, encoded)
		numBytes += encodedLen
		pendingIndex = lookups + 1

		if !complete {
			return &CachedReceipts{
				EncodedReceipts:     receiptsList,
				Bytes:               numBytes,
				PendingIndex:        pendingIndex,
				LastBlockIncomplete: true,
			}, false, nil
		}
	}
	if pendingIndex == len(query) {
		needMore = false
	}
	return &CachedReceipts{
		EncodedReceipts: receiptsList,
		Bytes:           numBytes,
		PendingIndex:    pendingIndex,
	}, needMore, nil
}

// encodeBlockReceiptsWithLimit encodes a block's receipts according to the protocol version.
// For eth/69+ it uses eth/69 encoding (no Bloom) with optional per-receipt size limiting.
// For eth/68 it uses standard RLP encoding.
func encodeBlockReceiptsWithLimit(receipts types.Receipts, totalBytes int, opts ReceiptQueryOpts) (rlp.RawValue, int, bool, error) {
	if opts.EthVersion >= 69 {
		return encodeBlockReceipts69WithLimit(receipts, totalBytes, opts.SizeLimit)
	}
	// eth/68: standard RLP encoding, no size limiting
	encoded, err := rlp.EncodeToBytes(receipts)
	if err != nil {
		return nil, 0, false, fmt.Errorf("failed to encode receipt: %w", err)
	}
	return encoded, len(encoded), true, nil
}

func AnswerGetReceiptsQuery(ctx context.Context, cfg *chain.Config, receiptsGetter ReceiptsGetter, br services.HeaderAndBodyReader, db kv.TemporalTx, query GetReceiptsPacket, cached *CachedReceipts, opts ReceiptQueryOpts) ([]rlp.RawValue, bool, error) {
	var (
		numBytes     int
		receipts     []rlp.RawValue
		pendingIndex int
	)

	if cached != nil {
		numBytes = cached.Bytes
		receipts = cached.EncodedReceipts
		pendingIndex = cached.PendingIndex
	}

	for lookups := pendingIndex; lookups < len(query); lookups++ {
		hash := query[lookups]
		if numBytes >= softResponseLimit || len(receipts) >= maxReceiptsServe ||
			lookups >= 2*maxReceiptsServe {
			break
		}
		number, _ := br.HeaderNumber(context.Background(), db, hash)
		if number == nil {
			return nil, false, nil
		}
		b, _, err := br.BlockWithSenders(context.Background(), db, hash, *number)
		if err != nil {
			return nil, false, err
		}
		if b == nil {
			return nil, false, nil
		}

		results, err := receiptsGetter.GetReceipts(ctx, cfg, db, b)
		if err != nil {
			return nil, false, err
		}

		if results == nil {
			header, err := rawdb.ReadHeaderByHash(db, hash)
			if err != nil {
				return nil, false, err
			}
			if header == nil || header.ReceiptHash != empty.RootHash {
				continue
			}
		}

		// For the first block, skip receipts before firstBlockReceiptIndex (eth/70)
		if lookups == 0 && opts.FirstBlockReceiptIndex > 0 && results != nil {
			if opts.FirstBlockReceiptIndex >= uint64(len(results)) {
				results = nil
			} else {
				results = results[opts.FirstBlockReceiptIndex:]
			}
		}

		encoded, encodedLen, complete, err := encodeBlockReceiptsWithLimit(results, numBytes, opts)
		if err != nil {
			return nil, false, err
		}
		receipts = append(receipts, encoded)
		numBytes += encodedLen

		if !complete {
			return receipts, true, nil
		}
	}
	return receipts, false, nil
}
