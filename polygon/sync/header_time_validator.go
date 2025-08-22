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

package sync

import (
	"context"
	"fmt"
	"time"

	lru "github.com/hashicorp/golang-lru/arc/v2"
	"github.com/jellydator/ttlcache/v3"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/polygon/bor"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
)

var VeBlopBlockTimeOut = 4 * time.Second // timeout for waiting for a new span in case main producer is down
var RecentHeadersCapacity uint64 = 4096

type HeaderTimeValidator struct {
	borConfig             *borcfg.BorConfig
	signaturesCache       *lru.ARCCache[libcommon.Hash, libcommon.Address]
	recentVerifiedHeaders *ttlcache.Cache[libcommon.Hash, *types.Header]
	blockProducersReader  blockProducersReader
	logger                log.Logger
}

func (htv *HeaderTimeValidator) ValidateHeaderTime(
	ctx context.Context,
	header *types.Header,
	now time.Time,
	parent *types.Header,
) error {
	headerNum := header.Number.Uint64()
	producers, err := htv.blockProducersReader.Producers(ctx, headerNum)
	if err != nil {
		return err
	}
	signer, err := bor.Ecrecover(header, htv.signaturesCache, htv.borConfig)
	if err != nil {
		return err
	}
	htv.logger.Debug("VALIDATING_HEADER_TIME:", "blockNum", header.Number.Uint64(), "blockHash", header.Hash(), "parentHash", parent.Hash(), "signer", signer, "producers", producers)

	// VeBlop checks for new span if block signer is different from producer
	if htv.borConfig.IsVeBlop(header.Number.Uint64()) {
		if len(producers.Validators) != 1 {
			return fmt.Errorf("unexpected number of producers post VeBlop (expected 1 producer) , blockNum=%d , numProducers=%d", header.Number.Uint64(), len(producers.Validators))
		}
		producer := producers.Validators[0]
		shouldWaitForNewSpans, timeout, err := htv.needToWaitForNewSpan(header, parent, producer.Address)
		if err != nil {
			return fmt.Errorf("needToWaitForNewSpan failed for blockNum=%d with %w", header.Number.Uint64(), err)
		}
		if shouldWaitForNewSpans && timeout > 0 {
			// we just need to wait without triggering any explicit synchronization, since the span fetcher is fetching
			// new spans every 1 second, which should be enough for the new spans to be observed
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(timeout): // no need to do anything yet, just wait

			}
			// after giving enought time for new spans to be observed we can now calculate the updated producer set
			producers, err = htv.blockProducersReader.Producers(ctx, headerNum)
			if err != nil {
				return err
			}
		}
	}
	err = bor.ValidateHeaderTime(header, now, parent, producers, htv.borConfig, htv.signaturesCache)
	if err != nil {
		return err
	}
	// Header time has been validated, therefore save this header to TTL
	htv.logger.Debug("VALIDATED_HEADER_TIME:", "blockNum", header.Number.Uint64(), "blockHash", header.Hash(), "parentHash", parent.Hash(), "signer", signer, "producers", producers)
	htv.UpdateLatestVerifiedHeader(header)
	return nil
}

func (htv *HeaderTimeValidator) UpdateLatestVerifiedHeader(header *types.Header) {
	htv.recentVerifiedHeaders.Set(header.Hash(), header, ttlcache.DefaultTTL)
}

// check if the conditions are met where we need to await for a span rotation
// If yes, then the second return value contains how long to wait for new spans to be fetched (this can vary)
func (htv *HeaderTimeValidator) needToWaitForNewSpan(header *types.Header, parent *types.Header, producer libcommon.Address) (bool, time.Duration, error) {
	author, err := bor.Ecrecover(header, htv.signaturesCache, htv.borConfig)
	if err != nil {
		return false, 0, err
	}
	parentAuthor, err := bor.Ecrecover(parent, htv.signaturesCache, htv.borConfig)
	if err != nil {
		return false, 0, err
	}
	headerNum := header.Number.Uint64()
	// the current producer has published a block, but it came too late(i.e. the parent has been evicted from the ttl cache)
	if author == producer && author == parentAuthor && !htv.recentVerifiedHeaders.Has(header.ParentHash) { // (this could be due to a fork also)
		htv.logger.Info("[span-rotation] need to wait for span rotation due to longer than expected block time from current producer", "blockNum", headerNum, "parentHeader", header.ParentHash, "author", author)
		return true, 2 * time.Second, nil
	} else if author != parentAuthor && author != producer { // new author but not matching the producer for this block
		htv.logger.Info("[span-rotation] need to wait for span rotation because the new author does not match the producer from current producer selection",
			"blockNum", headerNum, "author", author, "producer", producer, "parentAuthor", parentAuthor)
		return true, 2 * time.Second, nil // this situation has a short delay because the
	}
	return false, 0, nil

}
