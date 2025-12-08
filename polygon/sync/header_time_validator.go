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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/polygon/bor"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	lru "github.com/hashicorp/golang-lru/arc/v2"
	"github.com/jellydator/ttlcache/v3"
)

var VeBlopNewSpanTimeout = 8 * time.Second     // timeout for waiting for a new span
var VeBlopBlockTimeout = 4 * time.Second       // time for a block to be considered late
var DefaultRecentHeadersCapacity uint64 = 4096 // capacity of recent headers TTL cache

type HeaderTimeValidator struct {
	borConfig             *borcfg.BorConfig
	signaturesCache       *lru.ARCCache[common.Hash, common.Address]
	recentVerifiedHeaders *ttlcache.Cache[common.Hash, *types.Header]
	blockProducersTracker blockProducersTracker
	logger                log.Logger
}

func (htv *HeaderTimeValidator) ValidateHeaderTime(
	ctx context.Context,
	header *types.Header,
	now time.Time,
	parent *types.Header,
) error {
	headerNum := header.Number.Uint64()
	producers, err := htv.blockProducersTracker.Producers(ctx, headerNum)
	if err != nil {
		return err
	}
	signer, err := bor.Ecrecover(header, htv.signaturesCache, htv.borConfig)
	if err != nil {
		return err
	}
	htv.logger.Debug("validating header time:", "blockNum", header.Number.Uint64(), "blockHash", header.Hash(), "parentHash", parent.Hash(), "signer", signer, "producers", producers)

	// Rio/VeBlop checks for new span if block signer is different from producer
	if htv.borConfig.IsRio(header.Number.Uint64()) {
		if len(producers.Validators) != 1 {
			return fmt.Errorf("unexpected number of producers post Rio (expected 1 producer) , blockNum=%d , numProducers=%d", header.Number.Uint64(), len(producers.Validators))
		}
		producer := producers.Validators[0]
		shouldWaitForNewSpans, timeout, err := htv.needToWaitForNewSpan(header, parent, producer.Address)
		if err != nil {
			return fmt.Errorf("needToWaitForNewSpan failed for blockNum=%d with %w", header.Number.Uint64(), err)
		}
		if shouldWaitForNewSpans && timeout > 0 {
			newSpanWasProcessed, err := htv.blockProducersTracker.AnticipateNewSpanWithTimeout(ctx, timeout)
			if err != nil {
				return err
			}
			if newSpanWasProcessed {
				htv.logger.Info("[span-rotation] producer set was updated")
			} else {
				htv.logger.Info(fmt.Sprintf("[span-rotation] producer set was not updated within %.0f seconds", timeout.Seconds()))
			}
			// after giving enough time for new span to be observed we can now calculate the updated producer set
			producers, err = htv.blockProducersTracker.Producers(ctx, headerNum)
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
	htv.logger.Debug("validated header time:", "blockNum", header.Number.Uint64(), "blockHash", header.Hash(), "parentHash", parent.Hash(), "signer", signer, "producers", producers.ValidatorAddresses())
	htv.UpdateLatestVerifiedHeader(header)
	return nil
}

func (htv *HeaderTimeValidator) UpdateLatestVerifiedHeader(header *types.Header) {
	htv.recentVerifiedHeaders.Set(header.Hash(), header, ttlcache.DefaultTTL)
}

// check if the conditions are met where we need to await for a span rotation
// If yes, then the second return value contains how long to wait for new spans to be fetched (this can vary)
func (htv *HeaderTimeValidator) needToWaitForNewSpan(header *types.Header, parent *types.Header, producer common.Address) (bool, time.Duration, error) {
	author, err := bor.Ecrecover(header, htv.signaturesCache, htv.borConfig)
	if err != nil {
		return false, 0, err
	}
	parentAuthor, err := bor.Ecrecover(parent, htv.signaturesCache, htv.borConfig)
	if err != nil {
		return false, 0, err
	}
	headerNum := header.Number.Uint64()
	// the current producer has published a block, but it came too late (i.e. the parent has been evicted from the ttl cache)
	if author == producer && producer == parentAuthor && !htv.recentVerifiedHeaders.Has(header.ParentHash) {
		htv.logger.Info("[span-rotation] need to wait for span rotation due to longer than expected block time from current producer", "blockNum", headerNum, "parentHeader", header.ParentHash, "author", author)
		return true, VeBlopNewSpanTimeout, nil
	}
	return false, 0, nil

}
