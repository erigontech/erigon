// Copyright 2025 The Erigon Authors
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

package integrity

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

// CheckDomainLatestVsHistory compares GetLatest (domain files) against GetAsOf
// (history) for all keys in a domain. Any mismatch indicates a missing or wrong
// entry in the domain files — the signature of the collation/pruning race (#20169)
// where a deletion was lost.
//
// Unlike verify-domain-values (which checks entries IN files), this check catches
// MISSING entries: a key that was deleted but has no deletion entry in the domain
// file chain, causing GetLatest to return a stale pre-deletion value.
func CheckDomainLatestVsHistory(ctx context.Context, domain kv.Domain, tx kv.TemporalTx, maxTxNum uint64, failFast bool, logger log.Logger) error {
	it, err := tx.Debug().RangeLatest(domain, nil, nil, -1)
	if err != nil {
		return fmt.Errorf("RangeLatest(%s): %w", domain, err)
	}

	var checked, mismatches int
	start := time.Now()
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()

	for it.HasNext() {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		k, latestVal, err := it.Next()
		if err != nil {
			return fmt.Errorf("RangeLatest(%s) iterate: %w", domain, err)
		}
		checked++

		histVal, _, err := tx.GetAsOf(domain, k, maxTxNum)
		if err != nil {
			return fmt.Errorf("GetAsOf(%s, %x): %w", domain, k[:min(len(k), 20)], err)
		}

		if !bytes.Equal(latestVal, histVal) {
			mismatches++
			logger.Warn("[integrity] DomainLatestVsHistory mismatch",
				"domain", domain,
				"key", hex.EncodeToString(k[:min(len(k), 20)])+"...",
				"latest_len", len(latestVal),
				"history_len", len(histVal),
				"latest_zero", len(latestVal) == 0,
				"history_zero", len(histVal) == 0,
			)
			if failFast {
				return fmt.Errorf("%w: domain %s key %x: GetLatest=%x GetAsOf=%x",
					ErrIntegrity, domain, k, latestVal, histVal)
			}
		}

		select {
		case <-logEvery.C:
			logger.Info("[integrity] DomainLatestVsHistory progress",
				"domain", domain,
				"checked", checked,
				"mismatches", mismatches,
				"elapsed", time.Since(start).Round(time.Second),
			)
		default:
		}
	}

	logger.Info("[integrity] DomainLatestVsHistory done",
		"domain", domain,
		"checked", checked,
		"mismatches", mismatches,
		"elapsed", time.Since(start).Round(time.Second),
	)
	if mismatches > 0 {
		return fmt.Errorf("%w: domain %s has %d mismatches between GetLatest and GetAsOf",
			ErrIntegrity, domain, mismatches)
	}
	return nil
}
