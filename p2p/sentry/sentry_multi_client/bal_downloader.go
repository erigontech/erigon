// Copyright 2026 The Erigon Authors
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

package sentry_multi_client

import (
	"context"
	"encoding/hex"
	"errors"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

// BALDownloader runs a background loop that fills the local rawdb with Block
// Access Lists (EIP-7928) for recent blocks whose header commits to a BAL hash
// but whose BAL is not yet stored locally. It queries any connected peer that
// negotiated eth/71 (EIP-8159).
//
// The downloader is always-on — there is no feature flag. Same rollout model
// as eth/70: capability negotiation determines whether any work actually
// happens. If no peer advertises eth/71, every scan pass is a quick no-op;
// once at least one peer negotiates eth/71, missing BALs start flowing in.
//
// The block executor already regenerates and validates BALs locally via
// ProcessBAL, so a missing p2p-delivered BAL is never a correctness issue —
// only a CPU-cost optimisation. That is why this runs strictly in the
// background and never blocks stage progress.
type BALDownloader struct {
	mc     *MultiClient
	rwDB   kv.RwDB
	logger log.Logger

	// scanDepth is how many blocks back from head to scan on each pass. Keep
	// bounded so a large catch-up phase doesn't produce a scan of the whole
	// chain every tick.
	scanDepth uint64

	// scanInterval is the wall-clock gap between scan passes.
	scanInterval time.Duration

	// maxConcurrent bounds parallel GetBlockAccessLists fetches. BAL responses
	// are capped at softResponseLimit (2 MiB) each; 4 is enough throughput
	// without drowning a slow peer.
	maxConcurrent int
}

// NewBALDownloader constructs a downloader bound to the given MultiClient.
// rwDB is the chain DB used to persist fetched BALs (the MultiClient's own
// db field is read-only). Does not start the background loop; caller invokes
// Run(ctx).
func NewBALDownloader(mc *MultiClient, rwDB kv.RwDB, logger log.Logger) *BALDownloader {
	return &BALDownloader{
		mc:            mc,
		rwDB:          rwDB,
		logger:        logger,
		scanDepth:     256,
		scanInterval:  10 * time.Second,
		maxConcurrent: 4,
	}
}

// Run blocks until ctx is cancelled, firing a scan+fetch pass every
// scanInterval. Safe to call from a fresh goroutine.
func (d *BALDownloader) Run(ctx context.Context) {
	// Small initial delay so the sentries are connected and peers negotiated
	// before the first pass. Avoids a noisy "no peers" round on startup.
	select {
	case <-ctx.Done():
		return
	case <-time.After(15 * time.Second):
	}

	ticker := time.NewTicker(d.scanInterval)
	defer ticker.Stop()
	for {
		if err := d.scanAndFetch(ctx); err != nil && !errors.Is(err, context.Canceled) {
			d.logger.Debug("[bal-downloader] scan pass error", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// scanAndFetch walks back from head up to scanDepth blocks, collects the
// hashes of blocks whose header commits to a BAL but whose BAL is not in
// rawdb, and fetches them in parallel (bounded by maxConcurrent) from any
// eth/71 peer. Missing / failed entries are silently left for a later pass.
func (d *BALDownloader) scanAndFetch(ctx context.Context) error {
	peer, found, err := d.pickEth71Peer(ctx)
	if err != nil {
		return err
	}
	if !found {
		return nil // no eth/71 peers connected; nothing to do
	}

	missing, err := d.collectMissingBALs(ctx)
	if err != nil {
		return err
	}
	if len(missing) == 0 {
		return nil
	}

	d.logger.Debug("[bal-downloader] scan complete", "missing", len(missing), "peer", hex.EncodeToString(peer[:8]))

	// Fetch in batches. Each batch shares one GetBlockAccessLists round trip.
	const batchSize = 32
	sem := make(chan struct{}, d.maxConcurrent)
	var wg sync.WaitGroup
	for i := 0; i < len(missing); i += batchSize {
		end := i + batchSize
		if end > len(missing) {
			end = len(missing)
		}
		batch := missing[i:end]

		select {
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		case sem <- struct{}{}:
		}

		wg.Add(1)
		go func(batch []missingBAL) {
			defer wg.Done()
			defer func() { <-sem }()
			d.fetchBatch(ctx, peer, batch)
		}(batch)
	}
	wg.Wait()
	return nil
}

// missingBAL holds the info needed to fetch and persist one BAL.
type missingBAL struct {
	hash     common.Hash
	number   uint64
	expected common.Hash // header.BlockAccessListHash
}

// collectMissingBALs scans head..head-scanDepth and returns the subset whose
// BlockAccessListHash is non-nil and whose BAL is not currently in rawdb.
func (d *BALDownloader) collectMissingBALs(ctx context.Context) ([]missingBAL, error) {
	var missing []missingBAL
	err := d.mc.db.View(ctx, func(tx kv.Tx) error {
		head, err := d.mc.blockReader.CurrentBlock(tx)
		if err != nil || head == nil {
			return err
		}
		headNum := head.NumberU64()
		var low uint64
		if headNum > d.scanDepth {
			low = headNum - d.scanDepth
		}
		for n := headNum; n >= low; n-- {
			hdr, err := d.mc.blockReader.HeaderByNumber(ctx, tx, n)
			if err != nil {
				return err
			}
			if hdr == nil {
				break
			}
			if hdr.BlockAccessListHash == nil {
				// Pre-Amsterdam blocks — walk further back is pointless.
				break
			}
			hash := hdr.Hash()
			existing, _ := rawdb.ReadBlockAccessListBytes(tx, hash, n)
			if len(existing) > 0 {
				continue
			}
			missing = append(missing, missingBAL{
				hash:     hash,
				number:   n,
				expected: *hdr.BlockAccessListHash,
			})
			if n == 0 {
				break
			}
		}
		return nil
	})
	return missing, err
}

// fetchBatch issues a single GetBlockAccessLists for the batch's hashes and
// writes any validated returns to rawdb. Individual fetch failures are
// logged at debug and do not propagate — the next scan pass will retry.
func (d *BALDownloader) fetchBatch(ctx context.Context, peer [64]byte, batch []missingBAL) {
	hashes := make([]common.Hash, len(batch))
	expected := make([]common.Hash, len(batch))
	for i, m := range batch {
		hashes[i] = m.hash
		expected[i] = m.expected
	}

	got, err := d.mc.FetchBlockAccessLists(ctx, peer, hashes, expected)
	if err != nil {
		d.logger.Debug("[bal-downloader] fetch failed",
			"err", err,
			"peer", hex.EncodeToString(peer[:8]),
			"batch_size", len(batch),
			"from_block", batch[0].number,
			"to_block", batch[len(batch)-1].number,
		)
		return
	}

	// Write accepted entries. The fetcher already decoded EIP-8159 (post
	// ethereum/EIPs#11553) sentinels: nil = "peer doesn't have it" (was 0x80
	// on the wire) — skip and retry next pass from another peer; {0xc0} =
	// "genuinely empty BAL, hash-verified" — write the canonical RLP so
	// callers that distinguish "have" vs "don't have" via rawdb see the
	// record; anything else = hash-validated BAL bytes.
	var stored int
	if err := d.rwDB.Update(ctx, func(tx kv.RwTx) error {
		for i, payload := range got {
			if len(payload) == 0 {
				continue
			}
			if err := rawdb.WriteBlockAccessListBytes(tx, batch[i].hash, batch[i].number, payload); err != nil {
				return err
			}
			stored++
		}
		return nil
	}); err != nil {
		d.logger.Debug("[bal-downloader] db write failed", "err", err, "batch_size", len(batch))
		return
	}

	if stored > 0 {
		d.logger.Info("[bal-downloader] stored BALs",
			"count", stored,
			"peer", hex.EncodeToString(peer[:8]),
			"from_block", batch[0].number,
			"to_block", batch[len(batch)-1].number,
		)
	}
}

// pickEth71Peer iterates all sentries and returns a random peer that
// advertises the eth/71 capability, or (_, false, nil) if none is connected.
// Transient RPC errors are folded into "no peer found" rather than surfaced,
// because the caller retries each scan interval.
func (d *BALDownloader) pickEth71Peer(ctx context.Context) ([64]byte, bool, error) {
	type candidate struct {
		peerID  [64]byte
		sentryI int
	}
	var cands []candidate
	for i, sc := range d.mc.sentries {
		if ready, ok := sc.(interface{ Ready() bool }); ok && !ready.Ready() {
			continue
		}
		reply, err := sc.Peers(ctx, &emptypb.Empty{}, grpc.EmptyCallOption{})
		if err != nil {
			continue
		}
		for _, p := range reply.GetPeers() {
			if !peerSupportsEth71(p) {
				continue
			}
			raw, err := hex.DecodeString(p.GetId())
			if err != nil || len(raw) != 64 {
				continue
			}
			var pid [64]byte
			copy(pid[:], raw)
			cands = append(cands, candidate{peerID: pid, sentryI: i})
		}
	}
	if len(cands) == 0 {
		return [64]byte{}, false, nil
	}
	c := cands[rand.Intn(len(cands))] //nolint:gosec
	return c.peerID, true, nil
}

// peerSupportsEth71 checks the peer's advertised capabilities for "eth/71".
// Caps format is "proto/version" per go-ethereum convention (see caps
// assembled in p2p.Protocol). We match exact string.
func peerSupportsEth71(p *typesproto.PeerInfo) bool {
	for _, cap := range p.GetCaps() {
		if cap == "eth/71" {
			return true
		}
	}
	return false
}
