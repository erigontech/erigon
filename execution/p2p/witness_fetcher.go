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

package p2p

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/p2p/protocols/wit"
)

// WitnessBuffer collects witnesses received from peers for later processing.
type WitnessBuffer interface {
	AddWitness(blockNumber uint64, blockHash common.Hash, data []byte)
}

func NewWitnessFetcher(
	logger log.Logger,
	messageListener *MessageListener,
	messageSender *MessageSender,
	db kv.TemporalRoDB,
	blockReader services.FullBlockReader,
	witnessBuffer WitnessBuffer,
) *WitnessFetcher {
	f := &WitnessFetcher{
		logger:          logger,
		messageSender:   messageSender,
		db:              db,
		blockReader:     blockReader,
		witnessBuffer:   witnessBuffer,
		newWitnessTasks: make(chan *DecodedInboundMessage[*wit.NewWitnessPacket], responderTaskQueueSize),
		witnessTasks:    make(chan *DecodedInboundMessage[*wit.WitnessPacketRLPPacket], responderTaskQueueSize),
	}
	messageListener.RegisterNewWitnessObserver(func(message *DecodedInboundMessage[*wit.NewWitnessPacket]) {
		enqueueResponderTask(logger, witnessFetcherLogPrefix, f.newWitnessTasks, message)
	})
	messageListener.RegisterWitnessObserver(func(message *DecodedInboundMessage[*wit.WitnessPacketRLPPacket]) {
		enqueueResponderTask(logger, witnessFetcherLogPrefix, f.witnessTasks, message)
	})
	return f
}

// WitnessFetcher accumulates witnesses peers push to us (NewWitness) and witness
// pages we requested (Witness responses) into a WitnessBuffer, re-requesting any
// pages still missing.
type WitnessFetcher struct {
	logger          log.Logger
	messageSender   *MessageSender
	db              kv.TemporalRoDB
	blockReader     services.FullBlockReader
	witnessBuffer   WitnessBuffer
	newWitnessTasks chan *DecodedInboundMessage[*wit.NewWitnessPacket]
	witnessTasks    chan *DecodedInboundMessage[*wit.WitnessPacketRLPPacket]
}

func (f *WitnessFetcher) Run(ctx context.Context) error {
	f.logger.Info(witnessFetcherLogPrefix + " running witness fetcher component")
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return processResponderTasks(ctx, f.newWitnessTasks, f.handleNewWitness, f.logger, witnessFetcherLogPrefix)
	})
	eg.Go(func() error {
		return processResponderTasks(ctx, f.witnessTasks, f.handleWitness, f.logger, witnessFetcherLogPrefix)
	})
	return eg.Wait()
}

func (f *WitnessFetcher) handleNewWitness(ctx context.Context, message *DecodedInboundMessage[*wit.NewWitnessPacket]) error {
	witness := message.Decoded.Witness
	blockHash := witness.Header().Hash()
	var witBuf bytes.Buffer
	err := witness.EncodeRLP(&witBuf)
	if err != nil {
		return fmt.Errorf("error in witness encoding: err: %w", err)
	}
	f.witnessBuffer.AddWitness(witness.Header().Number.Uint64(), blockHash, witBuf.Bytes())
	return nil
}

// handleWitness processes a response to our GetWitness request: it reconstructs
// complete witnesses from their pages and re-requests any missing pages.
func (f *WitnessFetcher) handleWitness(ctx context.Context, message *DecodedInboundMessage[*wit.WitnessPacketRLPPacket]) error {
	tx, err := f.db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// group witness pages by hash to reconstruct complete witnesses
	witnessPages := make(map[common.Hash]map[uint64][]byte)
	witnessTotalPages := make(map[common.Hash]uint64)
	for _, pageResponse := range message.Decoded.WitnessPacketResponse {
		if pageResponse.TotalPages > wit.MaxWitnessPages {
			return fmt.Errorf("witness response advertises TotalPages %d > max %d for hash %x", pageResponse.TotalPages, wit.MaxWitnessPages, pageResponse.Hash)
		}
		// Page >= TotalPages is the empty-response sentinel documented on
		// wit.WitnessPageResponse.Page; skip without allocating.
		if pageResponse.Page >= pageResponse.TotalPages {
			continue
		}
		if prev, ok := witnessTotalPages[pageResponse.Hash]; ok && prev != pageResponse.TotalPages {
			return fmt.Errorf("witness response has inconsistent TotalPages for hash %x: %d vs %d", pageResponse.Hash, prev, pageResponse.TotalPages)
		}
		if witnessPages[pageResponse.Hash] == nil {
			witnessPages[pageResponse.Hash] = make(map[uint64][]byte)
		}
		witnessPages[pageResponse.Hash][pageResponse.Page] = pageResponse.Data
		witnessTotalPages[pageResponse.Hash] = pageResponse.TotalPages
	}
	// reconstruct witnesses
	for witnessHash, pages := range witnessPages {
		totalPages := witnessTotalPages[witnessHash]
		if uint64(len(pages)) != totalPages {
			f.requestMissingPages(ctx, message.PeerId, witnessHash, pages, totalPages)
			continue
		}
		header, err := f.blockReader.HeaderByHash(ctx, tx, witnessHash)
		if err != nil {
			return fmt.Errorf("reading header for witness hash %x: %w", witnessHash, err)
		}
		if header == nil {
			f.logger.Debug(witnessFetcherLogPrefix+" header not found for witness", "hash", witnessHash)
			continue
		}
		// reconstruct complete witness data by concatenating pages in order
		var completeWitness []byte
		complete := true
		for page := uint64(0); page < totalPages; page++ {
			pageData, exists := pages[page]
			if !exists {
				f.logger.Debug(witnessFetcherLogPrefix+" missing page in witness", "hash", witnessHash, "page", page)
				complete = false
				break
			}
			completeWitness = append(completeWitness, pageData...)
		}
		if complete {
			f.witnessBuffer.AddWitness(header.Number.Uint64(), witnessHash, completeWitness)
		}
	}
	return nil
}

func (f *WitnessFetcher) requestMissingPages(ctx context.Context, peerId *PeerId, witnessHash common.Hash, pages map[uint64][]byte, totalPages uint64) {
	var missingPages []uint64
	for page := uint64(0); page < totalPages; page++ {
		if _, exists := pages[page]; !exists {
			missingPages = append(missingPages, page)
		}
	}
	if len(missingPages) == 0 {
		return
	}
	witnessPageRequests := make([]wit.WitnessPageRequest, len(missingPages))
	for i, page := range missingPages {
		witnessPageRequests[i] = wit.WitnessPageRequest{
			Hash: witnessHash,
			Page: page,
		}
	}
	getWitnessReq := wit.GetWitnessPacket{
		RequestId: rand.Uint64(), //nolint:gosec // request id does not need crypto-grade randomness
		GetWitnessRequest: &wit.GetWitnessRequest{
			WitnessPages: witnessPageRequests,
		},
	}
	// send request for missing pages to the same peer
	err := f.messageSender.SendGetWitness(ctx, peerId, getWitnessReq)
	if err == nil {
		f.logger.Info(witnessFetcherLogPrefix+" requested missing witness pages from original peer", "hash", witnessHash, "missing_pages", missingPages, "peer", peerId)
		return
	}
	// if sending to the specific peer fails, try random peers as fallback
	// TODO: instead of sending to random peers, add new function to send to peers known to have witness
	f.logger.Info(witnessFetcherLogPrefix+" failed to send GetWitnessMsg to original peer, trying random peers", "err", err, "hash", witnessHash)
	err = f.messageSender.SendGetWitnessToRandomPeers(ctx, 1, getWitnessReq)
	if err != nil {
		f.logger.Warn(witnessFetcherLogPrefix+" failed to send GetWitnessMsg for missing pages to any peer", "err", err, "hash", witnessHash)
		return
	}
	f.logger.Info(witnessFetcherLogPrefix+" requested missing witness pages via random peer", "hash", witnessHash, "missing_pages", missingPages)
}

const witnessFetcherLogPrefix = "[p2p.witness.fetcher]"
