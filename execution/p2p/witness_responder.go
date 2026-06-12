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
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/p2p/protocols/wit"
)

func NewWitnessResponder(
	logger log.Logger,
	messageListener *MessageListener,
	publisher *WitnessPublisher,
	db kv.TemporalRoDB,
	blockReader services.FullBlockReader,
) *WitnessResponder {
	r := &WitnessResponder{
		logger:      logger,
		publisher:   publisher,
		db:          db,
		blockReader: blockReader,
		tasks:       make(chan *DecodedInboundMessage[*wit.GetWitnessPacket], responderTaskQueueSize),
	}
	messageListener.RegisterGetWitnessObserver(func(message *DecodedInboundMessage[*wit.GetWitnessPacket]) {
		enqueueResponderTask(logger, witnessResponderLogPrefix, r.tasks, message)
	})
	return r
}

// WitnessResponder answers inbound wit/0 GetWitness requests by serving the
// requested witness pages from the database.
type WitnessResponder struct {
	logger      log.Logger
	publisher   *WitnessPublisher
	db          kv.TemporalRoDB
	blockReader services.FullBlockReader
	tasks       chan *DecodedInboundMessage[*wit.GetWitnessPacket]
}

func (r *WitnessResponder) Run(ctx context.Context) error {
	r.logger.Info(witnessResponderLogPrefix + " running witness responder component")
	return processResponderTasks(ctx, r.tasks, r.handleGetWitness, r.logger, witnessResponderLogPrefix)
}

func (r *WitnessResponder) handleGetWitness(ctx context.Context, message *DecodedInboundMessage[*wit.GetWitnessPacket]) error {
	req := message.Decoded
	tx, err := r.db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	seen := make(map[common.Hash]struct{}, len(req.WitnessPages))
	for _, witnessPage := range req.WitnessPages {
		seen[witnessPage.Hash] = struct{}{}
	}
	witnessSize := make(map[common.Hash]uint64, len(seen))
	headers := make(map[common.Hash]*types.Header, len(seen))
	for witnessBlockHash := range seen {
		header, err := r.blockReader.HeaderByHash(ctx, tx, witnessBlockHash)
		if err != nil {
			return fmt.Errorf("reading header for witness hash %x: %w", witnessBlockHash, err)
		}
		if header == nil {
			continue
		}
		headers[witnessBlockHash] = header
		key := dbutils.HeaderKey(header.Number.Uint64(), witnessBlockHash)
		sizeBytes, err := tx.GetOne(kv.BorWitnessSizes, key)
		if err != nil {
			return fmt.Errorf("reading witness size for hash %x: %w", witnessBlockHash, err)
		}
		if len(sizeBytes) > 0 {
			witnessSize[witnessBlockHash] = binary.BigEndian.Uint64(sizeBytes)
		} else {
			witnessSize[witnessBlockHash] = 0
		}
	}
	var response wit.WitnessPacketResponse
	witnessCache := make(map[common.Hash][]byte, len(seen))
	totalResponsePayloadDataAmount := 0
	totalCached := 0
	for _, witnessPage := range req.WitnessPages {
		size := witnessSize[witnessPage.Hash]
		totalPages := (size + wit.PageSize - 1) / wit.PageSize // Ceiling division
		var witnessPageResponse wit.WitnessPageResponse
		witnessPageResponse.Page = witnessPage.Page
		witnessPageResponse.Hash = witnessPage.Hash
		witnessPageResponse.TotalPages = totalPages
		if witnessPage.Page < totalPages {
			var witnessBytes []byte
			if cachedRLPBytes, exists := witnessCache[witnessPage.Hash]; exists {
				witnessBytes = cachedRLPBytes
			} else {
				header, ok := headers[witnessPage.Hash]
				if !ok || header == nil {
					continue
				}
				key := dbutils.HeaderKey(header.Number.Uint64(), witnessPage.Hash)
				queriedBytes, err := tx.GetOne(kv.BorWitnesses, key)
				if err != nil {
					return fmt.Errorf("reading witness for hash %x: %w", witnessPage.Hash, err)
				}
				witnessCache[witnessPage.Hash] = queriedBytes
				witnessBytes = queriedBytes
				totalCached += len(queriedBytes)
			}
			start := min(wit.PageSize*witnessPage.Page, uint64(len(witnessBytes)))
			end := min(start+wit.PageSize, uint64(len(witnessBytes)))
			witnessPageResponse.Data = witnessBytes[start:end]
			totalResponsePayloadDataAmount += len(witnessPageResponse.Data)
		}
		response = append(response, witnessPageResponse)
		// fast fail check
		if totalCached >= wit.MaximumCachedWitnessOnARequest {
			return fmt.Errorf("request demands too much memory: %d bytes", totalCached)
		}
		// memory protection check
		if totalResponsePayloadDataAmount >= wit.MaximumResponseSize {
			return fmt.Errorf("response exceeds maximum p2p payload size: %d bytes", totalResponsePayloadDataAmount)
		}
	}
	err = r.publisher.PublishWitness(ctx, message.PeerId, wit.WitnessPacketRLPPacket{
		RequestId:             req.RequestId,
		WitnessPacketResponse: response,
	})
	if err != nil && !errors.Is(err, ErrPeerNotFound) {
		return fmt.Errorf("sending witness response: %w", err)
	}
	return nil
}

const witnessResponderLogPrefix = "[p2p.witness.responder]"
