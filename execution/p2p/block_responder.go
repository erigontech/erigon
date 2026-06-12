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
	"errors"
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

func NewBlockResponder(
	logger log.Logger,
	messageListener *MessageListener,
	publisher *Publisher,
	db kv.TemporalRoDB,
	blockReader services.FullBlockReader,
	chainConfig *chain.Config,
	receiptsGetter eth.ReceiptsGetter,
) *BlockResponder {
	r := &BlockResponder{
		logger:         logger,
		publisher:      publisher,
		db:             db,
		blockReader:    blockReader,
		chainConfig:    chainConfig,
		receiptsGetter: receiptsGetter,
		headerTasks:    make(chan *DecodedInboundMessage[*eth.GetBlockHeadersPacket66], responderTaskQueueSize),
		bodyTasks:      make(chan *DecodedInboundMessage[*eth.GetBlockBodiesPacket66], responderTaskQueueSize),
		receiptTasks:   make(chan getReceiptsTask, responderTaskQueueSize),
	}
	messageListener.RegisterGetBlockHeadersObserver(func(message *DecodedInboundMessage[*eth.GetBlockHeadersPacket66]) {
		enqueueResponderTask(logger, blockResponderLogPrefix, r.headerTasks, message)
	})
	messageListener.RegisterGetBlockBodiesObserver(func(message *DecodedInboundMessage[*eth.GetBlockBodiesPacket66]) {
		enqueueResponderTask(logger, blockResponderLogPrefix, r.bodyTasks, message)
	})
	messageListener.RegisterGetReceiptsObserver(func(message *DecodedInboundMessage[*eth.GetReceiptsPacket66]) {
		ethVersion := uint(direct.ETH68)
		if message.Id == sentryproto.MessageId_GET_RECEIPTS_69 {
			ethVersion = direct.ETH69
		}
		enqueueResponderTask(logger, blockResponderLogPrefix, r.receiptTasks, getReceiptsTask{
			peerId:     message.PeerId,
			requestId:  message.Decoded.RequestId,
			query:      message.Decoded.GetReceiptsPacket,
			ethVersion: ethVersion,
		})
	})
	messageListener.RegisterGetReceipts70Observer(func(message *DecodedInboundMessage[*eth.GetReceiptsPacket70]) {
		enqueueResponderTask(logger, blockResponderLogPrefix, r.receiptTasks, getReceiptsTask{
			peerId:                 message.PeerId,
			requestId:              message.Decoded.RequestId,
			query:                  message.Decoded.GetReceiptsPacket,
			ethVersion:             direct.ETH70,
			firstBlockReceiptIndex: message.Decoded.FirstBlockReceiptIndex,
		})
	})
	return r
}

// BlockResponder serves inbound GetBlockHeaders, GetBlockBodies and GetReceipts
// requests from peers. Requests are queued at registration time and processed by
// Run; header serving gets its own worker so that heavy body/receipt uploads do
// not delay it.
type BlockResponder struct {
	logger         log.Logger
	publisher      *Publisher
	db             kv.TemporalRoDB
	blockReader    services.FullBlockReader
	chainConfig    *chain.Config
	receiptsGetter eth.ReceiptsGetter
	headerTasks    chan *DecodedInboundMessage[*eth.GetBlockHeadersPacket66]
	bodyTasks      chan *DecodedInboundMessage[*eth.GetBlockBodiesPacket66]
	receiptTasks   chan getReceiptsTask
}

type getReceiptsTask struct {
	peerId                 *PeerId
	requestId              uint64
	query                  eth.GetReceiptsPacket
	ethVersion             uint
	firstBlockReceiptIndex uint64
}

func (r *BlockResponder) Run(ctx context.Context) error {
	r.logger.Info(blockResponderLogPrefix + " running block responder component")
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return processResponderTasks(ctx, r.headerTasks, r.handleGetBlockHeaders, r.logger, blockResponderLogPrefix)
	})
	eg.Go(func() error {
		return processResponderTasks(ctx, r.bodyTasks, r.handleGetBlockBodies, r.logger, blockResponderLogPrefix)
	})
	eg.Go(func() error {
		return processResponderTasks(ctx, r.receiptTasks, r.handleGetReceipts, r.logger, blockResponderLogPrefix)
	})
	return eg.Wait()
}

func (r *BlockResponder) handleGetBlockHeaders(ctx context.Context, message *DecodedInboundMessage[*eth.GetBlockHeadersPacket66]) error {
	var headers []*types.Header
	err := r.db.View(ctx, func(tx kv.Tx) (err error) {
		headers, err = eth.AnswerGetBlockHeadersQuery(tx, message.Decoded.GetBlockHeadersPacket, r.blockReader)
		return err
	})
	if err != nil {
		return fmt.Errorf("querying BlockHeaders: %w", err)
	}
	// Respond even when the headers list is empty: nodes running erigon 2.48 with
	// --sentry.drop-useless-peers kick peers whose requests time out with no response.
	err = r.publisher.PublishBlockHeaders(ctx, message.PeerId, eth.BlockHeadersPacket66{
		RequestId:          message.Decoded.RequestId,
		BlockHeadersPacket: headers,
	})
	if err != nil && !errors.Is(err, ErrPeerNotFound) {
		return fmt.Errorf("send header response: %w", err)
	}
	return nil
}

func (r *BlockResponder) handleGetBlockBodies(ctx context.Context, message *DecodedInboundMessage[*eth.GetBlockBodiesPacket66]) error {
	tx, err := r.db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	response := eth.AnswerGetBlockBodiesQuery(tx, message.Decoded.GetBlockBodiesPacket, r.blockReader)
	tx.Rollback()
	err = r.publisher.PublishBlockBodies(ctx, message.PeerId, eth.BlockBodiesRLPPacket66{
		RequestId:            message.Decoded.RequestId,
		BlockBodiesRLPPacket: response,
	})
	if err != nil && !errors.Is(err, ErrPeerNotFound) {
		return fmt.Errorf("send bodies response: %w", err)
	}
	return nil
}

func (r *BlockResponder) handleGetReceipts(ctx context.Context, task getReceiptsTask) error {
	sizeLimit := eth.NoSizeLimit
	if task.ethVersion >= direct.ETH70 {
		sizeLimit = eth.Eth70ResponseSizeLimit
	}
	opts := eth.ReceiptQueryOpts{
		EthVersion:             task.ethVersion,
		FirstBlockReceiptIndex: task.firstBlockReceiptIndex,
		SizeLimit:              sizeLimit,
	}
	cached, needMore, err := eth.AnswerGetReceiptsQueryCacheOnly(ctx, r.receiptsGetter, task.query, opts)
	if err != nil {
		return err
	}
	var receiptsList []rlp.RawValue
	var lastBlockIncomplete bool
	if cached != nil {
		receiptsList = cached.EncodedReceipts
		lastBlockIncomplete = cached.LastBlockIncomplete
	}
	if needMore {
		tx, err := r.db.BeginTemporalRo(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		receiptsList, lastBlockIncomplete, err = eth.AnswerGetReceiptsQuery(ctx, r.chainConfig, r.receiptsGetter, r.blockReader, tx, task.query, cached, opts)
		if err != nil {
			return err
		}
	}
	if task.ethVersion >= direct.ETH70 {
		err = r.publisher.PublishReceipts70(ctx, task.peerId, eth.ReceiptsRLPPacket70{
			RequestId:           task.requestId,
			LastBlockIncomplete: lastBlockIncomplete,
			ReceiptsRLPPacket:   receiptsList,
		})
	} else {
		err = r.publisher.PublishReceipts66(ctx, task.peerId, eth.ReceiptsRLPPacket66{
			RequestId:         task.requestId,
			ReceiptsRLPPacket: receiptsList,
		})
	}
	if err != nil && !errors.Is(err, ErrPeerNotFound) {
		return fmt.Errorf("send receipts response: %w", err)
	}
	return nil
}

const blockResponderLogPrefix = "[p2p.block.responder]"
