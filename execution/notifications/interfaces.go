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

package notifications

import (
	"context"

	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

// StateChangeConsumer receives protobuf-encoded state changes for remote
// (gRPC) distribution. Only the gRPC server (KvServer) implements this.
// This interface exists for backward compatibility with the remote rpcdaemon
// path. In-process consumers should use BlockBatchConsumer instead.
type StateChangeConsumer interface {
	SendStateChanges(ctx context.Context, sc *remoteproto.StateChangeBatch)
}

// BlockBatchConsumer receives native-typed block notifications for local
// (in-process) consumers like TxPool. No protobuf serialization involved.
type BlockBatchConsumer interface {
	OnNewBlock(ctx context.Context, notification *BlockBatchNotification) error
}

// ChainEventNotifier is the interface for delivering chain events (new headers,
// logs, receipts) to subscribers. Implemented by shards.Events.
type ChainEventNotifier interface {
	OnNewHeader(newHeadersRlp [][]byte)
	OnNewPendingLogs(types.Logs)
	OnLogs([]*LogNotification)
	HasLogSubscriptions() bool
	OnReceipts([]*ReceiptNotification)
	HasReceiptSubscriptions() bool
}
