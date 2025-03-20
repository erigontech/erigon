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

package diagnostics

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/holiman/uint256"
)

type PoolChangeEvent struct {
	Pool    string   `json:"pool"`
	Event   string   `json:"event"`
	TxnHash [32]byte `json:"txnHash"`
}

type DiagTxn struct {
	IDHash              [32]byte      `json:"hash"`
	SenderID            uint64        `json:"senderID"`
	Nonce               uint64        `json:"nonce"`
	Value               uint256.Int   `json:"value"`
	Gas                 uint64        `json:"gas"`
	FeeCap              uint256.Int   `json:"feeCap"`
	Tip                 uint256.Int   `json:"tip"`
	Size                uint32        `json:"size"`
	Type                byte          `json:"type"`
	Creation            bool          `json:"creation"`
	DataLen             int           `json:"dataLen"`
	AccessListAddrCount int           `json:"accessListAddrCount"`
	AccessListStorCount int           `json:"accessListStorCount"`
	BlobHashes          []common.Hash `json:"blobHashes"`
	Blobs               [][]byte      `json:"blobs"`
	IsLocal             bool          `json:"isLocal"`
	DiscardReason       string        `json:"discardReason"`
	Pool                string        `json:"pool"`
}

type IncomingTxnUpdate struct {
	Txns    []DiagTxn             `json:"txns"`
	Updates map[string][][32]byte `json:"updates"`
}

func (ti IncomingTxnUpdate) Type() Type {
	return TypeOf(ti)
}

type ProcessedRemoteTxnsUpdate struct {
	Txns [][32]byte `json:"txns"`
}

func (ti ProcessedRemoteTxnsUpdate) Type() Type {
	return TypeOf(ti)
}

type PendingAddEvent struct {
	TxnHash [32]byte `json:"txnHash"`
}

func (ti PendingAddEvent) Type() Type {
	return TypeOf(ti)
}

type PendingRemoveEvent struct {
	TxnHash [32]byte `json:"txnHash"`
}

func (ti PendingRemoveEvent) Type() Type {
	return TypeOf(ti)
}

type BaseFeeAddEvent struct {
	TxnHash [32]byte `json:"txnHash"`
}

func (ti BaseFeeAddEvent) Type() Type {
	return TypeOf(ti)
}

type BaseFeeRemoveEvent struct {
	TxnHash [32]byte `json:"txnHash"`
}

func (ti BaseFeeRemoveEvent) Type() Type {
	return TypeOf(ti)
}

type QueuedAddEvent struct {
	TxnHash [32]byte `json:"txnHash"`
}

func (ti QueuedAddEvent) Type() Type {
	return TypeOf(ti)
}

type QueuedRemoveEvent struct {
	TxnHash [32]byte `json:"txnHash"`
}

func (ti QueuedRemoveEvent) Type() Type {
	return TypeOf(ti)
}

func (d *DiagnosticClient) setupTxPoolDiagnostics(rootCtx context.Context) {
	d.runOnIncommingTxnListener(rootCtx)
	d.runOnProcessedRemoteTxnsListener(rootCtx)
	d.runOnPendingAddEvent(rootCtx)
	d.runOnPendingRemoveEvent(rootCtx)
	d.runOnBaseFeeAddEvent(rootCtx)
	d.runOnBaseFeeRemoveEvent(rootCtx)
	d.runOnQueuedAddEvent(rootCtx)
	d.runOnQueuedRemoveEvent(rootCtx)
	d.SetupNotifier()
}

func (d *DiagnosticClient) runOnIncommingTxnListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[IncomingTxnUpdate](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(IncomingTxnUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.Notify(DiagMessages{
					MessageType: "txpool",
					Message:     info,
				})
			}
		}
	}()
}

func (d *DiagnosticClient) runOnProcessedRemoteTxnsListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[ProcessedRemoteTxnsUpdate](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(ProcessedRemoteTxnsUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				fmt.Println(info)
				/*d.Notify(DiagMessages{
					MessageType: "txpool",
					Message:     info,
				})*/
			}
		}
	}()
}

func (d *DiagnosticClient) runOnPendingAddEvent(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[PendingAddEvent](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(PendingAddEvent{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.Notify(DiagMessages{
					MessageType: "txpool",
					Message: PoolChangeEvent{
						Pool:    "Pending",
						Event:   "add",
						TxnHash: info.TxnHash,
					},
				})
			}
		}
	}()
}

func (d *DiagnosticClient) runOnPendingRemoveEvent(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[PendingRemoveEvent](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(PendingRemoveEvent{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.Notify(DiagMessages{
					MessageType: "txpool",
					Message: PoolChangeEvent{
						Pool:    "Pending",
						Event:   "remove",
						TxnHash: info.TxnHash,
					},
				})
			}
		}
	}()
}

func (d *DiagnosticClient) runOnBaseFeeAddEvent(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[BaseFeeAddEvent](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(BaseFeeAddEvent{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.Notify(DiagMessages{
					MessageType: "txpool",
					Message: PoolChangeEvent{
						Pool:    "BaseFee",
						Event:   "add",
						TxnHash: info.TxnHash,
					},
				})
			}
		}
	}()
}

func (d *DiagnosticClient) runOnBaseFeeRemoveEvent(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[BaseFeeRemoveEvent](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(BaseFeeRemoveEvent{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.Notify(DiagMessages{
					MessageType: "txpool",
					Message: PoolChangeEvent{
						Pool:    "BaseFee",
						Event:   "remove",
						TxnHash: info.TxnHash,
					},
				})
			}
		}
	}()
}

func (d *DiagnosticClient) runOnQueuedAddEvent(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[QueuedAddEvent](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(QueuedAddEvent{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.Notify(DiagMessages{
					MessageType: "txpool",
					Message: PoolChangeEvent{
						Pool:    "Queued",
						Event:   "add",
						TxnHash: info.TxnHash,
					},
				})
			}
		}
	}()
}

func (d *DiagnosticClient) runOnQueuedRemoveEvent(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[QueuedRemoveEvent](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(QueuedRemoveEvent{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.Notify(DiagMessages{
					MessageType: "txpool",
					Message: PoolChangeEvent{
						Pool:    "Queued",
						Event:   "remove",
						TxnHash: info.TxnHash,
					},
				})
			}
		}
	}()
}
