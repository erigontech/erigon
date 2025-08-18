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

package diaglib

import (
	"context"
	"encoding/hex"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/holiman/uint256"
)

type PoolChangeEvent struct {
	Pool    string `json:"pool"`
	Event   string `json:"event"`
	TxnHash string `json:"txnHash"`
	Order   uint8  `json:"order"`
}

type DiagTxn struct {
	IDHash              string        `json:"hash"`
	SenderID            uint64        `json:"senderID"`
	Size                uint32        `json:"size"`
	Creation            bool          `json:"creation"`
	DataLen             int           `json:"dataLen"`
	AccessListAddrCount int           `json:"accessListAddrCount"`
	AccessListStorCount int           `json:"accessListStorCount"`
	BlobHashes          []common.Hash `json:"blobHashes"`
	IsLocal             bool          `json:"isLocal"`
	DiscardReason       string        `json:"discardReason"`
	Pool                string        `json:"pool"`
	OrderMarker         uint8         `json:"orderMarker"`
	RLP                 []byte        `json:"rlp"`
}

type IncomingTxnUpdate struct {
	Txns    []DiagTxn             `json:"txns"`
	Updates map[string][][32]byte `json:"updates"`
}

func (ti IncomingTxnUpdate) Type() Type {
	return TypeOf(ti)
}

type TxnHashOrder struct {
	OrderMarker uint8
	Hash        [32]byte
}

type PoolChangeBatch struct {
	Pool         string         `json:"pool"`
	OrderMarker  uint8          `json:"orderMarker"`
	Event        string         `json:"event"`
	TxnHashOrder []TxnHashOrder `json:"txnHash"`
}

type PoolChangeBatchEvent struct {
	Changes []PoolChangeBatch `json:"changes"`
}

func (ti PoolChangeBatchEvent) Type() Type {
	return TypeOf(ti)
}

type BlockUpdate struct {
	MinedTxns       []DiagTxn `json:"minedTxns"`
	UnwoundTxns     []DiagTxn `json:"unwoundTxns"`
	UnwoundBlobTxns []DiagTxn `json:"unwoundBlobTxns"`
	BlockNum        uint64    `json:"blkNum"`
	BlkTime         uint64    `json:"blkTime"`
}

func (ti BlockUpdate) Type() Type {
	return TypeOf(ti)
}

type SenderInfoUpdate struct {
	SenderId      uint64      `json:"senderId"`
	SenderNonce   uint64      `json:"senderNonce"`
	SenderBalance uint256.Int `json:"senderBalance"`
	BlockGasLimit uint64      `json:"blockGasLimit"`
}

func (ti SenderInfoUpdate) Type() Type {
	return TypeOf(ti)
}

type TxpoolDiagMessage struct {
	Type    string      `json:"type"`
	Message interface{} `json:"message"`
}

func (d *DiagnosticClient) setupTxPoolDiagnostics(rootCtx context.Context) {
	d.runOnIncommingTxnListener(rootCtx)
	d.runOnPoolChangeBatchEvent(rootCtx)
	d.runOnNewBlockListener(rootCtx)
	d.runOnSenderUpdateListener(rootCtx)

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
					Message: TxpoolDiagMessage{
						Type:    "incomingTxnUpdate",
						Message: info,
					},
				})
			}
		}
	}()
}

func (d *DiagnosticClient) runOnPoolChangeBatchEvent(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[PoolChangeBatchEvent](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(PoolChangeBatchEvent{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				for _, change := range info.Changes {
					for _, txnHash := range change.TxnHashOrder {
						d.Notify(DiagMessages{
							MessageType: "txpool",
							Message: TxpoolDiagMessage{
								Type: "poolChangeEvent",
								Message: PoolChangeEvent{
									Pool:    change.Pool,
									Event:   change.Event,
									TxnHash: hex.EncodeToString(txnHash.Hash[:]),
									Order:   txnHash.OrderMarker,
								},
							},
						})
					}
				}
			}
		}
	}()
}

func (d *DiagnosticClient) runOnNewBlockListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[BlockUpdate](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(BlockUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.Notify(DiagMessages{
					MessageType: "txpool",
					Message: TxpoolDiagMessage{
						Type:    "blockUpdate",
						Message: info,
					},
				})
			}
		}
	}()
}

func (d *DiagnosticClient) runOnSenderUpdateListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[SenderInfoUpdate](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(SenderInfoUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.Notify(DiagMessages{
					MessageType: "txpool",
					Message: TxpoolDiagMessage{
						Type:    "senderInfoUpdate",
						Message: info,
					},
				})
			}
		}
	}()
}
