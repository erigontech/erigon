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
	"encoding/hex"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/holiman/uint256"
)

type PoolChangeEvent struct {
	Pool    string `json:"pool"`
	Event   string `json:"event"`
	TxnHash string `json:"txnHash"`
}

type DiagTxn struct {
	IDHash              string        `json:"hash"`
	SenderID            uint64        `json:"senderID"`
	SenderAddress       string        `json:"senderAddress"`
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

type PoolChangeBatch struct {
	Pool    string     `json:"pool"`
	Event   string     `json:"event"`
	TxnHash [][32]byte `json:"txnHash"`
}

type PoolChangeBatchEvent struct {
	Changes []PoolChangeBatch `json:"changes"`
}

func (ti PoolChangeBatchEvent) Type() Type {
	return TypeOf(ti)
}

func (d *DiagnosticClient) setupTxPoolDiagnostics(rootCtx context.Context) {
	d.runOnIncommingTxnListener(rootCtx)
	d.runOnPoolChangeBatchEvent(rootCtx)
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
					for _, txnHash := range change.TxnHash {
						d.Notify(DiagMessages{
							MessageType: "txpool",
							Message: PoolChangeEvent{
								Pool:    change.Pool,
								Event:   change.Event,
								TxnHash: hex.EncodeToString(txnHash[:]),
							},
						})
					}
				}
			}
		}
	}()
}
