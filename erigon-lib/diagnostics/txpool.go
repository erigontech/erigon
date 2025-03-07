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
	Txns      []DiagTxn `json:"txns"`
	Senders   []byte    `json:"senders"`
	IsLocal   []bool    `json:"isLocal"`
	KnownTxns [][]byte  `json:"knownTxns"` //hashes of incomming transactions from p2p network which are already in the pool
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

func (d *DiagnosticClient) setupTxPoolDiagnostics(rootCtx context.Context) {
	d.runOnIncommingTxnListener(rootCtx)
	d.runOnProcessedRemoteTxnsListener(rootCtx)
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
