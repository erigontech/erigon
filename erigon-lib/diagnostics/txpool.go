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

func (d *DiagnosticClient) setupTxPoolDiagnostics(rootCtx context.Context) {
	d.runOnIncommingTxnListener(rootCtx)
	d.runOnPoolChangeBatchEvent(rootCtx)
	d.runOnNewBlockListener(rootCtx)
	d.SetupNotifier()
	//GetPoolTransactions(rootCtx, "localhost:9090")
}

/*func GetPoolTransactions(ctx context.Context, grpcAddr string) ([]*types.Transaction, error) {
	// Create a gRPC connection
	conn, err := grpc.Dial(grpcAddr, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %v", err)
	}
	defer conn.Close()

	// Create a txpool client
	txPoolClient := txpoolproto.NewTxpoolClient(conn)

	// Get all transactions
	return GetAllTransactions(ctx, txPoolClient)
}*/

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
					MessageType: "txpool_IncomingTxnUpdate",
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
					for _, txnHash := range change.TxnHashOrder {
						d.Notify(DiagMessages{
							MessageType: "txpool_PoolChangeEvent",
							Message: PoolChangeEvent{
								Pool:    change.Pool,
								Event:   change.Event,
								TxnHash: hex.EncodeToString(txnHash.Hash[:]),
								Order:   txnHash.OrderMarker,
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
					MessageType: "txpool_BlockUpdate",
					Message:     info,
				})
			}
		}
	}()
}

/*func GetAllTransactions(ctx context.Context, txPoolClient *txpool.TxPoolClient) ([]*types.Transaction, error) {
	// Create an empty request to get all transactions
	reply, err := txPoolClient.All(ctx, &txpoolproto.AllRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to get all transactions: %v", err)
	}

	// Decode all transactions
	transactions := make([]*types.Transaction, 0, len(reply.Txs))
	for _, tx := range reply.Txs {
		txn, err := types.DecodeWrappedTransaction(tx.RlpTx)
		if err != nil {
			return nil, fmt.Errorf("failed to decode transaction: %v", err)
		}

		// Set the sender address
		var sender common.Address
		copy(sender[:], tx.Sender)
		txn.SetSender(sender)

		transactions = append(transactions, txn)
	}

	return transactions, nil
}*/
