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

	"github.com/erigontech/erigon-lib/log/v3"
)

type DiagTxn struct {
	Hash []byte `json:"hash"`
}

type DiagTxnUpdate struct {
	Txns []DiagTxn `json:"txns"`
}

func (ti DiagTxnUpdate) Type() Type {
	return TypeOf(ti)
}

type DiagBlockTxnUpdate struct {
	Txns []DiagTxn `json:"txns"`
}

func (ti DiagBlockTxnUpdate) Type() Type {
	return TypeOf(ti)
}

type WsTxnMsg struct {
	Txns   []DiagTxn `json:"txns"`
	Source string    `json:"source"`
}

func (d *DiagnosticClient) setupTxPoolDiagnostics(rootCtx context.Context) {
	d.runOnNewTnxsListener(rootCtx)
	d.runOnNewBlockListener(rootCtx)
	d.SetupNotifier(rootCtx)
}

func (d *DiagnosticClient) runOnNewTnxsListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[DiagTxnUpdate](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(DiagTxnUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.Notify(DiagMessages{
					Type: "txpool",
					Message: WsTxnMsg{
						Source: "p2p",
						Txns:   info.Txns,
					},
				})
			}
		}
	}()
}

func (d *DiagnosticClient) runOnNewBlockListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[DiagBlockTxnUpdate](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(DiagBlockTxnUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.Notify(DiagMessages{
					Type: "txpool",
					Message: WsTxnMsg{
						Source: "block",
						Txns:   info.Txns,
					},
				})
			}
		}
	}()
}
