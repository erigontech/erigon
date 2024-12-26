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

	"github.com/erigontech/erigon-lib/log/v3"
)

type DiagTxn struct {
	Hash []byte
}

type DiagTxnUpdate struct {
	Txns []DiagTxn
}

func (ti DiagTxnUpdate) Type() Type {
	return TypeOf(ti)
}

type DiagBlockTxnUpdate struct {
	Txns []DiagTxn
}

func (ti DiagBlockTxnUpdate) Type() Type {
	return TypeOf(ti)
}

func (d *DiagnosticClient) setupTxPoolDiagnostics(rootCtx context.Context) {
	d.runOnNewTnxsListener(rootCtx)
	d.runOnNewBlockListener(rootCtx)
}

func (d *DiagnosticClient) runOnNewTnxsListener(rootCtx context.Context) {
	go func() {
		//ctx, ch, closeChannel := Context[DiagTxnUpdate](rootCtx, 1)
		ctx, _, closeChannel := Context[DiagTxnUpdate](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(DiagTxnUpdate{}), log.Root())
		for {
			select {
				case <-rootCtx.Done():
					return
				//case info := <-ch:
				//	for _, txn := range info.Txns {
				//		fmt.Println("Added txn with hash: ", txn.Hash)
				//	}
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
					for _, txn := range info.Txns {
						fmt.Println("DiagBlockTxnUpdate Added txn with hash: ", txn.Hash)
					}
			}
		}
	}()
}
