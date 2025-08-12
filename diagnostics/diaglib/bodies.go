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
	"encoding/json"
	"io"

	"github.com/erigontech/erigon-lib/log/v3"
)

func (d *DiagnosticClient) setupBodiesDiagnostics(rootCtx context.Context) {
	d.runBodiesBlockDownloadListener(rootCtx)
	d.runBodiesBlockWriteListener(rootCtx)
	d.runBodiesProcessingListener(rootCtx)
	d.runBodiesProcessedListener(rootCtx)
}

func (d *DiagnosticClient) runBodiesBlockDownloadListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[BodiesDownloadBlockUpdate](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(BodiesDownloadBlockUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.bodiesMutex.Lock()
				d.bodies.BlockDownload = info
				d.bodiesMutex.Unlock()
			}
		}

	}()
}

func (d *DiagnosticClient) runBodiesBlockWriteListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[BodiesWriteBlockUpdate](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(BodiesWriteBlockUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.bodiesMutex.Lock()
				d.bodies.BlockWrite = info
				d.bodiesMutex.Unlock()
			}
		}

	}()
}

func (d *DiagnosticClient) runBodiesProcessedListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[BodiesProcessedUpdate](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(BodiesProcessedUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.bodiesMutex.Lock()
				d.bodies.Processed = info
				d.bodiesMutex.Unlock()
			}
		}

	}()
}

func (d *DiagnosticClient) runBodiesProcessingListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[BodiesProcessingUpdate](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(BodiesProcessingUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.bodiesMutex.Lock()
				d.bodies.Processing = info
				d.bodiesMutex.Unlock()
			}
		}

	}()
}

func (d *DiagnosticClient) BodiesInfoJson(w io.Writer) {
	d.bodiesMutex.Lock()
	defer d.bodiesMutex.Unlock()
	if err := json.NewEncoder(w).Encode(d.bodies); err != nil {
		log.Debug("[diagnostics] BodiesInfoJson", "err", err)
	}
}
