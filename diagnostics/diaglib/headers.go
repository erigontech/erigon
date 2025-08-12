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

func (d *DiagnosticClient) setupHeadersDiagnostics(rootCtx context.Context) {
	d.runHeadersWaitingListener(rootCtx)
	d.runWriteHeadersListener(rootCtx)
	d.runCanonicalMarkerListener(rootCtx)
	d.runProcessedListener(rootCtx)
}

func (d *DiagnosticClient) HeadersJson(w io.Writer) {
	d.headerMutex.Lock()
	defer d.headerMutex.Unlock()
	if err := json.NewEncoder(w).Encode(d.headers); err != nil {
		log.Debug("[diagnostics] HeadersJson", "err", err)
	}
}

func (d *DiagnosticClient) runHeadersWaitingListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[HeadersWaitingUpdate](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(HeadersWaitingUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.headerMutex.Lock()
				d.headers.WaitingForHeaders = info.From
				d.headerMutex.Unlock()

				return
			}
		}
	}()
}

func (d *DiagnosticClient) runWriteHeadersListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[BlockHeadersUpdate](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(BlockHeadersUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.headerMutex.Lock()
				d.headers.WriteHeaders = info
				d.headerMutex.Unlock()

				return
			}
		}
	}()
}

func (d *DiagnosticClient) runCanonicalMarkerListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[HeaderCanonicalMarkerUpdate](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(HeaderCanonicalMarkerUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.headerMutex.Lock()
				d.headers.CanonicalMarker = info
				d.headerMutex.Unlock()

				return
			}
		}
	}()
}

func (d *DiagnosticClient) runProcessedListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[HeadersProcessedUpdate](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(HeadersProcessedUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.headerMutex.Lock()
				d.headers.Processed = info
				d.headerMutex.Unlock()

				return
			}
		}
	}()
}
