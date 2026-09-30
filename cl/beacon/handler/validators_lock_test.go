// Copyright 2026 The Erigon Authors
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

package handler

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common/log/v3"
)

type blockingResponseWriter struct {
	header  http.Header
	started chan struct{}
	release chan struct{}
}

func (w *blockingResponseWriter) Header() http.Header { return w.header }

func (w *blockingResponseWriter) WriteHeader(int) {}

func (w *blockingResponseWriter) Write(b []byte) (int, error) {
	close(w.started)
	<-w.release
	return len(b), nil
}

func TestGetValidatorsHeadDoesNotHoldHeadStateWhileWriting(t *testing.T) {
	_, blocks, _, _, postState, h, _, syncedData, fcu, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), true)
	var err error
	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)
	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot

	w := &blockingResponseWriter{header: http.Header{}, started: make(chan struct{}), release: make(chan struct{})}
	defer close(w.release)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/beacon/states/head/validators", nil)
	go h.mux.ServeHTTP(w, req)

	select {
	case <-w.started:
	case <-time.After(10 * time.Second):
		t.Fatal("validators response was never written")
	}

	published := make(chan error, 1)
	go func() { published <- syncedData.OnHeadState(postState) }()
	select {
	case err := <-published:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("head state update blocked while a validators response was being written")
	}
}
