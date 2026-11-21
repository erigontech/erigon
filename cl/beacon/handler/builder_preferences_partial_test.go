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
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/builder/mock_services"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

func TestBuilderPreferencesIsolatesSemanticallyInvalidEntry(t *testing.T) {
	for _, contentType := range []string{"application/json", "application/octet-stream"} {
		t.Run(contentType, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			client := mock_services.NewMockBuilderClient(ctrl)
			entries := testBuilderPreferencesEntries()
			var calls atomic.Int32
			client.EXPECT().SubmitBuilderPreferences(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, url string, pubkey common.Bytes48, _ *cltypes.BuilderPreferencesRequest) error {
				require.Equal(t, entries[1].URL, url)
				require.Equal(t, entries[1].ProposerPubkey, pubkey)
				calls.Add(1)
				return nil
			}).AnyTimes()
			var body []byte
			var err error
			if contentType == "application/json" {
				body, err = entries.MarshalJSON()
			} else {
				body, err = entries.EncodeSSZ(nil)
			}
			require.NoError(t, err)
			body = bytes.Replace(body, []byte("https://builder.example"), []byte("ftpsx://builder.example"), 1)
			handler := &ApiHandler{builderClient: client}
			request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/validator/builder_preferences", bytes.NewReader(body))
			request.Header.Set("Content-Type", contentType)
			request.Header.Set("Eth-Consensus-Version", "gloas")
			recorder := httptest.NewRecorder()
			handler.PostEthV1ValidatorBuilderPreferences(recorder, request)
			require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
			t.Logf("valid submissions=%d response=%s", calls.Load(), recorder.Body.String())
			require.Equal(t, int32(1), calls.Load(), "entry 0 semantic failure must not suppress valid entry 1")
			require.Contains(t, recorder.Body.String(), `"index":0`)
		})
	}
}
