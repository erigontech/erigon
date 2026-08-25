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
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/builder/mock_services"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

func testBuilderPreferencesEntries() cltypes.BuilderPreferencesEntries {
	entries := make(cltypes.BuilderPreferencesEntries, 2)
	for i := range entries {
		entries[i] = &cltypes.BuilderPreferencesEntry{
			ProposerPubkey: common.Bytes48{byte(i + 1)},
			URL:            "https://builder.example",
			Auth: &cltypes.SignedBuilderRequestAuth{Message: &cltypes.BuilderRequestAuth{
				Data: []byte("https://builder.example"), Slot: 10,
			}},
			MaxExecutionPayment: uint64(i + 1),
		}
	}
	return entries
}

func TestPostValidatorBuilderPreferencesReportsPartialFailuresAfterSubmittingAll(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock_services.NewMockBuilderClient(ctrl)
	entries := testBuilderPreferencesEntries()
	client.EXPECT().SubmitBuilderPreferences(gomock.Any(), entries[0].URL, entries[0].ProposerPubkey, gomock.Any()).Return(errors.New("first failed"))
	client.EXPECT().SubmitBuilderPreferences(gomock.Any(), entries[1].URL, entries[1].ProposerPubkey, gomock.Any()).Return(nil)
	handler := &ApiHandler{builderClient: client}
	body, err := entries.MarshalJSON()
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/validator/builder_preferences", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1ValidatorBuilderPreferences(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Contains(t, recorder.Body.String(), `"index":0`)
}

func TestPostValidatorBuilderPreferencesAcceptsSSZ(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock_services.NewMockBuilderClient(ctrl)
	entries := testBuilderPreferencesEntries()[:1]
	client.EXPECT().SubmitBuilderPreferences(gomock.Any(), entries[0].URL, entries[0].ProposerPubkey, gomock.Any()).Return(nil)
	handler := &ApiHandler{builderClient: client}
	body, err := entries.EncodeSSZ(nil)
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/validator/builder_preferences", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1ValidatorBuilderPreferences(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
}

func TestPostValidatorBuilderPreferencesAcceptsMaximumJSONList(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock_services.NewMockBuilderClient(ctrl)
	url := "https://builder.example/" + strings.Repeat("a", 2000)
	entry := &cltypes.BuilderPreferencesEntry{
		ProposerPubkey: common.Bytes48{1},
		URL:            url,
		Auth: &cltypes.SignedBuilderRequestAuth{Message: &cltypes.BuilderRequestAuth{
			Data: make([]byte, cltypes.MaxBuilderAuthDataSize), Slot: 10,
		}},
	}
	entries := make(cltypes.BuilderPreferencesEntries, cltypes.MaxBuilderPreferencesEntries)
	for i := range entries {
		entries[i] = entry
	}
	body, err := entries.MarshalJSON()
	require.NoError(t, err)
	require.Greater(t, len(body), 32<<20)
	client.EXPECT().SubmitBuilderPreferences(gomock.Any(), url, entry.ProposerPubkey, gomock.Any()).Return(nil).Times(len(entries))
	handler := &ApiHandler{builderClient: client}
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/validator/builder_preferences", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1ValidatorBuilderPreferences(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
}
