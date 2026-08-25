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
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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

func TestPostValidatorBuilderPreferencesReportsMalformedJSONEntryAndContinues(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock_services.NewMockBuilderClient(ctrl)
	valid := testBuilderPreferencesEntries()[1]
	validJSON, err := valid.MarshalJSON()
	require.NoError(t, err)
	client.EXPECT().SubmitBuilderPreferences(gomock.Any(), valid.URL, valid.ProposerPubkey, gomock.Any()).Return(nil)
	handler := &ApiHandler{builderClient: client}
	body := append([]byte(`[{"url":7},`), validJSON...)
	body = append(body, ']')
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/validator/builder_preferences", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1ValidatorBuilderPreferences(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Contains(t, recorder.Body.String(), `"index":0`)
}

func TestPostValidatorBuilderPreferencesRejectsStructurallyInvalidJSON(t *testing.T) {
	ctrl := gomock.NewController(t)
	handler := &ApiHandler{builderClient: mock_services.NewMockBuilderClient(ctrl)}
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/validator/builder_preferences", strings.NewReader(`[{"url":7}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1ValidatorBuilderPreferences(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.NotContains(t, recorder.Body.String(), `"failures"`)
}

func TestPostValidatorBuilderPreferencesBoundsSlowEntriesAndContinues(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock_services.NewMockBuilderClient(ctrl)
	entries := make(cltypes.BuilderPreferencesEntries, 40)
	for i := range entries {
		entries[i] = testBuilderPreferencesEntries()[0].Clone().(*cltypes.BuilderPreferencesEntry)
		entries[i].ProposerPubkey[0] = byte(i + 1)
	}
	client.EXPECT().SubmitBuilderPreferences(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, _ string, proposer common.Bytes48, _ *cltypes.BuilderPreferencesRequest) error {
			if proposer == entries[len(entries)-1].ProposerPubkey {
				return nil
			}
			select {
			case <-time.After(100 * time.Millisecond):
				return errors.New("slow failure")
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	).Times(len(entries))
	handler := &ApiHandler{builderClient: client}
	body, err := entries.MarshalJSON()
	require.NoError(t, err)
	requestContext, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	request := httptest.NewRequestWithContext(requestContext, http.MethodPost, "/eth/v1/validator/builder_preferences", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()
	started := time.Now()

	handler.PostEthV1ValidatorBuilderPreferences(recorder, request)

	require.Less(t, time.Since(started), 500*time.Millisecond)
	require.Equal(t, http.StatusBadRequest, recorder.Code)
	var response poolingError
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.Len(t, response.Failures, len(entries)-1)
	for i, failure := range response.Failures {
		require.Equal(t, i, failure.Index)
	}
}
