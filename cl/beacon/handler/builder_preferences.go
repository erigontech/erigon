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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
)

const (
	maxBuilderPreferencesRequestSize = 64 << 20
	maxBuilderFailureMessageSize     = 256
	builderPreferencesWorkers        = 32
	builderPreferencesRequestTimeout = 5 * time.Second
)

func (a *ApiHandler) PostEthV1ValidatorBuilderPreferences(w http.ResponseWriter, r *http.Request) {
	version, err := clparams.StringToClVersion(r.Header.Get("Eth-Consensus-Version"))
	if err != nil || version != clparams.GloasVersion {
		beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("Gloas Eth-Consensus-Version header is required")).WriteTo(w)
		return
	}
	if a.builderClient == nil {
		beaconhttp.NewEndpointError(http.StatusServiceUnavailable, errors.New("builder client is unavailable")).WriteTo(w)
		return
	}
	entries := cltypes.BuilderPreferencesEntries{}
	type indexedEntry struct {
		index int
		entry *cltypes.BuilderPreferencesEntry
	}
	var indexedEntries []indexedEntry
	failures := make([]poolingFailure, 0)
	contentType, err := requestContentType(r)
	if err != nil {
		beaconhttp.NewEndpointError(http.StatusUnsupportedMediaType, err).WriteTo(w)
		return
	}
	switch contentType {
	case "application/json":
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBuilderPreferencesRequestSize))
		var rawEntries []json.RawMessage
		if err := decoder.Decode(&rawEntries); err != nil {
			beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
			return
		}
		if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
			beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("request body contains trailing data")).WriteTo(w)
			return
		}
		if rawEntries == nil {
			beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("builder preferences entries cannot be null")).WriteTo(w)
			return
		}
		if len(rawEntries) > cltypes.MaxBuilderPreferencesEntries {
			beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("builder preferences entry count %d exceeds %d", len(rawEntries), cltypes.MaxBuilderPreferencesEntries)).WriteTo(w)
			return
		}
		indexedEntries = make([]indexedEntry, 0, len(rawEntries))
		for i, rawEntry := range rawEntries {
			entry := new(cltypes.BuilderPreferencesEntry)
			if err := json.Unmarshal(rawEntry, entry); err != nil {
				failures = append(failures, poolingFailure{Index: i, Message: builderFailureMessage(err)})
				continue
			}
			indexedEntries = append(indexedEntries, indexedEntry{index: i, entry: entry})
		}
	case "application/octet-stream":
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBuilderPreferencesRequestSize))
		if err != nil {
			beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
			return
		}
		if err := entries.DecodeSSZStrict(body, int(clparams.GloasVersion)); err != nil {
			beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
			return
		}
		indexedEntries = make([]indexedEntry, len(entries))
		for i, entry := range entries {
			indexedEntries[i] = indexedEntry{index: i, entry: entry}
		}
	default:
		beaconhttp.NewEndpointError(http.StatusUnsupportedMediaType, fmt.Errorf("unsupported content type: %s", contentType)).WriteTo(w)
		return
	}

	requestContext, cancel := context.WithTimeout(r.Context(), builderPreferencesRequestTimeout)
	defer cancel()
	jobs := make(chan indexedEntry, len(indexedEntries))
	results := make(chan poolingFailure, len(indexedEntries))
	workerCount := min(builderPreferencesWorkers, len(indexedEntries))
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for indexed := range jobs {
				entry := indexed.entry
				request := &cltypes.BuilderPreferencesRequest{
					Preferences: &cltypes.BuilderPreferences{MaxExecutionPayment: entry.MaxExecutionPayment},
					Auth:        entry.Auth,
				}
				if err := a.builderClient.SubmitBuilderPreferences(requestContext, entry.URL, entry.ProposerPubkey, request); err != nil {
					results <- poolingFailure{Index: indexed.index, Message: builderFailureMessage(err)}
				}
			}
		}()
	}
	for _, indexed := range indexedEntries {
		jobs <- indexed
	}
	close(jobs)
	workers.Wait()
	close(results)
	for failure := range results {
		failures = append(failures, failure)
	}
	sort.Slice(failures, func(i, j int) bool { return failures[i].Index < failures[j].Index })
	if len(failures) != 0 {
		a.writePoolingFailures(w, failures)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func builderFailureMessage(err error) string {
	message := err.Error()
	if len(message) > maxBuilderFailureMessageSize {
		return message[:maxBuilderFailureMessageSize]
	}
	return message
}
