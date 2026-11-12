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

package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
)

// beaconAPIRequests are the request helpers of the older capcli commands, each taking the URL it requests.
func beaconAPIRequests(t *testing.T) map[string]func(uri string) error {
	return map[string]func(base string) error{
		"block": func(base string) error {
			_, err := retrieveAndSanitizeBlockFromRemoteEndpoint(t.Context(), &clparams.MainnetBeaconConfig, base, nil)
			return err
		},
		"state": func(base string) error {
			_, err := getBeaconState(t.Context(), &clparams.MainnetBeaconConfig, base, 0)
			return err
		},
		"head": func(base string) error {
			_, err := getHead(t.Context(), base)
			return err
		},
		"state root": func(base string) error {
			_, err := getStateRootAtSlot(t.Context(), base, 0)
			return err
		},
		"benchmark": func(base string) error {
			_, err := timeRequest(t.Context(), base, "application/json", "GET", "")
			return err
		},
	}
}

func TestBeaconAPIRequestsKeepTheEndpointSecretOutOfErrors(t *testing.T) {
	closed, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	host := closed.Addr().String()
	require.NoError(t, closed.Close())

	requests := beaconAPIRequests(t)
	for name, request := range requests {
		t.Run(name, func(t *testing.T) {
			err := request("http://user:pw-secret@" + host + "/path-secret")
			require.Error(t, err)
			for _, secret := range []string{"pw-secret", "path-secret", "user"} {
				require.NotContains(t, err.Error(), secret)
			}
			require.Contains(t, err.Error(), host)

			for _, malformed := range []string{"http://h/%zz/path-secret", "http://user:Ab3x/path-secret@h/"} {
				err = request(malformed)
				require.Error(t, err)
				require.NotContains(t, err.Error(), "path-secret")
				require.NotContains(t, err.Error(), "Ab3x", "a parse error quotes the malformed part")
			}
		})
	}
}

// A malformed redirect is quoted by net/http, and a server can repeat the request's path and query in it.
func TestBeaconAPIRequestsKeepTheEndpointSecretOutOfMalformedRedirects(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", r.URL.Path+"/%zz?"+r.URL.RawQuery)
		w.WriteHeader(http.StatusFound)
	}))
	t.Cleanup(srv.Close)

	for name, request := range beaconAPIRequests(t) {
		t.Run(name, func(t *testing.T) {
			err := request(srv.URL + "/path-secret?apikey=query-secret")
			require.Error(t, err)
			require.NotContains(t, err.Error(), "path-secret")
			require.NotContains(t, err.Error(), "query-secret")
		})
	}
}

// notADatadir is a regular file: a command that touches its datadir fails on it.
func notADatadir(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(path, nil, 0o600))
	return path
}

var malformedEndpoints = []string{"beacon.example:5052/?apikey=key-secret", "ftp://beacon.example/key-secret", "http:///key-secret", "http://h/%zz/key-secret", "http://user:key-secret/x@h/", "http://:5052/key-secret"}

// A malformed --endpoint stops the fill before it touches the datadir, and the error names the flag by position so a
// key in the value stays out of it.
func TestBlobArchiveStoreFillRefusesAMalformedEndpoint(t *testing.T) {
	for _, endpoint := range malformedEndpoints {
		fill := &BlobArchiveStoreFill{
			chainCfg:     chainCfg{Chain: "mainnet"},
			outputFolder: outputFolder{Datadir: notADatadir(t)},
			Endpoints:    []string{"https://beacon.example", endpoint},
		}
		err := fill.Run(t.Context())
		require.ErrorContains(t, err, "--endpoint 2", endpoint)
		require.NotContains(t, err.Error(), "key-secret")
	}
}

func TestChainEndpointRefusesAMalformedEndpoint(t *testing.T) {
	for _, endpoint := range malformedEndpoints {
		chain := &ChainEndpoint{chainCfg: chainCfg{Chain: "mainnet"}, outputFolder: outputFolder{Datadir: notADatadir(t)}, Endpoint: endpoint}
		err := chain.Run(t.Context())
		require.ErrorContains(t, err, "--endpoint", endpoint)
		require.NotContains(t, err.Error(), "key-secret")
	}
}
