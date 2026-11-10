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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
)

func TestBeaconAPIRequestsKeepTheEndpointSecretOutOfErrors(t *testing.T) {
	closed, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	host := closed.Addr().String()
	require.NoError(t, closed.Close())

	requests := map[string]func(base string) error{
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
	for name, request := range requests {
		t.Run(name, func(t *testing.T) {
			err := request("http://user:pw-secret@" + host + "/path-secret")
			require.Error(t, err)
			for _, secret := range []string{"pw-secret", "path-secret", "user"} {
				require.NotContains(t, err.Error(), secret)
			}
			require.Contains(t, err.Error(), host)

			err = request("http://h/%zz/path-secret")
			require.Error(t, err)
			require.NotContains(t, err.Error(), "path-secret")
		})
	}
}

// A malformed --endpoint stops the fill before it touches the datadir, and the error names the flag by position so a
// key in the value stays out of it.
func TestBlobArchiveStoreFillRefusesAMalformedEndpoint(t *testing.T) {
	for _, endpoint := range []string{"beacon.example:5052/?apikey=key-secret", "ftp://beacon.example/key-secret", "http:///key-secret", "http://h/%zz/key-secret"} {
		fill := &BlobArchiveStoreFill{
			chainCfg:     chainCfg{Chain: "mainnet"},
			outputFolder: outputFolder{Datadir: t.TempDir()},
			Endpoints:    []string{"https://beacon.example", endpoint},
		}
		err := fill.Run(t.Context())
		require.ErrorContains(t, err, "--endpoint 2", endpoint)
		require.NotContains(t, err.Error(), "key-secret")
	}
}
