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

package node

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/rpc"
)

type registrationTestService struct{}

func (registrationTestService) Served() string   { return "served" }
func (registrationTestService) Withheld() string { return "withheld" }

type servedMethods interface {
	Served() string
}

func TestRegisterApisFromWhitelistHonoursIface(t *testing.T) {
	logger := testlog.Logger(t, log.LvlError)
	srv := newTestRPCServer(t)

	apis := []rpc.API{{
		Namespace: "test",
		Public:    true,
		Service:   registrationTestService{},
		Iface:     reflect.TypeFor[servedMethods](),
	}}
	require.NoError(t, RegisterApisFromWhitelist(apis, nil, srv, true, logger))

	client := rpc.DialInProc(srv, logger)
	defer client.Close()

	var served string
	require.NoError(t, client.Call(&served, "test_served"))
	require.Equal(t, "served", served)

	require.ErrorContains(t, client.Call(new(string), "test_withheld"), "does not exist/is not available")
}
