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

package rpc

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

type declaredTestService interface {
	Echo(str string, i int, args *echoArgs) echoResult
	Rets() (string, error)
}

func TestRegisterAPIExposesOnlyTheDeclaredInterface(t *testing.T) {
	server := NewServer(50, false, false, true, log.New(), 100)
	require.NoError(t, server.RegisterAPI(API{
		Namespace: "test",
		Service:   new(testService),
		Iface:     reflect.TypeFor[declaredTestService](),
	}))

	svc, ok := server.services.services["test"]
	require.True(t, ok)
	require.NotNil(t, svc.callbacks["echo"])
	require.NotNil(t, svc.callbacks["rets"])
	require.Len(t, svc.callbacks, 2)
}

func TestRegisterAPIWithoutInterfaceExposesEveryMethod(t *testing.T) {
	server := NewServer(50, false, false, true, log.New(), 100)
	require.NoError(t, server.RegisterAPI(API{Namespace: "test", Service: new(testService)}))

	svc, ok := server.services.services["test"]
	require.True(t, ok)
	require.NotNil(t, svc.callbacks["noArgsRets"])
}

type unimplementedTestService interface {
	Echo(str string, i int, args *echoArgs) echoResult
	Ech0(str string, i int, args *echoArgs) echoResult
}

func TestRegisterAPIRejectsAnInterfaceTheServiceDoesNotImplement(t *testing.T) {
	server := NewServer(50, false, false, true, log.New(), 100)
	err := server.RegisterAPI(API{
		Namespace: "test",
		Service:   new(testService),
		Iface:     reflect.TypeFor[unimplementedTestService](),
	})

	require.ErrorContains(t, err, "does not implement")
	require.NotContains(t, server.services.services, "test")
}

func TestRegisterAPIRejectsANonInterfaceIface(t *testing.T) {
	server := NewServer(50, false, false, true, log.New(), 100)
	err := server.RegisterAPI(API{
		Namespace: "test",
		Service:   new(testService),
		Iface:     reflect.TypeFor[*testService](),
	})

	require.ErrorContains(t, err, "not an interface")
	require.NotContains(t, server.services.services, "test")
}
