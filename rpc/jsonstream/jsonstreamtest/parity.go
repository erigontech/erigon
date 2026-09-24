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

// Package jsonstreamtest checks fast-JSON marshallers against encoding/json.
package jsonstreamtest

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/rpc/jsonstream"
)

type fastMarshaler[T any] interface {
	*T
	MarshalFastJSONTo(*jsonstream.StackStream) error
}

// RequireMatchesReflection checks that full encodes byte for byte as encoding/json encodes it,
// and again with each exported field in turn set to its zero value, and each pointer, slice and
// map field set to a pointer to zero or an empty value: the cases where null and omitempty differ.
func RequireMatchesReflection[T any, P fastMarshaler[T]](t *testing.T, full P) {
	t.Helper()
	requireSame[T, P](t, "full", full)
	for _, f := range reflect.VisibleFields(reflect.TypeFor[T]()) {
		if !f.IsExported() || f.Anonymous || f.Tag.Get("json") == "-" {
			continue
		}
		for name, value := range variants(f.Type) {
			v := *full
			reflect.ValueOf(&v).Elem().FieldByIndex(f.Index).Set(value)
			requireSame[T, P](t, f.Name+"/"+name, &v)
		}
	}
}

func variants(typ reflect.Type) map[string]reflect.Value {
	vs := map[string]reflect.Value{"zero": reflect.Zero(typ)}
	switch typ.Kind() {
	case reflect.Pointer:
		vs["ptr to zero"] = reflect.New(typ.Elem())
	case reflect.Slice:
		vs["empty"] = reflect.MakeSlice(typ, 0, 0)
	case reflect.Map:
		vs["empty"] = reflect.MakeMap(typ)
	}
	return vs
}

func requireSame[T any, P fastMarshaler[T]](t *testing.T, name string, v P) {
	t.Helper()
	want, wantErr := json.Marshal(v)
	got, gotErr := jsonstream.Marshal(v)
	if wantErr != nil || gotErr != nil {
		require.Equal(t, wantErr != nil, gotErr != nil, "%s: reflection err %v, fast err %v", name, wantErr, gotErr)
		return
	}
	require.Equal(t, string(want), string(got), name)
}
