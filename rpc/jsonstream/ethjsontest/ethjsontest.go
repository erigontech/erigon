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

// Package ethjsontest builds the JSON a struct's own declaration asks for, so a hand-written
// MarshalFastJSONTo can be diffed against the type it encodes instead of against a second
// copy of the declaration.
package ethjsontest

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/hexutil"
)

var u256 = reflect.TypeFor[uint256.Int]()

// Computed is a field an encoder writes that the struct has no field for, such as a block
// hash. Raw is the field's JSON, already encoded.
type Computed struct {
	Name string
	Raw  string
}

// ExpectedJSON encodes v the way its tags declare: the json tag gives each field's name, its
// position and whether it may be omitted, and the ethjson tag gives the hex form the JSON-RPC
// spec uses for it — "quantity" for a number, "data" for bytes. A field without an ethjson tag
// falls back to encoding/json. Computed fields are appended in order.
func ExpectedJSON(v any, computed ...Computed) ([]byte, error) {
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return []byte("null"), nil
		}
		rv = rv.Elem()
	}
	typ := rv.Type()

	buf := []byte{'{'}
	for i := range typ.NumField() {
		field := typ.Field(i)
		tag, ok := field.Tag.Lookup("json")
		if !ok {
			continue
		}
		name, opts, _ := strings.Cut(tag, ",")
		if name == "-" {
			continue
		}
		value := rv.Field(i)
		if strings.Contains(opts, "omitempty") && value.IsZero() {
			continue
		}
		encoded, err := jsonrpcValue(value, field.Tag.Get("ethjson"))
		if err != nil {
			return nil, fmt.Errorf("%s.%s: %w", typ.Name(), field.Name, err)
		}
		buf = appendField(buf, name, encoded)
	}
	for _, c := range computed {
		buf = appendField(buf, c.Name, []byte(c.Raw))
	}
	return append(buf, '}'), nil
}

func appendField(buf []byte, name string, encoded []byte) []byte {
	if len(buf) > 1 {
		buf = append(buf, ',')
	}
	buf = append(buf, '"')
	buf = append(buf, name...)
	buf = append(buf, '"', ':')
	return append(buf, encoded...)
}

func jsonrpcValue(v reflect.Value, form string) ([]byte, error) {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return []byte("null"), nil
		}
		v = v.Elem()
	}
	switch form {
	case "":
		return json.Marshal(v.Interface())
	case "quantity":
		if v.Type() == u256 {
			n := v.Interface().(uint256.Int)
			return json.Marshal((*hexutil.U256)(&n))
		}
		switch v.Kind() {
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return json.Marshal(hexutil.Uint64(v.Uint()))
		}
		return nil, fmt.Errorf("ethjson:\"quantity\" on %v", v.Type())
	case "data":
		bytes, err := asBytes(v)
		if err != nil {
			return nil, err
		}
		return json.Marshal(hexutil.Bytes(bytes))
	}
	return nil, fmt.Errorf("unknown ethjson form %q", form)
}

func asBytes(v reflect.Value) ([]byte, error) {
	switch {
	case v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8:
		return v.Bytes(), nil
	case v.Kind() == reflect.Array && v.Type().Elem().Kind() == reflect.Uint8:
		out := make([]byte, v.Len())
		reflect.Copy(reflect.ValueOf(out), v)
		return out, nil
	}
	return nil, fmt.Errorf("ethjson:\"data\" on %v", v.Type())
}
