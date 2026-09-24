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
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/hexutil"
)

var u256 = reflect.TypeFor[uint256.Int]()

// Computed is a field the encoder writes that the struct has no field for, such as a block
// hash. Raw is the field's JSON, already encoded.
type Computed struct {
	Name string
	Raw  string
}

// ExpectedJSON encodes v the way its tags declare: the json tag gives each field's name, its
// position and whether it may be omitted, and the ethjson tag gives the form the JSON-RPC spec
// writes it in — "quantity" for a number, "data" for bytes, "datalist" for an array of those,
// "objects" for an array of values that declare themselves, "bool" for a plain JSON bool. A
// field with no ethjson tag is an error, since nothing would say which form it is, and an
// embedded field is flattened as encoding/json flattens an anonymous one.
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
		if field.Anonymous {
			inner, err := ExpectedJSON(rv.Field(i).Interface())
			if err != nil {
				return nil, fmt.Errorf("%s.%s: %w", typ.Name(), field.Name, err)
			}
			buf = appendInlined(buf, inner)
			continue
		}
		tag, ok := field.Tag.Lookup("json")
		if !ok {
			continue
		}
		name, opts, _ := strings.Cut(tag, ",")
		if name == "-" {
			continue
		}
		value := rv.Field(i)
		if strings.Contains(opts, "omitempty") && isEmpty(value) {
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

// isEmpty is what encoding/json's omitempty leaves out: a zero value, and also a slice, map or
// string with nothing in it, which IsZero alone reports as present.
func isEmpty(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Slice, reflect.Map, reflect.String, reflect.Array:
		return v.Len() == 0
	}
	return v.IsZero()
}

// appendInlined splices an embedded struct's fields in, the way encoding/json flattens an
// anonymous field.
func appendInlined(buf []byte, object []byte) []byte {
	if len(object) <= 2 {
		return buf
	}
	if len(buf) > 1 {
		buf = append(buf, ',')
	}
	return append(buf, object[1:len(object)-1]...)
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
		// A field with no ethjson tag would be checked against encoding/json, which agrees
		// with the encoder often enough to hide a forgotten tag.
		return nil, errors.New("no ethjson tag")
	case "quantity":
		if v.Type().ConvertibleTo(u256) && u256.ConvertibleTo(v.Type()) {
			n := v.Convert(u256).Interface().(uint256.Int)
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
	case "bool":
		if v.Kind() != reflect.Bool {
			return nil, fmt.Errorf("ethjson:\"bool\" on %v", v.Type())
		}
		return json.Marshal(v.Bool())
	case "objects":
		return jsonArray(v, func(e reflect.Value) ([]byte, error) { return ExpectedJSON(e.Interface()) })
	case "datalist":
		return jsonArray(v, func(e reflect.Value) ([]byte, error) {
			bytes, err := asBytes(e)
			if err != nil {
				return nil, err
			}
			return json.Marshal(hexutil.Bytes(bytes))
		})
	}
	return nil, fmt.Errorf("unknown ethjson form %q", form)
}

// jsonArray encodes a slice, or an interface holding one, with elem per element. A nil slice
// is null, as encoding/json writes it.
func jsonArray(v reflect.Value, elem func(reflect.Value) ([]byte, error)) ([]byte, error) {
	if v.Kind() == reflect.Interface {
		if v.IsNil() {
			return []byte("null"), nil
		}
		v = v.Elem()
	}
	if v.Kind() == reflect.Slice && v.IsNil() {
		return []byte("null"), nil
	}
	buf := []byte{'['}
	for i := range v.Len() {
		if i > 0 {
			buf = append(buf, ',')
		}
		encoded, err := elem(v.Index(i))
		if err != nil {
			return nil, err
		}
		buf = append(buf, encoded...)
	}
	return append(buf, ']'), nil
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
