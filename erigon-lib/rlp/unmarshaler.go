// Copyright 2024 The Erigon Authors
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

package rlp

import (
	"fmt"
	"reflect"
)

type Unmarshaler interface {
	UnmarshalRLP(data []byte) error
}

func Unmarshal(data []byte, val any) error {
	buf := newBuf(data, 0)
	return unmarshal(buf, val)
}

func unmarshal(buf *buf, val any) error {
	rv := reflect.ValueOf(val)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("%w: v must be ptr", ErrDecode)
	}
	v := rv.Elem()
	err := reflectAny(buf, v, rv)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDecode, err)
	}
	return nil
}

func reflectAny(w *buf, v reflect.Value, rv reflect.Value) error {
	if um, ok := rv.Interface().(Unmarshaler); ok {
		return um.UnmarshalRLP(w.Bytes())
	}
	// figure out what we are reading
	prefix, err := w.ReadByte()
	if err != nil {
		return err
	}
	token := identifyToken(prefix)
	// switch
	switch token {
	case TokenDecimal:
		// in this case, the value is just the byte itself
		switch v.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			v.SetInt(int64(prefix))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			v.SetUint(uint64(prefix))
		case reflect.Invalid:
			// do nothing
		default:
			return fmt.Errorf("%w: decimal must be unmarshal into integer type", ErrDecode)
		}
	case TokenShortBlob:
		sz := int(token.Diff(prefix))
		str, err := nextFull(w, sz)
		if err != nil {
			return err
		}
		return putBlob(str, v, rv)
	case TokenLongBlob:
		lenSz := int(token.Diff(prefix))
		sz, err := nextBeInt(w, lenSz)
		if err != nil {
			return err
		}
		str, err := nextFull(w, sz)
		if err != nil {
			return err
		}
		return putBlob(str, v, rv)
	case TokenShortList:
		sz := int(token.Diff(prefix))
		buf, err := nextFull(w, sz)
		if err != nil {
			return err
		}
		return reflectList(newBuf(buf, 0), v, rv)
	case TokenLongList:
		lenSz := int(token.Diff(prefix))
		sz, err := nextBeInt(w, lenSz)
		if err != nil {
			return err
		}
		buf, err := nextFull(w, sz)
		if err != nil {
			return err
		}
		return reflectList(newBuf(buf, 0), v, rv)
	case TokenUnknown:
		return fmt.Errorf("%w: unknown token", ErrDecode)
	}
	return nil
}

func putBlob(w []byte, v reflect.Value, rv reflect.Value) error {
	switch v.Kind() {
	case reflect.String:
		v.SetString(string(w))
	case reflect.Slice:
		if v.Type().Elem().Kind() != reflect.Uint8 {
			return fmt.Errorf("%w: need to use uint8 as underlying if want slice output from longstring", ErrDecode)
		}
		v.SetBytes(w)
	case reflect.Array:
		if v.Type().Elem().Kind() != reflect.Uint8 {
			return fmt.Errorf("%w: need to use uint8 as underlying if want array output from longstring", ErrDecode)
		}
		reflect.Copy(v, reflect.ValueOf(w))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		val, err := BeInt(w, 0, len(w))
		if err != nil {
			return err
		}
		v.SetInt(int64(val))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		val, err := BeInt(w, 0, len(w))
		if err != nil {
			return err
		}
		v.SetUint(uint64(val))
	case reflect.Invalid:
		// do nothing
		return nil
	}
	return nil
}

func reflectList(w *buf, v reflect.Value, rv reflect.Value) error {
	switch v.Kind() {
	case reflect.Invalid:
		// do nothing
		return nil
	case reflect.Map:
		rv1 := reflect.New(v.Type().Key())
		v1 := rv1.Elem()
		err := reflectAny(w, v1, rv1)
		if err != nil {
			return err
		}
		rv2 := reflect.New(v.Type().Elem())
		v2 := rv2.Elem()
		err = reflectAny(w, v2, rv2)
		if err != nil {
			return err
		}
		v.SetMapIndex(rv1, rv2)
	case reflect.Struct:
		for idx := 0; idx < v.NumField(); idx++ {
			// Decode into element.
			rv1 := v.Field(idx).Addr()
			rt1 := v.Type().Field(idx)
			v1 := rv1.Elem()
			shouldSet := rt1.IsExported()
			if shouldSet {
				err := reflectAny(w, v1, rv1)
				if err != nil {
					return err
				}
			}
		}
	case reflect.Array, reflect.Slice:
		idx := 0
		for {
			if idx >= v.Cap() {
				v.Grow(1)
			}
			if idx >= v.Len() {
				v.SetLen(idx + 1)
			}
			if idx < v.Len() {
				// Decode into element.
				rv1 := v.Index(idx)
				v1 := rv1.Elem()
				err := reflectAny(w, v1, rv1)
				if err != nil {
					return err
				}
			} else {
				// Ran out of fixed array: skip.
				rv1 := reflect.Value{}
				err := reflectAny(w, rv1, rv1)
				if err != nil {
					return err
				}
			}
			idx++
		}
	}
	return nil
}
