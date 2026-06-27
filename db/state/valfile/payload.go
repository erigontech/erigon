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

package valfile

import "fmt"

// The payload after a domain value's ^step prefix is `flag(1B) || rest`: inline
// bytes or a Handle. The inline/external threshold policy lives at the call site.
const (
	flagInline   byte = 0
	flagExternal byte = 1
)

// EncodeInline appends `flagInline || v` to dst.
func EncodeInline(dst, v []byte) []byte {
	dst = append(dst, flagInline)
	return append(dst, v...)
}

// EncodeExternal appends `flagExternal || handle` to dst.
func EncodeExternal(dst []byte, h Handle) []byte {
	dst = append(dst, flagExternal)
	return h.AppendTo(dst)
}

// DecodePayload returns the value for a `flag || rest` payload. Inline payloads
// return their bytes directly (get is not consulted); external payloads decode
// the handle and call get to fetch from the value-file. dst is an optional reuse
// buffer passed through to get.
func DecodePayload(payload, dst []byte, get func(Handle, []byte) ([]byte, error)) ([]byte, error) {
	if len(payload) == 0 {
		return nil, fmt.Errorf("valfile: empty payload")
	}
	switch payload[0] {
	case flagInline:
		return payload[1:], nil
	case flagExternal:
		h, _, err := DecodeHandle(payload[1:])
		if err != nil {
			return nil, err
		}
		return get(h, dst)
	default:
		return nil, fmt.Errorf("valfile: unknown payload flag %d", payload[0])
	}
}
