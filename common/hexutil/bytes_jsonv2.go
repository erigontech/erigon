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

//go:build go1.27

package hexutil

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
)

var _ json.MarshalerTo = Bytes(nil)

// MarshalJSONTo appends the hex string into the encoder's buffer. A value that does not fit falls back
// to AppendText, so a one-shot Marshal does not grow a second buffer.
func (b Bytes) MarshalJSONTo(enc *jsontext.Encoder) error {
	buf := enc.AvailableBuffer()
	if cap(buf) < QuotedLen(len(b)) {
		return errors.ErrUnsupported
	}
	return enc.WriteValue(AppendQuoted(buf, b))
}
