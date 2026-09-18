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

package jsonw

import "encoding"

// JSONWriter is the JSON stream a MarshalFastJSONTo writes into, in the manner of json/v2's jsontext.Encoder.
type JSONWriter interface {
	// WriteHex writes b as a 0x-prefixed hex string.
	WriteHex(b []byte)
	// WriteRawBytes writes already-encoded JSON.
	WriteRawBytes(b []byte)
	// WriteQuotedText writes v.AppendText's output as a JSON string. The text must need no
	// escaping: callers pass hex quantities.
	WriteQuotedText(v encoding.TextAppender)
	// WriteString writes s as an escaped JSON string.
	WriteString(s string)
	WriteNil()
	WriteObjectStart()
	// WriteObjectField returns the writer, so a field and its value can be chained.
	WriteObjectField(name string) JSONWriter
	WriteObjectEnd()
	WriteArrayStart()
	WriteMore()
	WriteArrayEnd()
}
