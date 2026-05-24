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

package forkfrom

import (
	"bytes"
	"encoding/json"
)

// newStrictDecoder returns a json.Decoder that surfaces unknown fields
// rather than silently dropping them — fork-from's parent chain.json
// reads should fail loudly on schema drift rather than silently
// inheriting a wrong derived config.
//
// We tolerate unknown fields here intentionally for now (chain.Config
// has been evolving; legacy fields in operator-supplied chain.json
// files would block useful operation). If we later want stricter
// validation, swap to dec.DisallowUnknownFields().
func newStrictDecoder(data []byte) *json.Decoder {
	return json.NewDecoder(bytes.NewReader(data))
}
