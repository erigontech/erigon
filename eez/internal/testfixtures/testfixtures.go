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

// Package testfixtures loads the EEZ vectors and recordings vendored under
// eez/testdata. Provenance of every file is in eez/testdata/SOURCES.md.
package testfixtures

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
)

// ProtocolCommit is the eez-core-protocol revision the vendored vectors pin.
const ProtocolCommit = "855fe0602484750861b4c31502a1a91355d33ed6"

// Dir returns the absolute path of eez/testdata.
func Dir() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("testfixtures: cannot locate source file")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "testdata")
}

// Path returns the absolute path of a file under eez/testdata.
func Path(elem ...string) string {
	return filepath.Join(append([]string{Dir()}, elem...)...)
}

// decodeStrict unmarshals data into v and rejects unknown fields and trailing
// values, so a fixture with an unexpected schema fails instead of loading
// partially.
func decodeStrict(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if dec.More() {
		return fmt.Errorf("trailing data after JSON value")
	}
	return nil
}
