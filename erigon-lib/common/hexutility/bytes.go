/*
   Copyright 2023 The Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package hexutility

import (
	"encoding/hex"
	"encoding/json"
	"reflect"
)

var bytesT = reflect.TypeOf(Bytes(nil))

// Bytes marshals/unmarshals as a JSON string with 0x prefix.
// The empty slice marshals as "0x".
type Bytes []byte

const hexPrefix = `0x`

// MarshalText implements encoding.TextMarshaler
func (b Bytes) MarshalText() ([]byte, error) {
	result := make([]byte, len(b)*2+2)
	copy(result, hexPrefix)
	hex.Encode(result[2:], b)
	return result, nil
}

// UnmarshalJSON implements json.Unmarshaler.
func (b *Bytes) UnmarshalJSON(input []byte) error {
	if !isString(input) {
		return &json.UnmarshalTypeError{Value: "non-string", Type: bytesT}
	}
	return wrapTypeError(b.UnmarshalText(input[1:len(input)-1]), bytesT)
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (b *Bytes) UnmarshalText(input []byte) error {
	raw, err := checkText(input, true)
	if err != nil {
		return err
	}
	dec := make([]byte, len(raw)/2)
	_, err = hex.Decode(dec, raw)
	if err == nil {
		*b = dec
	}
	return err
}

// String returns the hex encoding of b.
func (b Bytes) String() string {
	return Encode(b)
}
