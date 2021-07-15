/*
   Copyright 2021 Erigon contributors

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

package txpool

import "fmt"

const ParseHashErrorPrefix = "parse hash payload"

// ParseHash extracts the next hash from the RLP encoding (payload) from a given position
// it appends the hash to the given slice, reusing the space if there is enough capacity
// The first returned value is the slice where hash is appended to.
// The second returned value is the new position in the RLP payload after the extraction
// of the hash.
func ParseHash(payload []byte, pos int, hashbuf []byte) ([]byte, int, error) {
	dataPos, dataLen, list, err := prefix(payload, pos)
	if err != nil {
		return nil, 0, fmt.Errorf("%s: to len: %w", ParseHashErrorPrefix, err)
	}
	if list {
		return nil, 0, fmt.Errorf("%s: hash must be a string, not list", ParseHashErrorPrefix)
	}
	if dataLen != 32 {
		return nil, 0, fmt.Errorf("%s: hash must be 32 bytes long", ParseHashErrorPrefix)
	}
	var hash []byte
	if total := len(hashbuf) + 32; cap(hashbuf) >= total {
		hash = hashbuf[:32] // Reuse the space in pkbuf, is it has enough capacity
	} else {
		hash = make([]byte, total)
		copy(hash, hashbuf)
	}
	copy(hash, payload[dataPos:dataPos+dataLen])
	return hash, dataPos + dataLen, nil
}
