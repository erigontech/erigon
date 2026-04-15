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

package random

import (
	crand "crypto/rand"
	mrand "math/rand"
	"sync"
	"time"
)

var randsrc = mrand.NewSource(time.Now().UnixNano())

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

var randomMutex sync.Mutex

func RandomBytes(n uint) []byte {
	randomMutex.Lock()
	defer randomMutex.Unlock()

	b := make([]byte, n)

	if n > 0 {
		// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
		for i, cache, remain := int(n-1), randsrc.Int63(), letterIdxMax; i >= 0; {
			if remain == 0 {
				cache, remain = randsrc.Int63(), letterIdxMax
			}
			if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
				b[i] = letterBytes[idx]
				i--
			}
			cache >>= letterIdxBits
			remain--
		}
	}

	return b
}

func RandomString(n uint) string {
	return string(RandomBytes(n))
}

func CreateNonce() ([]byte, error) {
	var buffer [16]byte

	for i := 0; i < len(buffer); {
		n, err := crand.Read(buffer[i:])
		if err != nil {
			return nil, err
		}
		i += n
	}
	return buffer[:], nil
}
