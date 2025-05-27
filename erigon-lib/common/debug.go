// Copyright 2015 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package common

import (
	"crypto/rand"
	"math/big"
	"time"
)

// RandomizeDuration - periodic parallel actions may interfere and resonance.
// Use this func to add small randomness to period
func RandomizeDuration(in time.Duration) time.Duration {
	randDuration, err := rand.Int(rand.Reader, big.NewInt(int64(time.Second)))
	if err != nil {
		panic(err)
	}
	return in + time.Duration(randDuration.Uint64())
}
