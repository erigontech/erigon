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

package observer

import (
	"context"
	"crypto/ecdsa"
	"time"

	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/p2p/enode"
)

func keygen(
	parentContext context.Context,
	targetKey *ecdsa.PublicKey,
	timeout time.Duration,
	concurrencyLimit uint,
	logger log.Logger,
) []*ecdsa.PublicKey {
	ctx, cancel := context.WithTimeout(parentContext, timeout)
	defer cancel()

	targetID := enode.PubkeyToIDV4(targetKey)
	cpus := concurrencyLimit

	type result struct {
		key      *ecdsa.PublicKey
		distance int
	}

	generatedKeys := make(chan result, cpus)

	for i := uint(0); i < cpus; i++ {
		go func() {
			for ctx.Err() == nil {
				keyPair, err := crypto.GenerateKey()
				if err != nil {
					logger.Error("keygen has failed to generate a key", "err", err)
					break
				}

				key := &keyPair.PublicKey
				id := enode.PubkeyToIDV4(key)
				distance := enode.LogDist(targetID, id)

				select {
				case generatedKeys <- result{key, distance}:
				case <-ctx.Done():
					break
				}
			}
		}()
	}

	keysAtDist := make(map[int]*ecdsa.PublicKey)

	for ctx.Err() == nil {
		select {
		case res := <-generatedKeys:
			keysAtDist[res.distance] = res.key
		case <-ctx.Done():
			break
		}
	}

	keys := valuesOfIntToPubkeyMap(keysAtDist)

	return keys
}

func valuesOfIntToPubkeyMap(m map[int]*ecdsa.PublicKey) []*ecdsa.PublicKey {
	values := make([]*ecdsa.PublicKey, 0, len(m))
	for _, value := range m {
		values = append(values, value)
	}
	return values
}
