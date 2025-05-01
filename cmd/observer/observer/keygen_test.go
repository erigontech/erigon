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
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/log/v3"
)

func TestKeygen(t *testing.T) {
	targetKeyPair, err := crypto.GenerateKey()
	assert.NotNil(t, targetKeyPair)
	assert.NoError(t, err)

	targetKey := &targetKeyPair.PublicKey
	keys := keygen(context.Background(), targetKey, 50*time.Millisecond, uint(runtime.GOMAXPROCS(-1)), log.Root())

	assert.NotNil(t, keys)
	assert.GreaterOrEqual(t, len(keys), 4)
}
