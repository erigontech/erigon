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

package eip8297

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestParallelStreamRootR18WorkerErrorReachesProducer(t *testing.T) {
	builder, err := NewParallelStreamRoot(streamRootHash(HashKeccak), 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	key := []byte{0, 0}
	value := streamRootValue(1)
	if err := builder.AddContext(ctx, key, value); err != nil {
		t.Fatal(err)
	}
	if err := builder.AddContext(ctx, key, value); err != nil {
		t.Fatal(err)
	}
	for shard := 1; shard < 10; shard++ {
		err = builder.AddContext(ctx, []byte{0, byte(shard << 4)}, value)
		if err != nil {
			break
		}
	}
	_, rootErr := builder.RootHash()
	if err == nil || !strings.Contains(err.Error(), "strictly ascending") {
		t.Fatalf("worker ordering error must reach producer; AddContext=%v, RootHash=%v", err, rootErr)
	}
}
