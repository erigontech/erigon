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

package bal

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/execution/types"
)

type regeneratorAdmissionReader struct {
	dbservices.FullBlockReader
	hash          common.Hash
	preflightDone chan struct{}
}

func (r regeneratorAdmissionReader) TxnumReader() rawdbv3.TxNumsReader {
	return rawdbv3.TxNums
}

func (r regeneratorAdmissionReader) Header(context.Context, kv.Getter, common.Hash, uint64) (*types.Header, error) {
	return &types.Header{BlockAccessListHash: &common.Hash{2}}, nil
}

func (r regeneratorAdmissionReader) CanonicalHash(context.Context, kv.Getter, uint64) (common.Hash, bool, error) {
	close(r.preflightDone)
	return r.hash, true, nil
}

func TestRegenerator_CancelledBeforeReplayAdmission(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cached []byte
	}{
		{name: "cache miss"},
		{name: "cache hit", cached: []byte{0xc0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			reader := regeneratorAdmissionReader{hash: common.Hash{1}, preflightDone: make(chan struct{})}
			gen := NewRegenerator(reader, nil, log.New())
			mu := gen.perBlockExecMu.lock(reader.hash)
			var got []byte
			var err error
			admitted := false
			done := make(chan struct{})
			go func() {
				got, err = gen.GetBlockAccessListBytes(ctx, nil, nil, reader.hash, 1, func() error {
					admitted = true
					return errors.New("unexpected replay admission")
				})
				close(done)
			}()

			<-reader.preflightDone
			cancel()
			if tc.cached != nil {
				gen.cache.Add(reader.hash, tc.cached)
			}
			gen.perBlockExecMu.unlock(mu, reader.hash)
			<-done

			require.False(t, admitted, "a cancelled request must not consume replay admission")
			require.Equal(t, tc.cached, got)
			if tc.cached == nil {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
