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

package l1_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
	"github.com/erigontech/erigon/eez/l1"
)

// beaconServer serves slots of blobs; legacy serves them only as
// blob_sidecars, as a node before Fulu does. A nonzero retention is the
// epochs of 16 slots its spec keeps blobs for.
func beaconServer(t *testing.T, genesis, secondsPerSlot uint64, slots map[uint64][][]byte, legacy bool, retention uint64) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		write := func(v any) { require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"data": v})) }
		slotOf := func(prefix string) ([][]byte, bool) {
			n, err := strconv.ParseUint(strings.TrimPrefix(r.URL.Path, prefix), 10, 64)
			require.NoError(t, err)
			blobs, ok := slots[n]
			return blobs, ok
		}
		switch path := r.URL.Path; {
		case path == "/eth/v1/beacon/genesis":
			write(map[string]string{"genesis_time": strconv.FormatUint(genesis, 10)})
		case path == "/eth/v1/config/spec":
			spec := map[string]string{"SECONDS_PER_SLOT": strconv.FormatUint(secondsPerSlot, 10)}
			if retention > 0 {
				spec["SLOTS_PER_EPOCH"], spec["MIN_EPOCHS_FOR_BLOB_SIDECARS_REQUESTS"] = "16", strconv.FormatUint(retention, 10)
			}
			write(spec)
		case strings.HasPrefix(path, "/eth/v1/beacon/blobs/") && !legacy:
			blobs, ok := slotOf("/eth/v1/beacon/blobs/")
			if !ok {
				http.NotFound(w, r)
				return
			}
			out := make([]hexutil.Bytes, len(blobs))
			for i, b := range blobs {
				out[i] = b
			}
			write(out)
		case strings.HasPrefix(path, "/eth/v1/beacon/blob_sidecars/"):
			blobs, ok := slotOf("/eth/v1/beacon/blob_sidecars/")
			if !ok {
				http.NotFound(w, r)
				return
			}
			out := make([]map[string]any, len(blobs))
			for i, b := range blobs {
				// The commitment a node advertises is never read.
				out[i] = map[string]any{"index": strconv.Itoa(i), "blob": hexutil.Bytes(b), "kzg_commitment": "0x00"}
			}
			write(out)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestBeaconBlobs(t *testing.T) {
	replay, err := testfixtures.LoadEEZDevReplay()
	require.NoError(t, err)
	slots := map[uint64][][]byte{}
	for _, b := range replay.Batches {
		slots[b.Slot] = b.Blobs()
	}
	for _, legacy := range []bool{false, true} {
		srv := beaconServer(t, replay.BeaconGenesisTime, replay.SecondsPerSlot, slots, legacy, 0)
		beacon := l1.NewBeacon(srv.URL + "/")
		for i, b := range replay.Batches {
			h := &l1.Header{Number: uint64(b.L1Block.Number), Hash: b.L1Block.Hash, Time: uint64(b.L1Block.Timestamp)}
			blobs, err := beacon.Blobs(context.Background(), h, b.Transaction.BlobVersionedHashes)
			require.NoError(t, err, "batch %d", i)
			require.Len(t, blobs, len(b.Transaction.BlobVersionedHashes))
			for j, blob := range blobs {
				hash, err := l1.VersionedHash(blob)
				require.NoError(t, err)
				require.Equal(t, b.Transaction.BlobVersionedHashes[j], hash)
			}
		}
	}
}

func TestBeaconBlobsRejects(t *testing.T) {
	replay, err := testfixtures.LoadEEZDevReplay()
	require.NoError(t, err)
	b := replay.Batches[0]
	h := &l1.Header{Number: uint64(b.L1Block.Number), Time: uint64(b.L1Block.Timestamp)}
	hashes := b.Transaction.BlobVersionedHashes
	ctx := context.Background()

	tampered := b.Blobs()
	tampered[0][100] ^= 1
	srv := beaconServer(t, replay.BeaconGenesisTime, replay.SecondsPerSlot, map[uint64][][]byte{b.Slot: tampered}, false, 0)
	_, err = l1.NewBeacon(srv.URL).Blobs(ctx, h, hashes)
	require.ErrorIs(t, err, l1.ErrBlobUnavailable)

	// A blob that is no field element vector.
	invalid := b.Blobs()
	invalid[0][0] = 0xff
	srv = beaconServer(t, replay.BeaconGenesisTime, replay.SecondsPerSlot, map[uint64][][]byte{b.Slot: invalid}, false, 0)
	_, err = l1.NewBeacon(srv.URL).Blobs(ctx, h, hashes)
	require.ErrorIs(t, err, l1.ErrInconsistent)

	srv = beaconServer(t, replay.BeaconGenesisTime, replay.SecondsPerSlot, map[uint64][][]byte{}, false, 0)
	beacon := l1.NewBeacon(srv.URL)
	_, err = beacon.Blobs(ctx, h, hashes)
	require.ErrorIs(t, err, l1.ErrBlobUnavailable)
	_, err = beacon.Blobs(ctx, &l1.Header{Time: h.Time + 1}, hashes)
	require.ErrorIs(t, err, l1.ErrInconsistent)

	_, err = l1.MatchBlobs(b.Blobs(), []common.Hash{{1}})
	require.ErrorIs(t, err, l1.ErrBlobUnavailable)
	_, err = l1.MatchBlobs([][]byte{{1}}, hashes)
	require.ErrorIs(t, err, l1.ErrInconsistent)
}

// TestBeaconBlobsLost requires the blobs of a slot past the node's
// retention to be lost when it does not serve them, and those of a recent
// slot to be unavailable only.
func TestBeaconBlobsLost(t *testing.T) {
	replay, err := testfixtures.LoadEEZDevReplay()
	require.NoError(t, err)
	b := replay.Batches[0]
	hashes := b.Transaction.BlobVersionedHashes
	const secondsPerSlot, epochs, head = 5, 10, 1000
	genesis := uint64(time.Now().Unix()) - head*secondsPerSlot
	old, recent := uint64(head-epochs*16-2), uint64(head-2)
	// old+1 serves no blobs; old+2 is kept by an archive node.
	slots := map[uint64][][]byte{old + 1: {}, old + 2: b.Blobs()}
	beacon := l1.NewBeacon(beaconServer(t, genesis, secondsPerSlot, slots, false, epochs).URL)
	at := func(slot uint64) *l1.Header { return &l1.Header{Number: slot, Time: genesis + slot*secondsPerSlot} }
	ctx := context.Background()

	for _, slot := range []uint64{old, old + 1} {
		_, err = beacon.Blobs(ctx, at(slot), hashes)
		require.ErrorIs(t, err, l1.ErrBlobLost, "slot %d", slot)
	}
	_, err = beacon.Blobs(ctx, at(old+2), hashes)
	require.NoError(t, err)
	_, err = beacon.Blobs(ctx, at(recent), hashes)
	require.ErrorIs(t, err, l1.ErrBlobUnavailable)
	require.NotErrorIs(t, err, l1.ErrBlobLost)
}
