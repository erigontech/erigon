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

package auth

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/stretchr/testify/require"
)

func newTestDB(t *testing.T) kv.RwDB {
	t.Helper()
	db := mdbx.New(dbcfg.ConsensusDB, log.Root()).
		InMem(t, t.TempDir()).
		WithTableCfg(AuthTablesCfg).
		MustOpen()
	t.Cleanup(db.Close)
	return db
}

func TestMDBXStorePutGetToken(t *testing.T) {
	db := newTestDB(t)
	store := NewMDBXStore(db)
	ctx := context.Background()

	nonce := make([]byte, 16)
	rand.Read(nonce)

	token := &Token{
		Issuer:    "did:pkh:eip155:1:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Audience:  "did:pkh:eip155:1:0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Command:   "/storage/read",
		Nonce:     nonce,
		Exp:       uint64(time.Now().Add(1 * time.Hour).Unix()),
		Signature: []byte{0x01, 0x02, 0x03},
	}
	cid := token.ComputeCID()

	// Put
	require.NoError(t, store.PutToken(ctx, cid, token))

	// Get
	got, err := store.GetToken(ctx, cid)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, token.Issuer, got.Issuer)
	require.Equal(t, token.Audience, got.Audience)
	require.Equal(t, token.Command, got.Command)
	require.Equal(t, token.Exp, got.Exp)

	// Get non-existent
	got, err = store.GetToken(ctx, common.Hash{0xff})
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestMDBXStoreRevocation(t *testing.T) {
	db := newTestDB(t)
	store := NewMDBXStore(db)
	ctx := context.Background()

	cid := common.Hash{0x01}
	issuer := "did:pkh:eip155:1:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	// Not revoked initially
	revoked, err := store.IsRevoked(ctx, cid, issuer, 1000)
	require.NoError(t, err)
	require.False(t, revoked)

	// Revoke specific token
	require.NoError(t, store.Revoke(ctx, cid))
	revoked, err = store.IsRevoked(ctx, cid, issuer, 1000)
	require.NoError(t, err)
	require.True(t, revoked)

	// Bulk revocation
	cid2 := common.Hash{0x02}
	require.NoError(t, store.RevokeAllBefore(ctx, issuer, 2000))

	revoked, err = store.IsRevoked(ctx, cid2, issuer, 1500) // before timestamp
	require.NoError(t, err)
	require.True(t, revoked)

	revoked, err = store.IsRevoked(ctx, cid2, issuer, 2500) // after timestamp
	require.NoError(t, err)
	require.False(t, revoked)
}

func TestMDBXStoreAsResolver(t *testing.T) {
	db := newTestDB(t)
	store := NewMDBXStore(db)
	ctx := context.Background()

	nonce := make([]byte, 16)
	rand.Read(nonce)

	token := &Token{
		Issuer:    "did:pkh:eip155:1:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Audience:  "did:pkh:eip155:1:0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Command:   "/storage/*",
		Nonce:     nonce,
		Exp:       uint64(time.Now().Add(1 * time.Hour).Unix()),
		Signature: []byte{0x01},
	}
	cid := token.ComputeCID()
	require.NoError(t, store.PutToken(ctx, cid, token))

	// Use as TokenResolver via StoreResolver adapter
	resolver := NewStoreResolver(store)
	got, err := resolver.Resolve(ctx, cid)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, token.Command, got.Command)
}
