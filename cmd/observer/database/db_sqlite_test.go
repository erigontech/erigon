package database

import (
	"context"
	"net"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDBSQLiteInsertAndFind(t *testing.T) {
	ctx := context.Background()
	db, err := NewDBSQLite(filepath.Join(t.TempDir(), "observer.sqlite"))
	require.Nil(t, err)
	defer func() { _ = db.Close() }()

	var id NodeID = "ba85011c70bcc5c04d8607d3a0ed29aa6179c092cbdda10d5d32684fb33ed01bd94f588ca8f91ac48318087dcb02eaf36773a7a453f0eedd6742af668097b29c"
	var addr NodeAddr
	addr.IP = net.ParseIP("10.0.1.16")
	addr.PortRLPx = 30303
	addr.PortDisc = 30304

	err = db.UpsertNodeAddr(ctx, id, addr)
	require.Nil(t, err)

	candidates, err := db.FindCandidates(ctx, 1)
	require.Nil(t, err)
	require.Equal(t, 1, len(candidates))

	candidateID := candidates[0]
	assert.Equal(t, id, candidateID)

	candidate, err := db.FindNodeAddr(ctx, candidateID)
	require.Nil(t, err)

	assert.Equal(t, addr.IP, candidate.IP)
	assert.Equal(t, addr.PortDisc, candidate.PortDisc)
	assert.Equal(t, addr.PortRLPx, candidate.PortRLPx)
}
