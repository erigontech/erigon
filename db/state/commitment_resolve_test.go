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

package state

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
)

func TestResolveCommitmentV3(t *testing.T) {
	type variant int
	const (
		byDefault variant = iota
		legacy
		v3
	)
	for _, test := range []struct {
		name  string
		pin   *bool
		files func(dirs datadir.Dirs)
		keys  [][]byte
		want  variant
	}{
		{name: "fresh datadir keeps the default", files: func(datadir.Dirs) {}},
		{name: "legacy domain file selects legacy", files: func(d datadir.Dirs) { touchFiles(t, d.SnapDomain, "v2.0-commitment.0-1.kv") }, want: legacy},
		{name: "legacy history file selects legacy", files: func(d datadir.Dirs) { touchFiles(t, d.SnapHistory, "v2.0-commitment.0-1.v") }, want: legacy},
		{name: "v3 domain file selects v3", files: func(d datadir.Dirs) { touchFiles(t, d.SnapDomain, "v3.0-commitment.0-1.kv") }, want: v3},
		{name: "mixed files select v3 for the mixed-files check", files: func(d datadir.Dirs) {
			touchFiles(t, d.SnapDomain, "v3.0-commitment.0-1.kv")
			touchFiles(t, d.SnapHistory, "v2.0-commitment.0-1.v")
		}, want: v3},
		{name: "legacy accounts file alone keeps the default", files: func(d datadir.Dirs) { touchFiles(t, d.SnapDomain, "v2.0-accounts.0-1.kv") }},
		{name: "pinned v3 is not switched by legacy files", pin: new(true), files: func(d datadir.Dirs) { touchFiles(t, d.SnapDomain, "v2.0-commitment.0-1.kv") }, want: v3},
		{name: "pinned legacy is not switched by v3 files", pin: new(false), files: func(d datadir.Dirs) { touchFiles(t, d.SnapDomain, "v3.0-commitment.0-1.kv") }, want: legacy},
		{name: "legacy chaindata state selects legacy", files: func(datadir.Dirs) {}, keys: [][]byte{commitment.KeyCommitmentState}, want: legacy},
		{name: "v3 chaindata state selects v3", files: func(datadir.Dirs) {}, keys: [][]byte{commitment.KeyCommitmentV3State}, want: v3},
		{name: "both chaindata states follow the v3 key", files: func(datadir.Dirs) {}, keys: [][]byte{commitment.KeyCommitmentState, commitment.KeyCommitmentV3State}, want: v3},
		{name: "a v3 file wins over a legacy chaindata state", files: func(d datadir.Dirs) { touchFiles(t, d.SnapDomain, "v3.0-commitment.0-1.kv") }, keys: [][]byte{commitment.KeyCommitmentState}, want: v3},
		{name: "a legacy file wins over a v3 chaindata state", files: func(d datadir.Dirs) { touchFiles(t, d.SnapDomain, "v2.0-commitment.0-1.kv") }, keys: [][]byte{commitment.KeyCommitmentV3State}, want: legacy},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Cleanup(statecfg.ResetCommitmentV3ForTest())
			if test.pin != nil {
				statecfg.SetCommitmentV3(*test.pin)
			}
			logger := log.New()
			dirs := datadir.New(t.TempDir())
			test.files(dirs)
			if test.keys != nil {
				db := mdbx.New(dbcfg.ChainDB, logger).Path(dirs.Chaindata).MustOpen()
				writeCommitmentStateKeys(t, db, test.keys...)
				db.Close()
			}

			require.NoError(t, ResolveCommitmentV3(context.Background(), dirs, nil, logger))
			wantV3 := test.want == v3 || test.want == byDefault && statecfg.DefaultCommitmentV3
			require.Equal(t, wantV3, statecfg.ExperimentalCommitmentV3)
			require.Equal(t, wantV3, statecfg.Schema.CommitmentDomain.CommitmentV3Records)
			require.Equal(t, test.want != byDefault, statecfg.CommitmentV3Pinned())
		})
	}
}

func TestResolveCommitmentV3ReadsAnOpenChaindata(t *testing.T) {
	t.Cleanup(statecfg.ResetCommitmentV3ForTest())
	for _, key := range [][]byte{commitment.KeyCommitmentState, commitment.KeyCommitmentV3State} {
		statecfg.ResetCommitmentV3ForTest()
		logger := log.New()
		dirs := datadir.New(t.TempDir())
		db := mdbx.New(dbcfg.ChainDB, logger).Path(dirs.Chaindata).MustOpen()
		writeCommitmentStateKeys(t, db, key)

		require.NoError(t, ResolveCommitmentV3(context.Background(), dirs, db, logger))
		db.Close()
		require.Equal(t, !bytes.Equal(key, commitment.KeyCommitmentState), statecfg.ExperimentalCommitmentV3, "%q", key)
	}
}

func writeCommitmentStateKeys(t *testing.T, db kv.RwDB, keys ...[]byte) {
	require.NoError(t, db.Update(context.Background(), func(tx kv.RwTx) error {
		for _, key := range keys {
			if err := tx.Put(kv.TblCommitmentVals, key, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe, 1}); err != nil {
				return err
			}
		}
		return nil
	}))
}
