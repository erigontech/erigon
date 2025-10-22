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

package app

import (
	"os"
	"testing"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
	"github.com/stretchr/testify/require"
)

type bundle struct {
	domain, history, ii *state.E3SnapSchema
}

type RootNum = kv.RootNum

func Test_DeleteLatestStateSnaps(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	b := bundle{}
	for _, dc := range []statecfg.DomainCfg{statecfg.Schema.AccountsDomain, statecfg.Schema.StorageDomain, statecfg.Schema.CodeDomain, statecfg.Schema.ReceiptDomain} {
		b.domain, b.history, b.ii = state.SnapSchemaFromDomainCfg(dc, dirs, 10)
		for i := 0; i < 10; i++ {
			createFiles(t, dirs, i*10, (i+1)*10, &b)
		}
	}

	b.domain, b.history, b.ii = state.SnapSchemaFromDomainCfg(statecfg.Schema.ReceiptDomain, dirs, 10)

	confirmExist(t, b.domain.DataFile(version.V1_0, 90, 100))

	// delete 9-10
	err := DeleteStateSnapshots(dirs, true, false, false, "", "receipt")
	require.NoError(t, err)
	confirmDoesntExist(t, b.domain.DataFile(version.V1_0, 90, 100))

	// should delete 8-9
	err = DeleteStateSnapshots(dirs, true, false, false, "", "receipt")
	require.NoError(t, err)
	confirmDoesntExist(t, b.domain.DataFile(version.V1_0, 80, 90))
}

func confirmExist(t *testing.T, filename string) {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		t.Errorf("file %s does not exist", filename)
	}
}

func confirmDoesntExist(t *testing.T, filename string) {
	if _, err := os.Stat(filename); !os.IsNotExist(err) {
		t.Errorf("file %s exists", filename)
	}
}

func createFiles(t *testing.T, dirs datadir.Dirs, from, to int, b *bundle) {
	t.Helper()

	rootFrom, rootTo := RootNum(from), RootNum(to)

	touchFile := func(filepath string) {
		file, err := os.OpenFile(filepath, os.O_RDONLY|os.O_CREATE, 0644)
		if err != nil {
			panic(err)
		}
		file.Close()
	}

	genFile := func(schema *state.E3SnapSchema) {
		touchFile(schema.DataFile(version.V1_0, rootFrom, rootTo))
		acc := schema.AccessorList()
		if acc.Has(statecfg.AccessorBTree) {
			touchFile(schema.BtIdxFile(version.V1_0, rootFrom, rootTo))
		}
		if acc.Has(statecfg.AccessorExistence) {
			touchFile(schema.ExistenceFile(version.V1_0, rootFrom, rootTo))
		}
		if acc.Has(statecfg.AccessorHashMap) {
			touchFile(schema.AccessorIdxFile(version.V1_0, rootFrom, rootTo, 0))
		}
	}

	genFile(b.domain)
	genFile(b.history)
	genFile(b.ii)
}
