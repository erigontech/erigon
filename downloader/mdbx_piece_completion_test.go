/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package downloader

import (
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
)

func TestMdbxPieceCompletion(t *testing.T) {
	db := memdb.NewTestDownloaderDB(t)
	pc, err := NewMdbxPieceCompletion(db)
	require.NoError(t, err)
	defer pc.Close()

	pk := metainfo.PieceKey{}

	b, err := pc.Get(pk)
	require.NoError(t, err)
	assert.False(t, b.Ok)

	require.NoError(t, pc.Set(pk, false))

	b, err = pc.Get(pk)
	require.NoError(t, err)
	assert.Equal(t, storage.Completion{Complete: false, Ok: true}, b)

	require.NoError(t, pc.Set(pk, true))

	b, err = pc.Get(pk)
	require.NoError(t, err)
	assert.Equal(t, storage.Completion{Complete: true, Ok: true}, b)
}
