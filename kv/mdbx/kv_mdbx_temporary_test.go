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

package mdbx

import (
	"context"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryDatabase(t *testing.T) {
	tmpDb, err := NewTemporaryMdbx()
	require.NoError(t, err)

	defer tmpDb.Close()

	require.NoError(t, tmpDb.Update(context.TODO(), func(tx kv.RwTx) error {
		return tx.Put(kv.Headers, []byte("lol"), []byte{0})
	}))

	require.NoError(t, tmpDb.View(context.TODO(), func(tx kv.Tx) error {
		v, err := tx.GetOne(kv.Headers, []byte("lol"))
		require.NoError(t, err)
		assert.Equal(t, []byte{0}, v)
		return nil
	}))

}
