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

package kvcfg

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/kv"
)

type ConfigKey []byte

var (
	HistoryV3 = ConfigKey("history.v3")
)

func (k ConfigKey) Enabled(tx kv.Tx) (bool, error) { return kv.GetBool(tx, kv.DatabaseInfo, k) }
func (k ConfigKey) FromDB(db kv.RoDB) (enabled bool) {
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		var err error
		enabled, err = k.Enabled(tx)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		panic(err)
	}
	return enabled
}
func (k ConfigKey) WriteOnce(tx kv.RwTx, v bool) (bool, error) {
	_, enabled, err := kv.EnsureNotChangedBool(tx, kv.DatabaseInfo, k, v)
	return enabled, err
}
func (k ConfigKey) EnsureNotChanged(tx kv.RwTx, value bool) (ok, enabled bool, err error) {
	return kv.EnsureNotChangedBool(tx, kv.DatabaseInfo, k, value)
}
func (k ConfigKey) ForceWrite(tx kv.RwTx, enabled bool) error {
	if enabled {
		if err := tx.Put(kv.DatabaseInfo, k, []byte{1}); err != nil {
			return err
		}
	} else {
		if err := tx.Put(kv.DatabaseInfo, k, []byte{0}); err != nil {
			return err
		}
	}
	return nil
}
