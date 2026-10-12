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

package rawdb

import (
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
)

func ReadShadowStateRoot(db kv.Getter, hash common.Hash, number uint64) ([]byte, error) {
	return db.GetOne(kv.ShadowStateRoot, dbutils.BlockBodyKey(number, hash))
}

func WriteShadowStateRoot(db kv.Putter, hash common.Hash, number uint64, root []byte) error {
	return db.Put(kv.ShadowStateRoot, dbutils.BlockBodyKey(number, hash), root)
}

func commitmentDomainStoppedKey(domain kv.Domain) []byte {
	return []byte("CommitmentDomainStopped." + domain.String())
}

func WriteCommitmentDomainStopped(db kv.Putter, domain kv.Domain) error {
	return db.Put(kv.DatabaseInfo, commitmentDomainStoppedKey(domain), []byte{1})
}

func ReadCommitmentDomainStopped(db kv.Getter, domain kv.Domain) (bool, error) {
	v, err := db.GetOne(kv.DatabaseInfo, commitmentDomainStoppedKey(domain))
	return len(v) > 0, err
}

func DeleteCommitmentDomainStopped(db kv.RwTx, domain kv.Domain) error {
	return db.Delete(kv.DatabaseInfo, commitmentDomainStoppedKey(domain))
}
