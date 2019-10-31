// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package ethdb

type table struct {
	db     Database
	prefix string
}

// NewTable returns a Database object that prefixes all keys with a given
// string.
func NewTable(db Database, prefix string) Database {
	return &table{
		db:     db,
		prefix: prefix,
	}
}

func (dt *table) Put(bucket, key []byte, value []byte) error {
	return dt.db.Put(bucket, append([]byte(dt.prefix), key...), value)
}

func (dt *table) PutS(hBucket, key, value []byte, timestamp uint64, noHistory bool) error {
	return dt.db.PutS(hBucket, append([]byte(dt.prefix), key...), value, timestamp, noHistory)
}

func (dt *table) MultiPut(tuples ...[]byte) (uint64, error) {
	panic("Not supported")
}

func (dt *table) Has(bucket, key []byte) (bool, error) {
	return dt.db.Has(bucket, append([]byte(dt.prefix), key...))
}

func (dt *table) Get(bucket, key []byte) ([]byte, error) {
	return dt.db.Get(bucket, append([]byte(dt.prefix), key...))
}

func (dt *table) GetS(hBucket, key []byte, timestamp uint64) ([]byte, error) {
	return dt.db.GetS(hBucket, append([]byte(dt.prefix), key...), timestamp)
}

func (dt *table) GetAsOf(bucket, hBucket, key []byte, timestamp uint64) ([]byte, error) {
	return dt.db.GetAsOf(bucket, hBucket, append([]byte(dt.prefix), key...), timestamp)
}

func (dt *table) Walk(bucket, startkey []byte, fixedbits uint, walker func([]byte, []byte) (bool, error)) error {
	return dt.db.Walk(bucket, append([]byte(dt.prefix), startkey...), fixedbits+uint(8*len(dt.prefix)), walker)
}

func (dt *table) MultiWalk(bucket []byte, startkeys [][]byte, fixedbits []uint, walker func(int, []byte, []byte) (bool, error)) error {
	panic("Not implemented")
}

func (dt *table) WalkAsOf(bucket, hBucket, startkey []byte, fixedbits uint, timestamp uint64, walker func([]byte, []byte) (bool, error)) error {
	panic("Not implemented")
}

func (dt *table) MultiWalkAsOf(bucket, hBucket []byte, startkeys [][]byte, fixedbits []uint, timestamp uint64, walker func(int, []byte, []byte) (bool, error)) error {
	return dt.db.MultiWalkAsOf(bucket, hBucket, startkeys, fixedbits, timestamp, walker)
}

func (dt *table) RewindData(timestampSrc, timestampDst uint64, df func(bucket, key, value []byte) error) error {
	return rewindData(dt, timestampSrc, timestampDst, df)
}

func (dt *table) Delete(bucket, key []byte) error {
	return dt.db.Delete(bucket, append([]byte(dt.prefix), key...))
}

func (dt *table) DeleteTimestamp(timestamp uint64) error {
	return dt.db.DeleteTimestamp(timestamp)
}

func (dt *table) Close() {
	// Do nothing; don't close the underlying DB.
}

func (dt *table) NewBatch() DbWithPendingMutations {
	panic("Not supported")
}

func (dt *table) Size() int {
	return dt.db.Size()
}

func (dt *table) Keys() ([][]byte, error) {
	panic("Not supported")
}

func (dt *table) MemCopy() Database {
	panic("Not implemented")
}
