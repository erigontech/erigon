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

package kv

import (
	"errors"

	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
)

// IndexKey is a key of an inverted index.
type IndexKey struct {
	Idx InvertedIdx
	Key []byte
}

// IndexQuery selects the timestamps in [FromTs, ToTs) that match every clause; a clause matches the
// timestamps of any of its keys. FromTs, ToTs and Asc have the semantics of TemporalTx.IndexRange.
type IndexQuery struct {
	Clauses [][]IndexKey
	FromTs  int
	ToTs    int
	Asc     order.By
}

var ErrEmptyIndexQuery = errors.New("index query needs at least one clause, and each clause at least one key")

// JoinIndexRanges evaluates q over the streams that indexRange returns for each key.
func JoinIndexRanges(q IndexQuery, indexRange func(name InvertedIdx, k []byte, fromTs, toTs int, asc order.By, limit int) (stream.U64, error)) (stream.U64, error) {
	if len(q.Clauses) == 0 {
		return nil, ErrEmptyIndexQuery
	}
	for _, clause := range q.Clauses {
		if len(clause) == 0 {
			return nil, ErrEmptyIndexQuery
		}
	}
	// Union and Intersect drop empty streams without closing them, so on error close every opened stream.
	var opened []stream.U64
	var res stream.U64
	for _, clause := range q.Clauses {
		var union stream.U64
		for _, k := range clause {
			it, err := indexRange(k.Idx, k.Key, q.FromTs, q.ToTs, q.Asc, Unlim)
			if err != nil {
				for _, o := range opened {
					o.Close()
				}
				return nil, err
			}
			opened = append(opened, it)
			union = stream.Union[uint64](union, it, q.Asc, Unlim)
		}
		if res == nil {
			res = union
			continue
		}
		res = stream.Intersect[uint64](res, union, q.Asc, Unlim)
	}
	return res, nil
}
