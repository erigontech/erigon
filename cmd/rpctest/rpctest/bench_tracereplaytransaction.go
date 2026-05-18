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

package rpctest

import (
	"fmt"
)

func BenchTraceReplayTransaction(erigonUrl, gethUrl string, needCompare bool, blockFrom uint64, blockTo uint64, recordFile string, errorFile string) error {
	setRoutes(erigonUrl, gethUrl)

	rec, errs, cleanup, err := openWriters(recordFile, errorFile)
	if err != nil {
		return err
	}
	defer cleanup()

	reqGen := &RequestGenerator{}

	for bn := blockFrom; bn < blockTo; bn++ {
		b, skip, err := fetchBlock(reqGen, bn, false, nil)
		if err != nil {
			return err
		}
		if skip {
			continue
		}
		for _, txn := range b.Result.Transactions {

			request := reqGen.traceReplayTransaction(txn.Hash)
			errCtx := fmt.Sprintf("block %d, txn %s", bn, txn.Hash)
			if err := requestAndCompare(request, "trace_replayTransaction", errCtx, reqGen, needCompare, rec, errs, nil,
				/* insertOnlyIfSuccess */ false); err != nil {
				fmt.Println(err)
				return err
			}
		}
	}
	return nil
}
