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

func BenchTraceTransaction(erigonUrl, gethUrl string, needCompare bool, blockFrom uint64, blockTo uint64, recordFileName string, errorFileName string) error {
	fmt.Println("BenchTraceTransaction: fromBlock:", blockFrom, ", blockTo:", blockTo)

	setRoutes(erigonUrl, gethUrl)

	rec, errs, cleanup, err := openWriters(recordFileName, errorFileName)
	if err != nil {
		return err
	}
	defer cleanup()

	var resultsCh chan CallResult = nil
	if !needCompare {
		resultsCh = make(chan CallResult, 1000)
		defer close(resultsCh)
		go vegetaWrite(true, []string{"trace_transaction"}, resultsCh)
	}

	reqGen := &RequestGenerator{}

	var nBlocks = 0
	var nTransactions = 0

	for bn := blockFrom; bn < blockTo; bn++ {
		if nBlocks%50 == 0 {
			fmt.Println("Processing Block: ", bn)
		}
		nBlocks++

		b, skip, err := fetchBlock(reqGen, bn, needCompare, rec)
		if err != nil {
			return err
		}
		if skip {
			continue
		}

		for idx, txn := range b.Result.Transactions {
			if idx%30 != 0 {
				continue
			}
			nTransactions++

			request := reqGen.traceTransaction(txn.Hash)
			errCtx := fmt.Sprintf("block %d, txn %s", bn, txn.Hash)
			if err := requestAndCompare(request, "trace_transaction", errCtx, reqGen, needCompare, rec, errs, resultsCh,
				/* insertOnlyIfSuccess */ false); err != nil {
				fmt.Println(err)
				return err
			}
		}
	}
	fmt.Println("\nProcessed Blocks: ", nBlocks, ", Transactions", nTransactions)

	return nil
}
