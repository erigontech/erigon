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

// BenchEthCreateAccessList compares response of Erigon with Geth
// but also can be used for comparing RPCDaemon with Geth or infura
// parameters:
// needCompare - if false - doesn't call Erigon and doesn't compare responses
//
//			    false value - to generate vegeta files, it's faster but we can generate vegeta files for Geth and Erigon
//	                 recordFile stores all eth_call returned with success
//	                 errorFile stores information when erigon and geth doesn't return same data
func BenchEthCreateAccessList(erigonURL, gethURL string, needCompare, latest bool, blockFrom, blockTo uint64, recordFileName string, errorFileName string) error {
	setRoutes(erigonURL, gethURL)

	rec, errs, cleanup, err := openWriters(recordFileName, errorFileName)
	if err != nil {
		return err
	}
	defer cleanup()

	var resultsCh chan CallResult = nil
	var nTransactions = 0

	if !needCompare {
		resultsCh = make(chan CallResult, 1000)
		defer close(resultsCh)
		go vegetaWrite(true, []string{"eth_createAccessList"}, resultsCh)
	}
	reqGen := &RequestGenerator{}

	for bn := blockFrom; bn <= blockTo; bn++ {

		b, skip, err := fetchBlock(reqGen, bn, needCompare, rec)
		if err != nil {
			return err
		}
		if skip {
			continue
		}

		for _, txn := range b.Result.Transactions {

			nTransactions = nTransactions + 1

			request := reqGen.ethCreateAccessList(txn.From, txn.To, &txn.Gas, &txn.GasPrice, &txn.Value, txn.Input, bn-1)
			errCtx := fmt.Sprintf(" bn=%d hash=%s", bn, txn.Hash)

			if err := requestAndCompare(request, "eth_createAccessList", errCtx, reqGen, needCompare, rec, errs, resultsCh,
				false); err != nil {
				return err
			}
		}

		fmt.Println("\nProcessed Transactions: ", nTransactions)
	}
	return nil
}
