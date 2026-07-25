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

// Compares response of Erigon with Geth
// but also can be used for comparing RPCDaemon with OpenEthereum
// parameters:
// needCompare - if false - doesn't call Erigon and doesn't compare responses
func BenchTraceBlock(erigonURL, oeURL string, needCompare bool, blockFrom uint64, blockTo uint64, recordFile string, errorFile string) error {
	setRoutes(erigonURL, oeURL)

	rec, errs, cleanup, err := openWriters(recordFile, errorFile)
	if err != nil {
		return err
	}
	defer cleanup()

	var res CallResult
	reqGen := &RequestGenerator{}

	var blockNumber EthBlockNumber
	res = reqGen.Erigon("eth_blockNumber", reqGen.blockNumber(), &blockNumber)
	if res.Err != nil {
		return fmt.Errorf("Could not get block number: %v\n", res.Err)
	}
	if blockNumber.Error != nil {
		return fmt.Errorf("Error getting block number: %d %s\n", blockNumber.Error.Code, blockNumber.Error.Message)
	}
	fmt.Printf("Last block: %d\n", blockNumber.Number)
	for bn := blockFrom; bn <= blockTo; bn++ {
		_, skip, err := fetchBlock(reqGen, bn, false, nil)
		if err != nil {
			return err
		}
		if skip {
			continue
		}

		request := reqGen.traceBlock(bn)
		errCtx := fmt.Sprintf("block %d", bn)
		if err := requestAndCompare(request, "trace_block", errCtx, reqGen, needCompare, rec, errs, nil /* insertOnlyIfSuccess */, false); err != nil {
			fmt.Println(err)
			return err
		}
	}
	return nil
}
