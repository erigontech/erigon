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

func BenchDebugTraceBlockByNumber(erigonUrl, gethUrl string, needCompare bool, blockFrom uint64, blockTo uint64, recordFileName string, errorFileName string) error {
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
		go vegetaWrite(true, []string{"debug_traceBlockByNumber"}, resultsCh)
	}

	reqGen := &RequestGenerator{}

	var nBlocks = 0
	for bn := blockFrom; bn < blockTo; bn++ {
		nBlocks++

		request := reqGen.debugTraceBlockByNumber(bn)
		errCtx := fmt.Sprintf("block %d", bn)
		if err := requestAndCompare(request, "debug_traceBlockByNumber", errCtx, reqGen, needCompare, rec, errs, resultsCh /* insertOnlyIfSuccess */, false); err != nil {
			return err
		}
	}
	fmt.Println("\nProcessed Blocks: ", nBlocks)

	return nil
}

func BenchDebugTraceBlockByHash(erigonUrl, gethUrl string, needCompare bool, blockFrom uint64, blockTo uint64, recordFile string, errorFile string) error {
	setRoutes(erigonUrl, gethUrl)

	rec, errs, cleanup, err := openWriters(recordFile, errorFile)
	if err != nil {
		return err
	}
	defer cleanup()

	var resultsCh chan CallResult = nil
	if !needCompare {
		resultsCh = make(chan CallResult, 1000)
		defer close(resultsCh)
		go vegetaWrite(true, []string{"debug_traceBlockByHash"}, resultsCh)
	}

	reqGen := &RequestGenerator{}

	var nBlocks = 0
	for bn := blockFrom; bn < blockTo; bn++ {
		b, skip, err := fetchBlock(reqGen, bn, false, nil)
		if err != nil {
			return err
		}
		if skip {
			continue
		}

		nBlocks++

		request := reqGen.traceBlockByHash(b.Result.Hash.Hex())
		errCtx := fmt.Sprintf("block %d, txn %s", bn, b.Result.Hash.Hex())
		if err := requestAndCompare(request, "debug_traceBlockByHash", errCtx, reqGen, needCompare, rec, errs, resultsCh,
			/* insertOnlyIfSuccess */ false); err != nil {
			fmt.Println(err)
			return err
		}
	}
	fmt.Println("\nProcessed Blocks: ", nBlocks)

	return nil
}

func BenchDebugTraceTransaction(erigonUrl, gethUrl string, needCompare bool, blockFrom uint64, blockTo uint64, additionalParams string, recordFileName string, errorFileName string) error {
	fmt.Println("BenchDebugTraceTransaction: fromBlock:", blockFrom, ", blockTo:", blockTo, ", additionalParams:", additionalParams)

	setRoutes(erigonUrl, gethUrl)

	if additionalParams == "" {
		additionalParams = "\"disableStorage\": true,\"disableMemory\": true,\"disableStack\": true"
	}

	rec, errs, cleanup, err := openWriters(recordFileName, errorFileName)
	if err != nil {
		return err
	}
	defer cleanup()

	var resultsCh chan CallResult = nil
	if !needCompare {
		resultsCh = make(chan CallResult, 1000)
		defer close(resultsCh)
		go vegetaWrite(true, []string{"debug_traceTransaction"}, resultsCh)
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

			request := reqGen.debugTraceTransaction(txn.Hash, additionalParams)
			errCtx := fmt.Sprintf("bn=%d hash=%s", bn, txn.Hash)

			if err := requestAndCompare(request, "debug_traceTransaction", errCtx, reqGen, needCompare, rec, errs, resultsCh,
				/* insertOnlyIfSuccess */ false); err != nil {
				return err
			}
		}
	}
	fmt.Println("\nProcessed Blocks: ", nBlocks, ", Transactions", nTransactions)

	return nil
}

func BenchDebugTraceCall(erigonURL, gethURL string, needCompare bool, blockFrom uint64, blockTo uint64, recordFile string, errorFile string) error {
	setRoutes(erigonURL, gethURL)

	rec, errs, cleanup, err := openWriters(recordFile, errorFile)
	if err != nil {
		return err
	}
	defer cleanup()

	var resultsCh chan CallResult = nil
	if !needCompare {
		resultsCh = make(chan CallResult, 1000)
		defer close(resultsCh)
		go vegetaWrite(true, []string{"debug_traceCall"}, resultsCh)
	}

	reqGen := &RequestGenerator{}

	var res CallResult

	var blockNumber EthBlockNumber
	res = reqGen.Erigon("eth_blockNumber", reqGen.blockNumber(), &blockNumber)
	if res.Err != nil {
		return fmt.Errorf("Could not get block number: %v\n", res.Err)
	}
	if blockNumber.Error != nil {
		return fmt.Errorf("Error getting block number: %d %s\n", blockNumber.Error.Code, blockNumber.Error.Message)
	}
	fmt.Printf("Last block: %d\n", blockNumber.Number)

	var nBlocks = 0
	var nTransactions = 0
	for bn := blockFrom; bn <= blockTo; bn++ {
		b, skip, err := fetchBlock(reqGen, bn, needCompare, nil)
		if err != nil {
			return err
		}
		if skip {
			continue
		}
		nBlocks++

		for _, txn := range b.Result.Transactions {
			nTransactions++

			request := reqGen.debugTraceCall(txn.From, txn.To, &txn.Gas, &txn.GasPrice, &txn.Value, txn.Input, bn-1)
			errCtx := fmt.Sprintf("block %d txn %s", bn, txn.Hash)
			if err := requestAndCompare(request, "debug_traceCall", errCtx, reqGen, needCompare, rec, errs, resultsCh,
				/* insertOnlyIfSuccess*/ false); err != nil {
				fmt.Println(err)
				return err
			}
		}
	}
	fmt.Println("\nProcessed Blocks: ", nBlocks, ", Transactions", nTransactions)

	return nil
}
