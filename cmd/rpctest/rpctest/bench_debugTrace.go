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
	"bufio"
	"fmt"
	"os"
)

func BenchDebugTraceBlockByNumber(erigonUrl, gethUrl string, needCompare bool, blockFrom uint64, blockTo uint64, recordFileName string, errorFileName string) error {
	setRoutes(erigonUrl, gethUrl)

	var rec *bufio.Writer
	if recordFileName != "" {
		f, err := os.Create(recordFileName)
		if err != nil {
			return fmt.Errorf("Cannot create file %s for recording: %v\n", recordFileName, err)
		}
		defer f.Close()
		rec = bufio.NewWriter(f)
		defer rec.Flush()
	}
	var errs *bufio.Writer
	if errorFileName != "" {
		ferr, err := os.Create(errorFileName)
		if err != nil {
			return fmt.Errorf("Cannot create file %s for error output: %v\n", errorFileName, err)
		}
		defer ferr.Close()
		errs = bufio.NewWriter(ferr)
		defer errs.Flush()
	}

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

	var rec *bufio.Writer
	if recordFile != "" {
		f, err := os.Create(recordFile)
		if err != nil {
			return fmt.Errorf("Cannot create file %s for recording: %v\n", recordFile, err)
		}
		defer f.Close()
		rec = bufio.NewWriter(f)
		defer rec.Flush()
	}
	var errs *bufio.Writer
	if errorFile != "" {
		ferr, err := os.Create(errorFile)
		if err != nil {
			return fmt.Errorf("Cannot create file %s for error output: %v\n", errorFile, err)
		}
		defer ferr.Close()
		errs = bufio.NewWriter(ferr)
		defer errs.Flush()
	}

	var resultsCh chan CallResult = nil
	if !needCompare {
		resultsCh = make(chan CallResult, 1000)
		defer close(resultsCh)
		go vegetaWrite(true, []string{"debug_traceBlockByHash"}, resultsCh)
	}

	reqGen := &RequestGenerator{}

	var res CallResult
	var nBlocks = 0
	for bn := blockFrom; bn < blockTo; bn++ {
		var b EthBlockByNumber
		res = reqGen.Erigon("eth_getBlockByNumber", reqGen.getBlockByNumber(bn, true /* withTxs */), &b)
		if res.Err != nil {
			return fmt.Errorf("retrieve block (Erigon) %d: %v", blockFrom, res.Err)
		}
		if b.Error != nil {
			return fmt.Errorf("retrieving block (Erigon): %d %s", b.Error.Code, b.Error.Message)
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

	var rec *bufio.Writer
	if recordFileName != "" {
		f, err := os.Create(recordFileName)
		if err != nil {
			return fmt.Errorf("Cannot create file %s for recording: %v\n", recordFileName, err)
		}
		defer f.Close()
		rec = bufio.NewWriter(f)
		defer rec.Flush()
	}
	var errs *bufio.Writer
	if errorFileName != "" {
		ferr, err := os.Create(errorFileName)
		if err != nil {
			return fmt.Errorf("Cannot create file %s for error output: %v\n", errorFileName, err)
		}
		defer ferr.Close()
		errs = bufio.NewWriter(ferr)
		defer errs.Flush()
	}

	var resultsCh chan CallResult = nil
	if !needCompare {
		resultsCh = make(chan CallResult, 1000)
		defer close(resultsCh)
		go vegetaWrite(true, []string{"debug_traceTransaction"}, resultsCh)
	}

	reqGen := &RequestGenerator{}

	var res CallResult
	var nBlocks = 0
	var nTransactions = 0
	for bn := blockFrom; bn < blockTo; bn++ {
		if nBlocks%50 == 0 {
			fmt.Println("Processing Block: ", bn)
		}
		nBlocks++

		var erigonBlock EthBlockByNumber
		res = reqGen.Erigon("eth_getBlockByNumber", reqGen.getBlockByNumber(bn, true /* withTxs */), &erigonBlock)
		if res.Err != nil {
			return fmt.Errorf("Could not retrieve block (Erigon) %d: %v\n", bn, res.Err)
		}

		if erigonBlock.Error != nil {
			return fmt.Errorf("Error retrieving block (Erigon): %d %s\n", erigonBlock.Error.Code, erigonBlock.Error.Message)
		}

		if needCompare {
			var otherBlock EthBlockByNumber
			res = reqGen.Geth("eth_getBlockByNumber", reqGen.getBlockByNumber(bn, true /* withTxs */), &otherBlock)
			if res.Err != nil {
				return fmt.Errorf("Could not retrieve block (geth) %d: %v\n", bn, res.Err)
			}
			if otherBlock.Error != nil {
				return fmt.Errorf("Error retrieving block (geth): %d %s\n", otherBlock.Error.Code, otherBlock.Error.Message)
			}
			if !compareBlocks(&erigonBlock, &otherBlock) {
				if rec != nil {
					fmt.Fprintf(rec, "Block difference for block=%d\n", bn)
					rec.Flush()
					continue
				} else {
					return fmt.Errorf("block %d has different fields\n", bn)
				}
			}
		}

		for idx, txn := range erigonBlock.Result.Transactions {
			if idx%30 != 0 {
				continue
			}
			nTransactions++

			var request string
			request = reqGen.debugTraceTransaction(txn.Hash, additionalParams)
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

	var rec *bufio.Writer
	if recordFile != "" {
		f, err := os.Create(recordFile)
		if err != nil {
			return fmt.Errorf("Cannot create file %s for recording: %v\n", recordFile, err)
		}
		defer f.Close()
		rec = bufio.NewWriter(f)
		defer rec.Flush()
	}
	var errs *bufio.Writer
	if errorFile != "" {
		ferr, err := os.Create(errorFile)
		if err != nil {
			return fmt.Errorf("Cannot create file %s for error output: %v\n", errorFile, err)
		}
		defer ferr.Close()
		errs = bufio.NewWriter(ferr)
		defer errs.Flush()
	}

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

		var b EthBlockByNumber
		res = reqGen.Erigon("eth_getBlockByNumber", reqGen.getBlockByNumber(bn, true /* withTxs */), &b)
		if res.Err != nil {
			return fmt.Errorf("Could not retrieve block (Erigon) %d: %v\n", bn, res.Err)
		}

		if b.Error != nil {
			return fmt.Errorf("Error retrieving block (Erigon): %d %s\n", b.Error.Code, b.Error.Message)
		}

		if needCompare {
			var bg EthBlockByNumber
			res = reqGen.Geth("eth_getBlockByNumber", reqGen.getBlockByNumber(bn, true /* withTxs */), &bg)
			if res.Err != nil {
				return fmt.Errorf("Could not retrieve block (geth) %d: %v\n", bn, res.Err)
			}
			if bg.Error != nil {
				return fmt.Errorf("Error retrieving block (geth): %d %s\n", bg.Error.Code, bg.Error.Message)
			}
			if !compareBlocks(&b, &bg) {
				return fmt.Errorf("Block difference for %d\n", bn)
			}
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
