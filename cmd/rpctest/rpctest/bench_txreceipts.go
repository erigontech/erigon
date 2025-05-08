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
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
)

// benchTxReceipt compares response of Erigon with Geth
// but also can be used for comparing RPCDaemon with Geth
// parameters:
// needCompare - if false - doesn't call Erigon and doesn't compare responses
func BenchTxReceipt(erigonURL, gethURL string, needCompare bool, blockFrom uint64, blockTo uint64, recordFileName string, errorFileName string) error {
	setRoutes(erigonURL, gethURL)

	var rec *bufio.Writer
	var resultsCh chan CallResult = nil

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

	if !needCompare {
		resultsCh = make(chan CallResult, 1000)
		defer close(resultsCh)
		go vegetaWrite(true, []string{"eth_getTransactionReceipt"}, resultsCh)
	}

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

		var b EthBlockByNumber
		res = reqGen.Erigon("eth_getBlockByNumber", reqGen.getBlockByNumber(bn, true /* withTxs */), &b)
		if res.Err != nil {
			return fmt.Errorf("Could not retrieve block (Erigon) %d: %v\n", bn, res.Err)
		}
		if b.Error != nil {
			return fmt.Errorf("Error retrieving block (Erigon): %d %s\n", b.Error.Code, b.Error.Message)
		}

		for _, txn := range b.Result.Transactions {

			request := reqGen.getTransactionReceipt(txn.Hash)
			errCtx := fmt.Sprintf("block %d, txn %s", bn, txn.Hash)
			if err := requestAndCompare(request, "eth_getTransactionReceipt", errCtx, reqGen, needCompare, rec, errs, resultsCh,
				/* insertOnlyIfSuccess */ false); err != nil {
				return err
			}
		}
	}
	return nil
}

const logInterval = 20 * time.Second

func BenchBlockReceipts(erigonURL, gethURL string, needCompare bool, blockFrom uint64, blockTo uint64, recordFileName string, errorFileName string) error {
	setRoutes(erigonURL, gethURL)

	var rec *bufio.Writer
	var resultsCh chan CallResult = nil

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

	if !needCompare {
		resultsCh = make(chan CallResult, 1000)
		defer close(resultsCh)
		go vegetaWrite(true, []string{"eth_getBlockReceipts"}, resultsCh)
	}

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
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	log.Info("starting", "last_block", blockNumber.Number, "from", blockNumber, "to", blockTo)
	for bn := blockFrom; bn <= blockTo; bn++ {
		request := reqGen.getBlockReceipts(bn)
		errCtx := fmt.Sprintf("block %d", bn)
		if err := requestAndCompare(request, "eth_getBlockReceipts", errCtx, reqGen, needCompare, rec, errs, resultsCh,
			/* insertOnlyIfSuccess */ false); err != nil {
			return err
		}

		select {
		case <-logEvery.C:
			log.Info("progess", "b", bn)
		default:
		}
	}
	return nil
}
