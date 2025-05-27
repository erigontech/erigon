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

func BenchTraceReplayTransaction(erigonUrl, gethUrl string, needCompare bool, blockFrom uint64, blockTo uint64, recordFile string, errorFile string) error {
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

	var res CallResult
	reqGen := &RequestGenerator{}

	for bn := blockFrom; bn < blockTo; bn++ {
		var b EthBlockByNumber
		res = reqGen.Erigon("eth_getBlockByNumber", reqGen.getBlockByNumber(bn, true /* withTxs */), &b)
		if res.Err != nil {
			return fmt.Errorf("retrieve block (Erigon) %d: %v", blockFrom, res.Err)
		}
		if b.Error != nil {
			return fmt.Errorf("retrieving block (Erigon): %d %s", b.Error.Code, b.Error.Message)
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
