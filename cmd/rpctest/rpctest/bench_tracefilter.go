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
	"math/rand"
	"os"

	"github.com/erigontech/erigon-lib/common"
)

// Compares response of Erigon with OpenEthereum
// but also can be used for comparing RPCDaemon with Geth
// parameters:
// needCompare - if false - doesn't call Erigon and doesn't compare responses
func BenchTraceFilter(erigonURL, oeURL string, needCompare bool, blockFrom uint64, blockTo uint64, recordFile string, errorFile string) error {
	setRoutes(erigonURL, oeURL)

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

	var blockNumber EthBlockNumber
	res = reqGen.Erigon("eth_blockNumber", reqGen.blockNumber(), &blockNumber)
	if res.Err != nil {
		return fmt.Errorf("Could not get block number: %v\n", res.Err)
	}
	if blockNumber.Error != nil {
		return fmt.Errorf("Error getting block number: %d %s\n", blockNumber.Error.Code, blockNumber.Error.Message)
	}
	fmt.Printf("Last block: %d\n", blockNumber.Number)
	rnd := rand.New(rand.NewSource(42)) // nolint:gosec
	prevBn := blockFrom
	for bn := blockFrom + 100; bn < blockTo; bn += 100 {
		// Checking modified accounts

		var mag DebugModifiedAccounts
		res = reqGen.Erigon("debug_getModifiedAccountsByNumber", reqGen.getModifiedAccountsByNumber(prevBn, bn), &mag)
		if res.Err != nil {
			return fmt.Errorf("Could not get modified accounts (Erigon): %v\n", res.Err)
		}
		if mag.Error != nil {
			return fmt.Errorf("Error getting modified accounts (Erigon): %d %s\n", mag.Error.Code, mag.Error.Message)
		}
		if res.Err == nil && mag.Error == nil {
			accountSet := extractAccountMap(&mag)
			accounts := make([]common.Address, 0, len(accountSet))
			for account := range accountSet {
				accounts = append(accounts, account)
			}
			// Randomly select 100 accounts
			selects := 100
			if len(accounts) < 100 {
				selects = len(accounts)
			}
			for i := 0; i < selects; i++ {
				idx := i
				if len(accounts) > 100 {
					idx = int(rnd.Int31n(int32(len(accounts))))
				}
				account := accounts[idx]

				request := reqGen.traceFilterFrom(prevBn, bn, account)
				errCtx := fmt.Sprintf("traceFilterFrom fromBlock %d, toBlock %d, fromAddress %x", prevBn, bn, account)
				if err := requestAndCompare(request, "trace_filter", errCtx, reqGen, needCompare, rec, errs, nil /* insertOnlyIfSuccess */, false); err != nil {
					fmt.Println(err)
					return err
				}

				request = reqGen.traceFilterTo(prevBn, bn, account)
				errCtx = fmt.Sprintf("traceFilterTo fromBlock %d, toBlock %d, fromAddress %x", prevBn, bn, account)
				if err := requestAndCompare(request, "trace_filter", errCtx, reqGen, needCompare, rec, errs, nil /* insertOnlyIfSuccess */, false); err != nil {
					fmt.Println(err)
					return err
				}
			}
			/*
				if len(accounts) > 1 {
					from := accounts[0]
					to := accounts[1]

					request := reqGen.traceFilterUnion(prevBn, bn, from, to)
					errCtx := fmt.Sprintf("traceFilterUnion fromBlock %d, toBlock %d, fromAddress %x, toAddress %x", prevBn, bn, from, to)
					if err := requestAndCompare(request, "trace_filter", errCtx, reqGen, needCompare, rec, errs, nil, false); err != nil {
						fmt.Println(err)
						return err
					}

					request = reqGen.traceFilterAfter(prevBn, bn, 1)
					errCtx = fmt.Sprintf("traceFilterAfter fromBlock %d, toBlock %d, after %x", prevBn, bn, 1)
					if err := requestAndCompare(request, "trace_filter", errCtx, reqGen, needCompare, rec, errs, nil, false); err != nil {
						fmt.Println(err)
						return err
					}

					request = reqGen.traceFilterCount(prevBn, bn, 1)
					errCtx = fmt.Sprintf("traceFilterCount fromBlock %d, toBlock %d, count %x", prevBn, bn, 1)
					if err := requestAndCompare(request, "trace_filter", errCtx, reqGen, needCompare, rec, errs, nil, false); err != nil {
						fmt.Println(err)
						return err
					}

					request = reqGen.traceFilterCountAfter(prevBn, bn, 1, 1)
					errCtx = fmt.Sprintf("traceFilterCountAfter fromBlock %d, toBlock %d, count %x, after %x", prevBn, bn, 1, 1)
					if err := requestAndCompare(request, "trace_filter", errCtx, reqGen, needCompare, rec, errs, nil, false); err != nil {
						fmt.Println(err)
						return err
					}
				}
			*/
		}
		fmt.Printf("Done blocks %d-%d, modified accounts: %d\n", prevBn, bn, len(mag.Result))
		prevBn = bn
	}
	return nil
}
