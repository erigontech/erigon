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
)

// BenchOverlayGetLogs compares response of Erigon with Geth
// but also can be used for comparing RPCDaemon with Geth or infura
// parameters:
// needCompare - if false - doesn't call Erigon and doesn't compare responses
//
//	false value - to generate vegeta files, it's faster but we can generate vegeta files for Geth and Erigon
//	recordFile stores all eth_getlogs returned with success
//	errorFile stores information when erigon and geth doesn't return same data
func BenchOverlayGetLogs(erigonURL string, needCompare bool, blockFrom uint64, blockTo uint64, recordFile string, errorFile string) error {
	setRoutes(erigonURL, erigonURL)

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
		go vegetaWrite(true, []string{"debug_getModifiedAccountsByNumber", "eth_getLogs"}, resultsCh)
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

	prevBn := blockFrom
	rnd := rand.New(rand.NewSource(42)) // nolint:gosec
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
			for account := range accountSet {

				requestEth := reqGen.getLogs(prevBn, bn, account)
				requestOverlay := reqGen.getOverlayLogs(prevBn, bn, account)
				errCtx := fmt.Sprintf("account %x blocks %d-%d", account, prevBn, bn)
				if err := requestAndCompareErigon(requestEth, requestOverlay, "eth_getLogs", "overlay_getLogs", errCtx, reqGen, needCompare, rec, errs, resultsCh,
					/* insertOnlyIfSuccess */ false); err != nil {
					fmt.Println(err)
					return err
				}
				topics := getTopics(res.Result)
				// All combination of account and one topic
				for _, topic := range topics {

					requestEth = reqGen.getLogs1(prevBn, bn+10000, account, topic)
					requestOverlay := reqGen.getOverlayLogs1(prevBn, bn+10000, account, topic)
					errCtx := fmt.Sprintf("account %x topic %x blocks %d-%d", account, topic, prevBn, bn)
					if err := requestAndCompareErigon(requestEth, requestOverlay, "eth_getLogs", "overlay_getLogs", errCtx, reqGen, needCompare, rec, errs, resultsCh,
						/* insertOnlyIfSuccess */ false); err != nil {
						fmt.Println(err)
						return err
					}
				}
				// Random combinations of two topics
				if len(topics) >= 2 {
					idx1 := rnd.Int31n(int32(len(topics)))
					idx2 := rnd.Int31n(int32(len(topics) - 1))
					if idx2 >= idx1 {
						idx2++
					}

					requestEth = reqGen.getLogs2(prevBn, bn+100000, account, topics[idx1], topics[idx2])
					requestOverlay := reqGen.getOverlayLogs2(prevBn, bn+100000, account, topics[idx1], topics[idx2])
					errCtx := fmt.Sprintf("account %x topic1 %x topic2 %x blocks %d-%d", account, topics[idx1], topics[idx2], prevBn, bn)
					if err := requestAndCompareErigon(requestEth, requestOverlay, "eth_getLogs", "overlay_getLogs", errCtx, reqGen, needCompare, rec, errs, resultsCh,
						/* insertOnlyIfSuccess */ false); err != nil {
						fmt.Println(err)
						return err
					}
				}
			}
		}
		fmt.Printf("Done blocks %d-%d, modified accounts: %d\n", prevBn, bn, len(mag.Result))
		prevBn = bn
	}
	return nil
}
