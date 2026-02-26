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
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/erigontech/erigon/common"
)

// BenchEthGetBalance compares response of Erigon with Geth
// but also can be used for comparing RPCDaemon with Geth
// parameters:
// needCompare - if false - doesn't call Erigon and doesn't compare responses
//
//	use false value - to generate vegeta files, it's faster but we can generate vegeta files for Geth and Erigon
func BenchEthGetBalance(erigonURL, gethURL string, needCompare bool, blockFrom uint64, blockTo uint64) error {
	setRoutes(erigonURL, gethURL)

	reqGen := &RequestGenerator{}

	var res CallResult
	var resultsCh chan CallResult = nil
	if !needCompare {
		resultsCh = make(chan CallResult, 1000)
		defer close(resultsCh)
		go vegetaWrite(true, []string{"eth_getBalance"}, resultsCh)
	}

	var blockNumber EthBlockNumber
	res = reqGen.Erigon("eth_blockNumber", reqGen.blockNumber(), &blockNumber)
	if res.Err != nil {
		return fmt.Errorf("Could not get block number: %v\n", res.Err)
	}
	if blockNumber.Error != nil {
		return fmt.Errorf("Error getting block number: %d %s\n", blockNumber.Error.Code, blockNumber.Error.Message)
	}
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

		for txn := range b.Result.Transactions {

			tx := b.Result.Transactions[txn]
			var balance EthBalance
			account := tx.From

			res = reqGen.Erigon("eth_getBalance", reqGen.getBalance(account, bn), &balance)
			if !needCompare {
				resultsCh <- res
			}
			if res.Err != nil {
				return fmt.Errorf("Could not get account balance (Erigon): %v\n", res.Err)
			}
			if balance.Error != nil {
				return fmt.Errorf("Error getting account balance (Erigon): %d %s", balance.Error.Code, balance.Error.Message)
			}
			if needCompare {
				var balanceg EthBalance
				res = reqGen.Geth("eth_getBalance", reqGen.getBalance(account, bn), &balanceg)
				if res.Err != nil {
					return fmt.Errorf("Could not get account balance (geth): %v\n", res.Err)
				}
				if balanceg.Error != nil {
					return fmt.Errorf("Error getting account balance (geth): %d %s\n", balanceg.Error.Code, balanceg.Error.Message)
				}
				if !compareBalances(&balance, &balanceg) {
					return fmt.Errorf("Account %x balance difference for block %d\n", account, bn)
				}
			}
		}
	}
	return nil
}

func BenchEthGetBalanceRandomAccount(erigonURL string, concurentRequests int) error {
	setRoutes(erigonURL, "")

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

	latencyLen := 1000
	latency := make([]int, 0, latencyLen)
	var m sync.Mutex
	var finishedRequests uint64

	go func() {
		lastPrint := time.Now()
		p50 := float64(0)
		p90 := float64(0)
		p99 := float64(0)
		rps := float64(0)

		for {

			time.Sleep(time.Second)
			m.Lock()

			if len(latency) > 0 {
				sort.Ints(latency)

				p50 = float64(latency[len(latency)/2]) / 1000 // convert to ms
				p90 = float64(latency[len(latency)/10*9]) / 1000
				p99 = float64(latency[len(latency)/100*99]) / 1000
				rps = float64(finishedRequests) / float64(time.Since(lastPrint).Seconds())

				lastPrint = time.Now()
				finishedRequests = 0
			}

			m.Unlock()

			fmt.Printf("Latency 50p: %.2fms 90p: %.2fms 99p: %.2fms RPS: %.2f req/s\n",
				p50,
				p90,
				p99,
				rps,
			)

		}

	}()

	reqQueue := make(chan struct{}, concurentRequests)

	for {
		bn := uint64(rand.Intn(
			int(blockNumber.Number.Uint64()),
		))

		var b EthBlockByNumber
		res = reqGen.Erigon("eth_getBlockByNumber", reqGen.getBlockByNumber(bn, true /* withTxs */), &b)
		if res.Err != nil {
			return fmt.Errorf("Could not retrieve block (Erigon) %d: %v\n", bn, res.Err)
		}

		if b.Error != nil {
			return fmt.Errorf("Error retrieving block (Erigon): %d %s\n", b.Error.Code, b.Error.Message)
		}

		for txn := range b.Result.Transactions {
			account := b.Result.Transactions[txn].From

			reqQueue <- struct{}{}

			go func(account common.Address, bn uint64, launchedAt time.Time) {
				var balance EthBalance

				res = reqGen.Erigon("eth_getBalance", reqGen.getBalance(account, bn), &balance)
				if res.Err != nil {
					panic(fmt.Errorf("Could not get account balance (Erigon): %v\n", res.Err))
				}
				if balance.Error != nil {
					panic(fmt.Errorf("Error getting account balance (Erigon): %d %s", balance.Error.Code, balance.Error.Message))
				}

				reqLatency := int(time.Since(launchedAt).Microseconds())
				<-reqQueue

				m.Lock()
				finishedRequests += 1

				if len(latency) == latencyLen {
					latency = latency[1:]
				}

				latency = append(latency, reqLatency)
				m.Unlock()
			}(account, bn, time.Now())
		}
	}
}
