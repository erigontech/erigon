package rpctest

import (
	"fmt"
	"net/http"
	"time"
)

// BenchEthGetBalance compares response of Erigon with Geth
// but also can be used for comparing RPCDaemon with Geth
// parameters:
// needCompare - if false - doesn't call Erigon and doesn't compare responses
//
//	use false value - to generate vegeta files, it's faster but we can generate vegeta files for Geth and Erigon
func BenchEthGetBalance(erigonURL, gethURL string, needCompare bool, blockFrom uint64, blockTo uint64) {
	setRoutes(erigonURL, gethURL)
	var client = &http.Client{
		Timeout: time.Second * 600,
	}

	reqGen := &RequestGenerator{
		client: client,
	}

	var res CallResult
	var resultsCh chan CallResult = nil
	if !needCompare {
		resultsCh = make(chan CallResult, 1000)
		defer close(resultsCh)
		go vegetaWrite(true, []string{"eth_getBalance"}, resultsCh)
	}

	reqGen.reqID++
	var blockNumber EthBlockNumber
	res = reqGen.Erigon("eth_blockNumber", reqGen.blockNumber(), &blockNumber)
	if res.Err != nil {
		fmt.Printf("Could not get block number: %v\n", res.Err)
		return
	}
	if blockNumber.Error != nil {
		fmt.Printf("Error getting block number: %d %s\n", blockNumber.Error.Code, blockNumber.Error.Message)
		return
	}
	for bn := blockFrom; bn <= blockTo; bn++ {
		reqGen.reqID++
		var b EthBlockByNumber
		res = reqGen.Erigon("eth_getBlockByNumber", reqGen.getBlockByNumber(bn, true /* withTxs */), &b)
		if res.Err != nil {
			fmt.Printf("Could not retrieve block (Erigon) %d: %v\n", bn, res.Err)
			return
		}

		if b.Error != nil {
			fmt.Printf("Error retrieving block (Erigon): %d %s\n", b.Error.Code, b.Error.Message)
		}

		if needCompare {
			var bg EthBlockByNumber
			res = reqGen.Geth("eth_getBlockByNumber", reqGen.getBlockByNumber(bn, true /* withTxs */), &bg)
			if res.Err != nil {
				fmt.Printf("Could not retrieve block (geth) %d: %v\n", bn, res.Err)
				return
			}
			if bg.Error != nil {
				fmt.Printf("Error retrieving block (geth): %d %s\n", bg.Error.Code, bg.Error.Message)
				return
			}
			if !compareBlocks(&b, &bg) {
				fmt.Printf("Block difference for %d\n", bn)
				return
			}
		}

		for txn := range b.Result.Transactions {

			tx := b.Result.Transactions[txn]
			var balance EthBalance
			account := tx.From
			reqGen.reqID++
			res = reqGen.Erigon("eth_getBalance", reqGen.getBalance(account, bn), &balance)
			if !needCompare {
				resultsCh <- res
			}
			if res.Err != nil {
				fmt.Printf("Could not get account balance (Erigon): %v\n", res.Err)
				return
			}
			if balance.Error != nil {
				fmt.Printf("Error getting account balance (Erigon): %d %s", balance.Error.Code, balance.Error.Message)
				return
			}
			if needCompare {
				var balanceg EthBalance
				res = reqGen.Geth("eth_getBalance", reqGen.getBalance(account, bn), &balanceg)
				if res.Err != nil {
					fmt.Printf("Could not get account balance (geth): %v\n", res.Err)
					return
				}
				if balanceg.Error != nil {
					fmt.Printf("Error getting account balance (geth): %d %s\n", balanceg.Error.Code, balanceg.Error.Message)
					return
				}
				if !compareBalances(&balance, &balanceg) {
					fmt.Printf("Account %x balance difference for block %d\n", account, bn)
					return
				}
			}
		}
	}
}
