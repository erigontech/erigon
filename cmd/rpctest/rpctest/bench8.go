package rpctest

import (
	"fmt"
	"net/http"
	"time"
)

func Bench8(tgURL, gethURL string, needCompare bool, blockNum uint64) {
	setRoutes(tgURL, gethURL)
	var client = &http.Client{
		Timeout: time.Second * 600,
	}

	var res CallResult
	reqGen := &RequestGenerator{
		client: client,
	}

	reqGen.reqID++
	var blockNumber EthBlockNumber
	res = reqGen.TurboGeth("eth_blockNumber", reqGen.blockNumber(), &blockNumber)
	if res.Err != nil {
		fmt.Printf("Could not get block number: %v\n", res.Err)
		return
	}
	if blockNumber.Error != nil {
		fmt.Printf("Error getting block number: %d %s\n", blockNumber.Error.Code, blockNumber.Error.Message)
		return
	}
	lastBlock := blockNumber.Number
	fmt.Printf("Last block: %d\n", lastBlock)

	firstBn := int(blockNum)
	prevBn := firstBn
	for bn := firstBn; bn <= int(lastBlock); bn++ {

		if prevBn < bn && bn%100 == 0 {
			// Checking modified accounts
			reqGen.reqID++
			var mag DebugModifiedAccounts
			res = reqGen.TurboGeth("debug_getModifiedAccountsByNumber", reqGen.getModifiedAccountsByNumber(prevBn, bn), &mag)
			if res.Err != nil {
				fmt.Printf("Could not get modified accounts (turbo-geth): %v\n", res.Err)
				return
			}
			if mag.Error != nil {
				fmt.Printf("Error getting modified accounts (turbo-geth): %d %s\n", mag.Error.Code, mag.Error.Message)
				return
			}
			if res.Err == nil && mag.Error == nil {
				accountSet := extractAccountMap(&mag)
				for account := range accountSet {
					reqGen.reqID++
					var logs EthLogs
					res = reqGen.TurboGeth("eth_getLogs", reqGen.getLogs(prevBn, bn, account), &logs)
					if res.Err != nil {
						fmt.Printf("Could not get logs for account (turbo-geth) %x: %v\n", account, res.Err)
						return
					}
					if logs.Error != nil {
						fmt.Printf("Error getting logs for account (turbo-geth) %x: %d %s\n", account, logs.Error.Code, logs.Error.Message)
						return
					}
					if needCompare {
						var logsg EthLogs
						res = reqGen.Geth("eth_getLogs", reqGen.getLogs(prevBn, bn, account), &logsg)
						if res.Err != nil {
							fmt.Printf("Could not get logs for account (geth) %x: %v\n", account, res.Err)
							return
						}
						if logsg.Error != nil {
							fmt.Printf("Error getting logs for account (geth) %x: %d %s\n", account, logsg.Error.Code, logsg.Error.Message)
							return
						}
						if !compareLogs(&logs, &logsg) {
							fmt.Printf("Different logs for account %x and block %d-%d\n", account, prevBn, bn)
							return
						}
					}
				}
			}
			fmt.Printf("Done blocks %d-%d, modified accounts: %d\n", prevBn, bn, len(mag.Result))
			prevBn = bn
		}
	}
}
