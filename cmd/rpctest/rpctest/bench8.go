package rpctest

import (
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"github.com/ledgerwatch/turbo-geth/log"
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
	rnd := rand.New(rand.NewSource(42)) // nolint:gosec
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
						} else if logsg.Error != nil {
							fmt.Printf("Error getting logs for account (geth) %x: %d %s\n", account, logsg.Error.Code, logsg.Error.Message)
						} else {
							if !compareLogs(&logs, &logsg) {
								fmt.Printf("Different logs for account %x and block %d-%d\n", account, prevBn, bn)
								return
							}
						}
					}
					log.Info("Results", "count", len(logs.Result))
					topics := getTopics(&logs)
					// All combination of account and one topic
					for _, topic := range topics {
						reqGen.reqID++
						var logs1 EthLogs
						res = reqGen.TurboGeth("eth_getLogs", reqGen.getLogs1(prevBn, bn+10000, account, topic), &logs1)
						if res.Err != nil {
							fmt.Printf("Could not get logs for account (turbo-geth) %x %x: %v\n", account, topic, res.Err)
							return
						}
						if logs1.Error != nil {
							fmt.Printf("Error getting logs for account (turbo-geth) %x %x: %d %s\n", account, topic, logs1.Error.Code, logs1.Error.Message)
							return
						}
						if needCompare {
							var logsg1 EthLogs
							res = reqGen.Geth("eth_getLogs", reqGen.getLogs1(prevBn, bn+10000, account, topic), &logsg1)
							if res.Err != nil {
								fmt.Printf("Could not get logs for account (geth) %x %x: %v\n", account, topic, res.Err)
							} else if logsg1.Error != nil {
								fmt.Printf("Error getting logs for account (geth) %x %x: %d %s\n", account, topic, logsg1.Error.Code, logsg1.Error.Message)
							} else {
								if !compareLogs(&logs1, &logsg1) {
									fmt.Printf("Different logs for account %x %x and block %d-%d\n", account, topic, prevBn, bn)
									return
								}
							}
						}
						log.Info("Results", "count", len(logs1.Result))
					}
					// Random combinations of two topics
					if len(topics) >= 2 {
						idx1 := rnd.Int31n(int32(len(topics)))
						idx2 := rnd.Int31n(int32(len(topics) - 1))
						if idx2 >= idx1 {
							idx2++
						}
						reqGen.reqID++
						var logs2 EthLogs
						res = reqGen.TurboGeth("eth_getLogs", reqGen.getLogs2(prevBn, bn+100000, account, topics[idx1], topics[idx2]), &logs2)
						if res.Err != nil {
							fmt.Printf("Could not get logs for account (turbo-geth) %x %x %x: %v\n", account, topics[idx1], topics[idx2], res.Err)
							return
						}
						if logs2.Error != nil {
							fmt.Printf("Error getting logs for account (turbo-geth) %x %x %x: %d %s\n", account, topics[idx1], topics[idx2], logs2.Error.Code, logs2.Error.Message)
							return
						}
						if needCompare {
							var logsg2 EthLogs
							res = reqGen.Geth("eth_getLogs", reqGen.getLogs2(prevBn, bn+100000, account, topics[idx1], topics[idx2]), &logsg2)
							if res.Err != nil {
								fmt.Printf("Could not get logs for account (geth) %x %x %x: %v\n", account, topics[idx1], topics[idx2], res.Err)
							} else if logsg2.Error != nil {
								fmt.Printf("Error getting logs for account (geth) %x %x %x: %d %s\n", account, topics[idx1], topics[idx2], logsg2.Error.Code, logsg2.Error.Message)
							} else {
								if !compareLogs(&logs2, &logsg2) {
									fmt.Printf("Different logs for account %x %x %x and block %d-%d\n", account, topics[idx1], topics[idx2], prevBn, bn)
									return
								}
							}
						}
						log.Info("Results", "count", len(logs2.Result))
					}
				}
			}
			fmt.Printf("Done blocks %d-%d, modified accounts: %d\n", prevBn, bn, len(mag.Result))
			prevBn = bn
		}
	}
}
