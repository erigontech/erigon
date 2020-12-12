package rpctest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/log"
)

func compareBlocks(b, bg *EthBlockByNumber) bool {
	r := b.Result
	rg := bg.Result
	if r.Difficulty.ToInt().Cmp(rg.Difficulty.ToInt()) != 0 {
		fmt.Printf("Difficulty difference %d %d\n", r.Difficulty.ToInt(), rg.Difficulty.ToInt())
		return false
	}
	if r.Miner != rg.Miner {
		fmt.Printf("Miner different %x %x\n", r.Miner, rg.Miner)
		return false
	}
	if len(r.Transactions) != len(rg.Transactions) {
		fmt.Printf("Num of txs different: %d %d\n", len(r.Transactions), len(rg.Transactions))
		return false
	}
	for i, tx := range r.Transactions {
		txg := rg.Transactions[i]
		if tx.From != txg.From {
			fmt.Printf("Tx %d different From: %x %x\n", i, tx.From, txg.From)
			return false
		}
		if (tx.To == nil && txg.To != nil) || (tx.To != nil && txg.To == nil) {
			fmt.Printf("Tx %d different To nilness: %t %t\n", i, (tx.To == nil), (txg.To == nil))
			return false
		}
		if tx.To != nil && txg.To != nil && *tx.To != *txg.To {
			fmt.Printf("Tx %d different To: %x %x\n", i, *tx.To, *txg.To)
			return false
		}
		if tx.Hash != txg.Hash {
			fmt.Printf("Tx %x different Hash: %s %s\n", i, tx.Hash, txg.Hash)
			return false
		}
	}
	return true
}

func compareTraces(trace, traceg *EthTxTrace) bool {
	r := trace.Result
	rg := traceg.Result
	if r.Gas != rg.Gas {
		fmt.Printf("Trace different Gas: %d / %d\n", r.Gas, rg.Gas)
		return false
	}
	if r.Failed != rg.Failed {
		fmt.Printf("Trace different Failed: %t / %t\n", r.Failed, rg.Failed)
		return false
	}
	if r.ReturnValue != rg.ReturnValue {
		fmt.Printf("Trace different ReturnValue: %s / %s\n", r.ReturnValue, rg.ReturnValue)
		return false
	}
	if len(r.StructLogs) != len(rg.StructLogs) {
		fmt.Printf("Trace different length: %d / %d\n", len(r.StructLogs), len(rg.StructLogs))
		return false
	}
	for i, l := range r.StructLogs {
		lg := rg.StructLogs[i]
		if l.Op != lg.Op {
			fmt.Printf("Trace different Op: %d %s %s\n", i, l.Op, lg.Op)
			return false
		}
		if l.Pc != lg.Pc {
			fmt.Printf("Trace different Pc: %d %d %d\n", i, l.Pc, lg.Pc)
			return false
		}
	}
	return true
}

func compareTraceCalls(trace, traceg *TraceCall) bool {
	r := trace.Result
	rg := traceg.Result
	if !bytes.Equal(r.Output, rg.Output) {
		fmt.Print("Root output is different: %x %x\n", r.Output, rg.Output)
		return false
	}
	if len(r.Trace) != len(rg.Trace) {
		fmt.Printf("Traces have different lengths: %d / %d\n", len(r.Trace), len(rg.Trace))
		return false
	}
	for i, t := range r.Trace {
		tg := rg.Trace[i]
		if t.Type != tg.Type {
			fmt.Printf("Trace different Type: %d %s %s\n", i, t.Type, tg.Type)
			return false
		}
	}
	return true
}

func compareBalances(balance, balanceg *EthBalance) bool {
	if balance.Balance.ToInt().Cmp(balanceg.Balance.ToInt()) != 0 {
		fmt.Printf("Different balance: %d %d\n", balance.Balance.ToInt(), balanceg.Balance.ToInt())
		return false
	}
	return true
}

func extractAccountMap(ma *DebugModifiedAccounts) map[common.Address]struct{} {
	r := ma.Result
	rset := make(map[common.Address]struct{})
	for _, a := range r {
		rset[a] = struct{}{}
	}
	return rset
}

func printStorageRange(sm map[common.Hash]storageEntry) {
	for k := range sm {
		fmt.Printf("%x\n", k)
	}
}

func compareStorageRanges(sm, smg map[common.Hash]storageEntry) bool {
	for k, v := range sm {
		vg, ok := smg[k]
		if !ok {
			fmt.Printf("%x not present in smg\n", k)
			return false
		}
		if v.Key == nil {
			fmt.Printf("v.Key == nil for %x\n", k)
			return false
		}
		if k != crypto.Keccak256Hash(v.Key[:]) {
			fmt.Printf("Sec key %x does not match key %x\n", k, *v.Key)
			return false
		}
		if v.Value != vg.Value {
			fmt.Printf("Different values for %x: %x %x [%x]\n", k, v.Value, vg.Value, *v.Key)
			return false
		}
	}
	for k, v := range smg {
		if _, ok := sm[k]; !ok {
			fmt.Printf("%x not present in sm\n", k)
			return false
		}
		if k != crypto.Keccak256Hash(v.Key[:]) {
			fmt.Printf("Sec key (g) %x does not match key %x\n", k, *v.Key)
			return false
		}
	}
	return true
}

/*
	// Derived fields. These fields are filled in by the node
	// but not secured by consensus.
	// block in which the transaction was included
	BlockNumber hexutil.Uint64 `json:"blockNumber"`
	// hash of the transaction
	TxHash common.Hash    `json:"transactionHash" gencodec:"required"`
	// index of the transaction in the block
	TxIndex hexutil.Uint  `json:"transactionIndex" gencodec:"required"`
	// hash of the block in which the transaction was included
	BlockHash common.Hash `json:"blockHash"`
	// index of the log in the receipt
	Index hexutil.Uint    `json:"logIndex" gencodec:"required"`

	// The Removed field is true if this log was reverted due to a chain reorganisation.
	// You must pay attention to this field if you receive logs through a filter query.
	Removed bool `json:"removed"`
*/

func compareReceipts(receipt, receiptg *EthReceipt) bool {
	r := receipt.Result
	rg := receiptg.Result
	if r.TxHash != rg.TxHash {
		fmt.Printf("Different tx hashes: %x %x\n", r.TxHash, rg.TxHash)
		return false
	}
	if r.Status != rg.Status {
		fmt.Printf("Different status: %d %d\n", r.Status, rg.Status)
		return false
	}
	if r.CumulativeGasUsed != rg.CumulativeGasUsed {
		fmt.Printf("Different cumulativeGasUsed: %d %d\n", r.CumulativeGasUsed, rg.CumulativeGasUsed)
		return false
	}
	if !bytes.Equal(r.Bloom, rg.Bloom) {
		fmt.Printf("Different blooms: %x %x\n", r.Bloom, rg.Bloom)
		return false
	}
	if r.ContractAddress == nil && rg.ContractAddress != nil {
		fmt.Printf("Different contract addresses: nil %x\n", rg.ContractAddress)
		return false
	}
	if r.ContractAddress != nil && rg.ContractAddress == nil {
		fmt.Printf("Different contract addresses: %x nil\n", r.ContractAddress)
		return false
	}
	if r.ContractAddress != nil && rg.ContractAddress != nil && *r.ContractAddress != *rg.ContractAddress {
		fmt.Printf("Different contract addresses: %x %x\n", r.ContractAddress, rg.ContractAddress)
		return false
	}
	if r.GasUsed != rg.GasUsed {
		fmt.Printf("Different gasUsed: %d %d\n", r.GasUsed, rg.GasUsed)
		return false
	}
	if len(r.Logs) != len(rg.Logs) {
		fmt.Printf("Different log lenths: %d %d\n", len(r.Logs), len(rg.Logs))
		return false
	}
	for i, l := range r.Logs {
		lg := rg.Logs[i]
		if l.Address != lg.Address {
			fmt.Printf("Different log %d addresses: %x %x\n", i, l.Address, lg.Address)
			return false
		}
		if len(l.Topics) != len(lg.Topics) {
			fmt.Printf("Different log %d topic lengths: %d %d\n", i, len(l.Topics), len(lg.Topics))
			return false
		}
		for j, t := range l.Topics {
			tg := lg.Topics[j]
			if t != tg {
				fmt.Printf("Different log %d topics %d: %x %x\n", i, j, t, tg)
				return false
			}
		}
		if !bytes.Equal(l.Data, lg.Data) {
			fmt.Printf("Different log %d data: %x %x\n", i, l.Data, lg.Data)
			return false
		}
	}
	return true
}

func compareLogs(logs, logsg *EthLogs) bool {
	r := logs.Result
	rg := logsg.Result
	if len(r) != len(rg) {
		fmt.Printf("Different log lenths: %d %d\n", len(r), len(rg))
		return false
	}
	for i, l := range r {
		lg := rg[i]
		if l.Address != lg.Address {
			fmt.Printf("Different log %d addresses: %x %x\n", i, l.Address, lg.Address)
			return false
		}
		if len(l.Topics) != len(lg.Topics) {
			fmt.Printf("Different log %d topic lengths: %d %d\n", i, len(l.Topics), len(lg.Topics))
			return false
		}
		for j, t := range l.Topics {
			tg := lg.Topics[j]
			if t != tg {
				fmt.Printf("Different log %d topics %d: %x %x\n", i, j, t, tg)
				return false
			}
		}
		if !bytes.Equal(l.Data, lg.Data) {
			fmt.Printf("Different log %d data: %x %x\n", i, l.Data, lg.Data)
			return false
		}
	}
	return true
}

func getTopics(logs *EthLogs) []common.Hash {
	topicSet := make(map[common.Hash]struct{})
	r := logs.Result
	for _, l := range r {
		for _, t := range l.Topics {
			topicSet[t] = struct{}{}
		}
	}
	topics := make([]common.Hash, len(topicSet))
	i := 0
	for t := range topicSet {
		topics[i] = t
		i++
	}
	return topics
}

func compareAccountRanges(tg, geth map[common.Address]state.DumpAccount) bool {
	allAddresses := make(map[common.Address]struct{})
	for k := range tg {
		allAddresses[k] = struct{}{}
	}

	for k := range geth {
		allAddresses[k] = struct{}{}
	}

	for addr := range allAddresses {
		tgAcc, tgOk := tg[addr]
		if !tgOk {
			fmt.Printf("missing account in TurboGeth %x\n", addr)
			return false
		}

		gethAcc, gethOk := geth[addr]
		if !gethOk {
			fmt.Printf("missing account in Geth %x\n", addr)
			return false
		}
		different := false
		if tgAcc.Balance != gethAcc.Balance {
			fmt.Printf("Different balance for %x: turbo %s, geth %s\n", addr, tgAcc.Balance, gethAcc.Balance)
			different = true
		}
		if tgAcc.Nonce != gethAcc.Nonce {
			fmt.Printf("Different nonce for %x: turbo %d, geth %d\n", addr, tgAcc.Nonce, gethAcc.Nonce)
			different = true
		}
		// We do not compare Root, because Turbo-geth does not compute it
		if tgAcc.CodeHash != gethAcc.CodeHash {
			fmt.Printf("Different codehash for %x: turbo %s, geth %s\n", addr, tgAcc.CodeHash, gethAcc.CodeHash)
			different = true
		}
		if tgAcc.Code != gethAcc.Code {
			fmt.Printf("Different codehash for %x: turbo %s, geth %s\n", addr, tgAcc.Code, gethAcc.Code)
			different = true
		}
		if different {
			return false
		}
	}
	return true
}

func compareProofs(proof, gethProof *EthGetProof) bool {
	r := proof.Result
	rg := gethProof.Result

	/*
	   	Address      common.Address  `json:"address"`
	   	AccountProof []string        `json:"accountProof"`
	   	Balance      *hexutil.Big    `json:"balance"`
	   	CodeHash     common.Hash     `json:"codeHash"`
	   	Nonce        hexutil.Uint64  `json:"nonce"`
	   	StorageHash  common.Hash     `json:"storageHash"`
	   	StorageProof []StorageResult `json:"storageProof"`
	   }
	   type StorageResult struct {
	   	Key   string       `json:"key"`
	   	Value *hexutil.Big `json:"value"`
	   	Proof []string     `json:"proof"`
	*/
	equal := true
	if r.Address != rg.Address {
		fmt.Printf("Different addresses %x / %x\n", r.Address, rg.Address)
		equal = false
	}
	if len(r.AccountProof) == len(rg.AccountProof) {
		for i, ap := range r.AccountProof {
			if ap != rg.AccountProof[i] {
				fmt.Printf("Different item %d in account proof: %s %s\n", i, ap, rg.AccountProof[i])
				equal = false
			}
		}
	} else {
		fmt.Printf("Different length of AccountProof: %d / %d\n", len(r.AccountProof), len(rg.AccountProof))
		equal = false
	}
	if r.Balance.ToInt().Cmp(rg.Balance.ToInt()) != 0 {
		fmt.Printf("Different balance: %s / %s\n", r.Balance.ToInt(), rg.Balance.ToInt())
		equal = false
	}
	if r.CodeHash != rg.CodeHash {
		fmt.Printf("Different CodeHash: %x / %x\n", r.CodeHash, rg.CodeHash)
		equal = false
	}
	if r.Nonce != rg.Nonce {
		fmt.Printf("Different nonce: %d / %d\n", r.Nonce, rg.Nonce)
		equal = false
	}
	if r.StorageHash != rg.StorageHash {
		fmt.Printf("Different StorageHash: %x / %x\n", r.StorageHash, rg.StorageHash)
		equal = false
	}
	if len(r.StorageProof) == len(rg.StorageProof) {
		for i, sp := range r.StorageProof {
			spg := rg.StorageProof[i]
			if sp.Key != spg.Key {
				fmt.Printf("Different storage proof key in item %d: %s / %s\n", i, sp.Key, spg.Key)
				equal = false
			}
			if sp.Value.ToInt().Cmp(spg.Value.ToInt()) != 0 {
				fmt.Printf("Different storage proof values in item %d: %x / %x\n", i, sp.Value.ToInt().Bytes(), spg.Value.ToInt().Bytes())
				equal = false
			}
			if len(sp.Proof) == len(spg.Proof) {
				for j, p := range sp.Proof {
					pg := spg.Proof[j]
					if p != pg {
						fmt.Printf("Different storage proof item %d in item %d: %s / %s\n", j, i, p, pg)
						equal = false
					}
				}
			} else {
				fmt.Printf("Different length of storage proof in the item %d: %d / %d\n", i, len(sp.Proof), len(spg.Proof))
				equal = false
			}
		}
	} else {
		fmt.Printf("Different length of StorageProof: %d / %d\n", len(r.StorageProof), len(rg.StorageProof))
		equal = false
	}
	return equal
}

func post(client *http.Client, url, request string, response interface{}) error {
	log.Info("Getting", "url", url, "request", request)
	start := time.Now()
	r, err := client.Post(url, "application/json", strings.NewReader(request))
	if err != nil {
		return err
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		return fmt.Errorf("status %s", r.Status)
	}
	decoder := json.NewDecoder(r.Body)
	err = decoder.Decode(response)
	log.Info("Got in", "time", time.Since(start).Seconds())
	return err
}

func print(client *http.Client, url, request string) {
	r, err := client.Post(url, "application/json", strings.NewReader(request))
	if err != nil {
		fmt.Printf("Could not print: %v\n", err)
		return
	}
	if r.StatusCode != 200 {
		fmt.Printf("Status %s", r.Status)
		return
	}
	fmt.Printf("ContentLength: %d\n", r.ContentLength)
	buf := make([]byte, 2000000)
	l, err := r.Body.Read(buf)
	if err != nil && err != io.EOF {
		fmt.Printf("Could not read response: %v\n", err)
		return
	}
	if l < len(buf) {
		fmt.Printf("Could not read response: %d out of %d\n", l, len(buf))
		//return
	}
	fmt.Printf("%s\n", buf[:l])
}

func setRoutes(tgUrl, gethURL string) {
	routes = make(map[string]string)
	routes[TurboGeth] = tgUrl
	routes[Geth] = gethURL
}
