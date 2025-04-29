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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/valyala/fastjson"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon/core/state"
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
	for i, txn := range r.Transactions {
		txg := rg.Transactions[i]
		if txn.From != txg.From {
			fmt.Printf("Tx %d different From: %x %x\n", i, txn.From, txg.From)
			return false
		}
		if (txn.To == nil && txg.To != nil) || (txn.To != nil && txg.To == nil) {
			fmt.Printf("Tx %d different To nilness: %t %t\n", i, txn.To == nil, txg.To == nil)
			return false
		}
		if txn.To != nil && txg.To != nil && *txn.To != *txg.To {
			fmt.Printf("Tx %d different To: %x %x\n", i, *txn.To, *txg.To)
			return false
		}
		if txn.Hash != txg.Hash {
			fmt.Printf("Tx %x different Hash: %s %s\n", i, txn.Hash, txg.Hash)
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

func compareJsonValues(prefix string, v, vg *fastjson.Value) error {
	var vType = fastjson.TypeNull
	var vgType = fastjson.TypeNull
	if v != nil {
		vType = v.Type()
	}
	if vg != nil {
		vgType = vg.Type()
	}
	if vType != vgType {
		return fmt.Errorf("different types for prefix %s: %s / %s", prefix, vType.String(), vgType.String())
	}
	switch vType {
	case fastjson.TypeNull:
		// Nothing to do
	case fastjson.TypeObject:
		obj, err := v.Object()
		if err != nil {
			return fmt.Errorf("convering erigon val to object at prefix %s: %w", prefix, err)
		}
		objg, errg := vg.Object()
		if errg != nil {
			return fmt.Errorf("convering g val to object at prefix %s: %w", prefix, errg)
		}
		objg.Visit(func(key []byte, vg1 *fastjson.Value) {
			if err != nil {
				return
			}
			v1 := obj.Get(string(key))
			if v1 == nil && vg1.Type() != fastjson.TypeNull {
				err = fmt.Errorf("erigon missing value at prefix: %s", prefix+"."+string(key))
				return
			}
			if e := compareJsonValues(prefix+"."+string(key), v1, vg1); e != nil {
				err = e
			}
		})
		if err != nil {
			return err
		}
		// Finding keys that are present in Erigon but missing in G
		obj.Visit(func(key []byte, v1 *fastjson.Value) {
			if err != nil {
				return
			}
			if objg.Get(string(key)) == nil && v1.Type() != fastjson.TypeNull {
				err = fmt.Errorf("g missing value at prefix: %s", prefix+"."+string(key))
				return
			}
		})
		if err != nil {
			return err
		}
	case fastjson.TypeArray:
		arr, err := v.Array()
		if err != nil {
			return fmt.Errorf("converting Erigon val to array at prefix %s: %w", prefix, err)
		}
		arrg, errg := vg.Array()
		if errg != nil {
			return fmt.Errorf("converting g val to array at prefix %s: %w", prefix, errg)
		}
		if len(arr) != len(arrg) {
			return fmt.Errorf("arrays have different length at prefix %s: %d / %d", prefix, len(arr), len(arrg))
		}
		for i, item := range arr {
			itemg := arrg[i]
			if e := compareJsonValues(fmt.Sprintf("%s[%d]", prefix, i), item, itemg); e != nil {
				return e
			}
		}
	case fastjson.TypeString:
		if v.String() != vg.String() {
			return fmt.Errorf("different string values at prefix %s: %s / %s", prefix, v.String(), vg.String())
		}
	case fastjson.TypeNumber:
		i, err := v.Int()
		if err != nil {
			return fmt.Errorf("converting Erigon val to int at prefix %s: %w", prefix, err)
		}
		ig, errg := vg.Int()
		if errg != nil {
			return fmt.Errorf("converting g val to int at prefix %s: %w", prefix, errg)
		}
		if i != ig {
			return fmt.Errorf("different int values at prefix %s: %d / %d", prefix, i, ig)
		}
	}
	return nil
}

func compareResults(trace, traceg *fastjson.Value) error {
	r := trace.Get("result")
	rg := traceg.Get("result")
	return compareJsonValues("result", r, rg)
}

func compareErrors(errVal *fastjson.Value, errValg *fastjson.Value, methodName string, errCtx string, errs *bufio.Writer) error {
	if errVal != nil && errValg == nil {
		if errs != nil {
			fmt.Printf("different results for method %s, errCtx: %s\n", methodName, errCtx)
			fmt.Fprintf(errs, "Different results for method %s, errCtx: %s\n", methodName, errCtx)
			fmt.Fprintf(errs, "Result (Erigon) returns error code=%d message=%s, while G/OE returns OK\n", errVal.GetInt("code"), errVal.GetStringBytes("message"))
			errs.Flush() // nolint:errcheck
		} else {
			return fmt.Errorf("different result (Erigon) returns error code=%d message=%s, while G/OE returns OK", errVal.GetInt("code"), errVal.GetStringBytes("message"))
		}
	} else if errVal == nil && errValg != nil {
		if errs != nil {
			fmt.Printf("different results for method %s, errCtx: %s\n", methodName, errCtx)
			fmt.Fprintf(errs, "Different results for method %s, errCtx: %s\n", methodName, errCtx)
			fmt.Fprintf(errs, "Result (Erigon) returns OK, while G/OE returns error code=%d message=%s\n", errValg.GetInt("code"), errValg.GetStringBytes("message"))
			errs.Flush() // nolint:errcheck
		} else {
			return fmt.Errorf("different result (Erigon) returns OK, while G/OE returns error code=%d message=%s", errValg.GetInt("code"), errValg.GetStringBytes("message"))
		}
	} else {
		s1 := strings.ToUpper(string(errVal.GetStringBytes("message")))
		s2 := strings.ToUpper(string(errValg.GetStringBytes("message")))
		if strings.Compare(s1, s2) != 0 {
			if errs != nil {
				fmt.Printf("different error-message for method %s, errCtx: %s\n", methodName, errCtx)
				fmt.Fprintf(errs, "Different results for method %s, errCtx: %s\n", methodName, errCtx)
				fmt.Fprintf(errs, "error-message (Erigon) returns message=%s, while G/OE returns message=%s\n", errVal.GetStringBytes("message"), errValg.GetStringBytes("message"))
				errs.Flush() // nolint:errcheck
			} else {
				return fmt.Errorf("different error-message (Erigon) returns message=%s, while G/OE returns message=%s", errVal.GetStringBytes("message"), errValg.GetStringBytes("message"))
			}
		} else if errVal.GetInt("code") != errValg.GetInt("code") {
			if errs != nil {
				fmt.Printf("Different error-code for method %s, errCtx: %s\n", methodName, errCtx)
				fmt.Fprintf(errs, "Different results for method %s, errCtx: %s\n", methodName, errCtx)
				fmt.Fprintf(errs, "error-code (Erigon) returns code=%d, while G/OE returns code=%d\n", errVal.GetInt("code"), errValg.GetInt("code"))
				errs.Flush() // nolint:errcheck
			} else {
				return fmt.Errorf("different error-code (Erigon) returns code=%d, while G/OE returns code=%d", errVal.GetInt("code"), errValg.GetInt("code"))
			}
		}
	}
	return nil
}

func requestAndCompare(request string, methodName string, errCtx string, reqGen *RequestGenerator, needCompare bool, rec *bufio.Writer, errs *bufio.Writer, channel chan CallResult, insertOnlyIfSuccess bool) error {
	recording := rec != nil
	res := reqGen.Erigon2(methodName, request)
	if res.Err != nil {
		return fmt.Errorf("could not invoke %s (Erigon): %w", methodName, res.Err)
	}
	errVal := res.Result.Get("error")
	if errVal != nil {
		if !needCompare && channel == nil {
			return fmt.Errorf("error invoking %s (Erigon): %d %s", methodName, errVal.GetInt("code"), errVal.GetStringBytes("message"))
		}
	}
	if needCompare {
		resg := reqGen.Geth2(methodName, request)
		if resg.Err != nil {
			return fmt.Errorf("could not invoke %s (Geth/OE): %w", methodName, res.Err)
		}
		errValg := resg.Result.Get("error")
		if errVal == nil && errValg == nil {
			if err := compareResults(res.Result, resg.Result); err != nil {
				recording = false
				if errs != nil {
					fmt.Printf("different results for method %s, errCtx: %s: %v\n", methodName, errCtx, err)
					fmt.Fprintf(errs, "\nDifferent results for method %s, errCtx %s: %v\n", methodName, errCtx, err)
					fmt.Fprintf(errs, "Request=====================================\n%s\n", request)
					fmt.Fprintf(errs, "TG response=================================\n%s\n", res.Response)
					fmt.Fprintf(errs, "G/OE response=================================\n%s\n", resg.Response)
					errs.Flush() // nolint:errcheck
					// Keep going
				} else {
					reqFile, _ := os.Create("request.json")                //nolint:errcheck
					reqFile.Write([]byte(request))                         //nolint:errcheck
					reqFile.Close()                                        //nolint:errcheck
					erigonRespFile, _ := os.Create("erigon-response.json") //nolint:errcheck
					erigonRespFile.Write(res.Response)                     //nolint:errcheck
					erigonRespFile.Close()                                 //nolint:errcheck
					oeRespFile, _ := os.Create("oe-response.json")         //nolint:errcheck
					oeRespFile.Write(resg.Response)                        //nolint:errcheck
					oeRespFile.Close()                                     //nolint:errcheck
					return fmt.Errorf("different results for method %s, errCtx %s: %v\nRequest in file request.json, Erigon response in file erigon-response.json, Geth/OE response in file oe-response.json", methodName, errCtx, err)
				}
			}
		} else {
			return compareErrors(errVal, errValg, methodName, errCtx, errs)
		}
	} else {
		if channel != nil {
			if insertOnlyIfSuccess == false || (insertOnlyIfSuccess && errVal == nil) {
				channel <- res
			}
		}
	}

	if recording {
		fmt.Fprintf(rec, "%s\n%s\n\n", request, res.Response)
	}
	return nil
}

func requestAndCompareErigon(requestA, requestB string, methodNameA, methodNameB string, errCtx string, reqGen *RequestGenerator, needCompare bool, rec *bufio.Writer, errs *bufio.Writer, channel chan CallResult, insertOnlyIfSuccess bool) error {
	recording := rec != nil
	res := reqGen.Erigon2(methodNameA, requestA)
	if res.Err != nil {
		return fmt.Errorf("could not invoke %s (Erigon): %w", methodNameA, res.Err)
	}
	errVal := res.Result.Get("error")
	if errVal != nil {
		if !needCompare && channel == nil {
			return fmt.Errorf("error invoking %s (Erigon): %d %s", methodNameA, errVal.GetInt("code"), errVal.GetStringBytes("message"))
		}
	}
	if needCompare {
		resg := reqGen.Erigon2(methodNameB, requestB)
		if resg.Err != nil {
			return fmt.Errorf("could not invoke %s (Geth/OE): %w", methodNameB, res.Err)
		}
		errValg := resg.Result.Get("error")
		if errVal == nil && errValg == nil {
			if err := compareResults(res.Result, resg.Result); err != nil {
				recording = false
				if errs != nil {
					fmt.Printf("different results for methods %s, %s, errCtx: %s: %v\n", methodNameA, methodNameB, errCtx, err)
					fmt.Fprintf(errs, "\nDifferent results for methods %s, %s, errCtx %s: %v\n", methodNameA, methodNameB, errCtx, err)
					fmt.Fprintf(errs, "Request=====================================\n%s\n", requestA)
					fmt.Fprintf(errs, "%s response=================================\n%s\n", methodNameA, res.Response)
					fmt.Fprintf(errs, "%s response=================================\n%s\n", methodNameB, resg.Response)
					errs.Flush() // nolint:errcheck
					// Keep going
				} else {
					reqFile, _ := os.Create("request.json")                //nolint:errcheck
					reqFile.Write([]byte(requestA))                        //nolint:errcheck
					reqFile.Close()                                        //nolint:errcheck
					erigonRespFile, _ := os.Create("erigon-response.json") //nolint:errcheck
					erigonRespFile.Write(res.Response)                     //nolint:errcheck
					erigonRespFile.Close()                                 //nolint:errcheck
					oeRespFile, _ := os.Create("oe-response.json")         //nolint:errcheck
					oeRespFile.Write(resg.Response)                        //nolint:errcheck
					oeRespFile.Close()                                     //nolint:errcheck
					return fmt.Errorf("different results for methods %s, %s, errCtx %s: %v\nRequest in file request.json, Erigon response in file erigon-response.json, Geth/OE response in file oe-response.json", methodNameA, methodNameB, errCtx, err)
				}
			}
		} else {
			//TODO fix for two methods
			return compareErrors(errVal, errValg, methodNameA, errCtx, errs)
		}
	} else {
		if channel != nil {
			if insertOnlyIfSuccess == false || (insertOnlyIfSuccess && errVal == nil) {
				channel <- res
			}
		}
	}

	if recording {
		fmt.Fprintf(rec, "%s\n%s\n\n", requestA, res.Response)
	}
	return nil
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
		fmt.Printf("Different txn hashes: %x %x\n", r.TxHash, rg.TxHash)
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

func getTopics(v *fastjson.Value) []common.Hash {
	topicSet := make(map[common.Hash]struct{})
	r := v.GetArray("result")
	for _, l := range r {
		for _, t := range l.GetArray("topics") {
			topic := common.HexToHash(t.String())
			topicSet[topic] = struct{}{}
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

func compareAccountRanges(erigon, geth map[common.Address]state.DumpAccount) bool {
	allAddresses := make(map[common.Address]struct{})
	for k := range erigon {
		allAddresses[k] = struct{}{}
	}

	for k := range geth {
		allAddresses[k] = struct{}{}
	}

	for addr := range allAddresses {
		eriAcc, eriOk := erigon[addr]
		if !eriOk {
			fmt.Printf("missing account in Erigon %x\n", addr)
			return false
		}

		gethAcc, gethOk := geth[addr]
		if !gethOk {
			fmt.Printf("missing account in Geth %x\n", addr)
			return false
		}
		different := false
		if eriAcc.Balance != gethAcc.Balance {
			fmt.Printf("Different balance for %x: erigon %s, geth %s\n", addr, eriAcc.Balance, gethAcc.Balance)
			different = true
		}
		if eriAcc.Nonce != gethAcc.Nonce {
			fmt.Printf("Different nonce for %x: erigon %d, geth %d\n", addr, eriAcc.Nonce, gethAcc.Nonce)
			different = true
		}
		// We do not compare Root, because Erigon does not compute it
		if eriAcc.CodeHash.String() != gethAcc.CodeHash.String() {
			fmt.Printf("Different codehash for %x: erigon %s, geth %s\n", addr, eriAcc.CodeHash, gethAcc.CodeHash)
			different = true
		}
		if eriAcc.Code.String() != gethAcc.Code.String() {
			fmt.Printf("Different codehash for %x: erigon %s, geth %s\n", addr, eriAcc.Code, gethAcc.Code)
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
	//fmt.Printf("Request=%s\n", request)
	//log.Info("Getting", "url", url, "request", request)
	//start := time.Now()
	r, err := client.Post(url, "application/json", strings.NewReader(request))
	if err != nil {
		return err
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		return fmt.Errorf("status %s", r.Status)
	}
	b, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	err = json.Unmarshal(b, response)
	if err != nil {
		fmt.Printf("json: %s\n", string(b))
		panic(err)
	}
	//log.Info("Got in", "time", time.Since(start).Seconds())
	return err
}

func post2(client *http.Client, url, request string) ([]byte, *fastjson.Value, error) {
	//fmt.Printf("Request=%s\n", request)
	//log.Info("Getting", "url", url, "request", request)
	//start := time.Now()
	r, err := client.Post(url, "application/json", strings.NewReader(request))
	if err != nil {
		return nil, nil, err
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		return nil, nil, fmt.Errorf("status %s", r.Status)
	}
	var buf bytes.Buffer
	if _, err = buf.ReadFrom(r.Body); err != nil {
		return nil, nil, fmt.Errorf("reading http response: %w", err)
	}
	var p fastjson.Parser
	response := buf.Bytes()
	v, err := p.ParseBytes(response)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing http response: %w", err)
	}
	//log.Info("Got in", "time", time.Since(start).Seconds())
	return response, v, nil
}

func print(client *http.Client, url, request string) {
	r, err := client.Post(url, "application/json", strings.NewReader(request))
	if err != nil {
		fmt.Printf("Could not print: %v\n", err)
		return
	}
	defer r.Body.Close()
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

func setRoutes(erigonUrl, gethURL string) {
	routes = make(map[string]string)
	routes[Erigon] = erigonUrl
	routes[Geth] = gethURL
}

func compareBlockTransactions(b, bg *OtsBlockTransactions) bool {
	r := b.Result
	rg := bg.Result
	if r.FullBlock.Difficulty.ToInt().Cmp(rg.FullBlock.Difficulty.ToInt()) != 0 {
		fmt.Printf("Different Difficulty  %d %d\n", r.FullBlock.Difficulty.ToInt(), rg.FullBlock.Difficulty.ToInt())
		return false
	}
	if r.FullBlock.ExtraData != rg.FullBlock.ExtraData {
		fmt.Printf("Different ExtraData  %s %s\n", r.FullBlock.ExtraData, rg.FullBlock.ExtraData)
		return false
	}
	if r.FullBlock.GasLimit.ToInt().Cmp(rg.FullBlock.GasLimit.ToInt()) != 0 {
		fmt.Printf("Different GasLimit  %d %d\n", r.FullBlock.GasLimit.ToInt(), rg.FullBlock.GasLimit.ToInt())
		return false
	}
	if r.FullBlock.GasUsed.ToInt().Cmp(rg.FullBlock.GasUsed.ToInt()) != 0 {
		fmt.Printf("Different GasUsed  %d %d\n", r.FullBlock.GasUsed.ToInt(), rg.FullBlock.GasUsed.ToInt())
		return false
	}
	if r.FullBlock.Hash.String() != rg.FullBlock.Hash.String() {
		fmt.Printf("Different Hash  %s %s\n", r.FullBlock.Hash.String(), rg.FullBlock.Hash.String())
		return false
	}
	if r.FullBlock.Bloom != rg.FullBlock.Bloom {
		fmt.Printf("Different Bloom  %s %s\n", r.FullBlock.Bloom, rg.FullBlock.Bloom)
		return false
	}
	if r.FullBlock.Miner != rg.FullBlock.Miner {
		fmt.Printf("Different Miner  %x %x\n", r.FullBlock.Miner, rg.FullBlock.Miner)
		return false
	}
	if r.FullBlock.MixHash != rg.FullBlock.MixHash {
		fmt.Printf("Different MixHash  %s %s\n", r.FullBlock.MixHash, rg.FullBlock.MixHash)
		return false
	}
	if r.FullBlock.Nonce != rg.FullBlock.Nonce {
		fmt.Printf("Different Nonce  %s %s\n", r.FullBlock.Nonce, rg.FullBlock.Nonce)
		return false
	}
	if r.FullBlock.Number.ToInt().Cmp(rg.FullBlock.Number.ToInt()) != 0 {
		fmt.Printf("Different Number  %d %d\n", r.FullBlock.Number.ToInt(), rg.FullBlock.Number.ToInt())
		return false
	}
	if r.FullBlock.ParentHash != rg.FullBlock.ParentHash {
		fmt.Printf("Different ParentHash  %s %s\n", r.FullBlock.ParentHash, rg.FullBlock.ParentHash)
		return false
	}
	if r.FullBlock.ReceiptsRoot != rg.FullBlock.ReceiptsRoot {
		fmt.Printf("Different ReceiptsRoot  %s %s\n", r.FullBlock.ReceiptsRoot, rg.FullBlock.ReceiptsRoot)
		return false
	}
	if r.FullBlock.Sha3Uncles != rg.FullBlock.Sha3Uncles {
		fmt.Printf("Different Sha3Uncles  %s %s\n", r.FullBlock.Sha3Uncles, rg.FullBlock.Sha3Uncles)
		return false
	}
	if r.FullBlock.Size.ToInt().Cmp(rg.FullBlock.Size.ToInt()) != 0 {
		fmt.Printf("Different Size  %d %d\n", r.FullBlock.Size.ToInt(), rg.FullBlock.Size.ToInt())
		return false
	}
	if r.FullBlock.StateRoot != rg.FullBlock.StateRoot {
		fmt.Printf("Different StateRoot  %s %s\n", r.FullBlock.StateRoot, rg.FullBlock.StateRoot)
		return false
	}
	if r.FullBlock.Timestamp != rg.FullBlock.Timestamp {
		fmt.Printf("Different Timestamp  %s %s\n", r.FullBlock.Timestamp, rg.FullBlock.Timestamp)
		return false
	}
	if len(r.FullBlock.Transactions) != len(rg.FullBlock.Transactions) {
		fmt.Printf("Num of txs different: %d %d\n", len(r.FullBlock.Transactions), len(rg.FullBlock.Transactions))
		return false
	}
	for i, uncle := range r.FullBlock.Uncles {
		if uncle != rg.FullBlock.Uncles[i] {
			fmt.Printf("Uncles %d different: %x %x\n", i, uncle, rg.FullBlock.Uncles[i])
			return false
		}
	}
	for i, txn := range r.FullBlock.Transactions {
		txg := rg.FullBlock.Transactions[i]
		if txn.From != txg.From {
			fmt.Printf("Tx %d different From: %x %x\n", i, txn.From, txg.From)
			return false
		}
		if (txn.To == nil && txg.To != nil) || (txn.To != nil && txg.To == nil) {
			fmt.Printf("Tx %d different To nilness: %t %t\n", i, txn.To == nil, txg.To == nil)
			return false
		}
		if txn.To != nil && txg.To != nil && *txn.To != *txg.To {
			fmt.Printf("Tx %d different To: %x %x\n", i, *txn.To, *txg.To)
			return false
		}
		if txn.Hash != txg.Hash {
			fmt.Printf("Tx %d different Hash: %s %s\n", i, txn.Hash, txg.Hash)
			return false
		}
		if txn.BlockHash.String() != txg.BlockHash.String() {
			fmt.Printf("Tx %d different BlockHash: %s %s\n", i, txn.BlockHash.String(), txg.BlockHash.String())
			return false
		}
		if txn.BlockNumber.String() != txg.BlockNumber.String() {
			fmt.Printf("Tx %d different TransactionHash: %s %s\n", i, txn.BlockNumber.String(), txg.BlockNumber.String())
			return false
		}
		if txn.Gas.ToInt().Cmp(txg.Gas.ToInt()) != 0 {
			fmt.Printf("Tx %d different Gas: %d %d\n", i, txn.Gas.ToInt(), txg.Gas.ToInt())
			return false
		}
		if txn.GasPrice.ToInt().Cmp(txg.GasPrice.ToInt()) != 0 {
			fmt.Printf("Tx %d different GasPrice: %d %d\n", i, txn.GasPrice.ToInt(), txg.GasPrice.ToInt())
			return false
		}
		if txn.Input.String() != txg.Input.String() {
			fmt.Printf("Tx %d different Input: %s %s\n", i, txn.Input.String(), txg.Input.String())
			return false
		}
		if txn.TransactionIndex.String() != txg.TransactionIndex.String() {
			fmt.Printf("Tx %d different TransactionIndex: %s %s\n", i, txn.TransactionIndex.String(), txg.TransactionIndex.String())
			return false
		}
		if txn.Value.ToInt().Cmp(txg.Value.ToInt()) != 0 {
			fmt.Printf("Tx %d different Value: %d %d\n", i, txn.Value.ToInt(), txg.Value.ToInt())
			return false
		}
		if txn.Type.ToInt().Cmp(txg.Type.ToInt()) != 0 {
			fmt.Printf("Tx %d different Type: %d %d\n", i, txn.Type.ToInt(), txg.Type.ToInt())
			return false
		}
		if txn.ChainId.ToInt().Cmp(txg.ChainId.ToInt()) != 0 {
			fmt.Printf("Tx %d different ChainId: %d %d\n", i, txn.ChainId.ToInt(), txg.ChainId.ToInt())
			return false
		}
	}
	for i, rcp := range r.Receipts {
		rcpg := rg.Receipts[i]
		if rcp.From != rcpg.From {
			fmt.Printf("Receipt %d different From: %x %x\n", i, rcp.From, rcpg.From)
			return false
		}
		if (rcp.To == nil && rcpg.To != nil) || (rcp.To != nil && rcpg.To == nil) {
			fmt.Printf("Receipt %d different To nilness: %t %t\n", i, rcp.To == nil, rcpg.To == nil)
			return false
		}
		if rcp.To != nil && rcpg.To != nil && *rcp.To != *rcpg.To {
			fmt.Printf("Receipt %d different To: %x %x\n", i, *rcp.To, *rcpg.To)
			return false
		}
		if rcp.BlockHash != rcpg.BlockHash {
			fmt.Printf("Receipt %d different Hash: %s %s\n", i, rcp.BlockHash, rcpg.BlockHash)
			return false
		}
		if rcp.ContractAddress != rcpg.ContractAddress {
			fmt.Printf("Receipt %d different ContractAddress: %s %s\n", i, rcp.ContractAddress, rcpg.ContractAddress)
			return false
		}
		if rcp.CumulativeGasUsed.ToInt().Cmp(rcpg.CumulativeGasUsed.ToInt()) != 0 {
			fmt.Printf("Receipt %d different CumulativeGasUsed: %d %d\n", i, rcp.CumulativeGasUsed.ToInt(), rcpg.CumulativeGasUsed.ToInt())
			return false
		}
		if rcp.EffectiveGasPrice.ToInt().Cmp(rcpg.EffectiveGasPrice.ToInt()) != 0 {
			fmt.Printf("Receipt %d different EffectiveGasPrice: %d %d\n", i, rcp.EffectiveGasPrice.ToInt(), rcpg.EffectiveGasPrice.ToInt())
			return false
		}
		if rcp.GasUsed.ToInt().Cmp(rcpg.GasUsed.ToInt()) != 0 {
			fmt.Printf("Receipt %d different GasUsed: %d %d\n", i, rcp.GasUsed.ToInt(), rcpg.GasUsed.ToInt())
			return false
		}
		if rcp.TransactionHash != rcpg.TransactionHash {
			fmt.Printf("Receipt %d different TransactionHash: %s %s\n", i, rcp.TransactionHash, rcpg.TransactionHash)
			return false
		}
		if rcp.BlockHash.String() != rcpg.BlockHash.String() {
			fmt.Printf("Receipt %d different TransactionHash: %s %s\n", i, rcp.BlockHash.String(), rcpg.BlockHash.String())
			return false
		}
		if rcp.BlockNumber.String() != rcpg.BlockNumber.String() {
			fmt.Printf("Receipt %d different TransactionHash: %s %s\n", i, rcp.BlockNumber.String(), rcpg.BlockNumber.String())
			return false
		}
	}
	return true
}
