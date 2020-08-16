package rpctest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"net"
	"net/http"
	"os"
	"time"
)

func CompareAccountRange(tgURL, gethURL, tmpDataDir, gethDataDir string, blockNum uint64, notRegenerateGethData bool)  {
	err:=os.RemoveAll(tmpDataDir)
	if err!=nil {
		fmt.Println("cmd/rpctest/rpctest/account_range_verify.go:20 err", err)
		return
	}

	if !notRegenerateGethData {
		err=os.RemoveAll(gethDataDir)
		if err!=nil {
			fmt.Println("cmd/rpctest/rpctest/account_range_verify.go:27 err", err)
			return
		}
	}


	resultsKV:=ethdb.NewLMDB().Path(tmpDataDir).MustOpen()
	gethKV:=ethdb.NewLMDB().Path(gethDataDir).MustOpen()
	resultsDB:=ethdb.NewObjectDatabase(resultsKV)
	gethResultsDB:=ethdb.NewObjectDatabase(gethKV)

	var client = &http.Client{
		Timeout: time.Minute * 60,
		Transport: 		&http.Transport{
			MaxResponseHeaderBytes: 256*1024*1024,
			ReadBufferSize: 64*1024*1024,
			DialContext: (&net.Dialer{
				//Timeout:   60 * time.Minute,
				KeepAlive: 60 * time.Minute,
			}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			IdleConnTimeout:       60 * time.Minute,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 60 * time.Minute,
		},
	}

	type DebugAccountRange struct {
		CommonResponse
		Result state.IteratorDump `json:"result"`
	}

	f:=func (url string, db ethdb.Database) error {
		i:=uint64(0)
		reqGen := &RequestGenerator{
			client: client,
		}
		next:=[]byte{}
		for {
			reqGen.reqID++
			ar:=DebugAccountRange{}
			req:=reqGen.accountRange(int(blockNum),next, 256)
			fmt.Println(req)
			err:=post(client,url, req,&ar)
			if err!=nil {
				return fmt.Errorf("Post err %w", err)
			}
			if ar.Error!=nil {
				spew.Dump(ar)
				return fmt.Errorf("response error %w", ar.Error)
			}
			var addr common.Address
			var acc state.DumpAccount
			for addr,acc=range ar.Result.Accounts {
				i++
				b,err:=json.Marshal(acc)
				if err!=nil {
					return err
				}
				err = db.Put(dbutils.AccountsHistoryBucket, addr.Bytes(), b)
				if err!=nil {
					return err
				}
			}
			fmt.Println("request id", reqGen.reqID, "accounts", i, addr.String())
			if len(ar.Result.Next)==0 {
				return nil
			}
			next = ar.Result.Next
		}
	}
	err=f(tgURL,resultsDB)
	if err!=nil {
		fmt.Println("cmd/rpctest/rpctest/account_range_verify.go:100 err", err)
		return
	}

	if !notRegenerateGethData {
		err = f(gethURL, gethResultsDB)
		if err!=nil {
			fmt.Println("cmd/rpctest/rpctest/account_range_verify.go:107 err", err)
			return
		}
	}

	tgTx,err:=resultsKV.Begin(context.Background(), nil, false)
	if err!=nil {
		fmt.Println("cmd/rpctest/rpctest/account_range_verify.go:107 err", err)
		return
	}
	gethTx,err:=gethKV.Begin(context.Background(), nil, false)
	if err!=nil {
		fmt.Println("cmd/rpctest/rpctest/account_range_verify.go:107 err", err)
		return
	}
	tgCursor:=tgTx.Bucket(dbutils.AccountsHistoryBucket).Cursor()
	gethCursor:=gethTx.Bucket(dbutils.AccountsHistoryBucket).Cursor()

	tgKey, tgVal, err1:=tgCursor.Next()
	if err1!=nil {
		fmt.Println("cmd/rpctest/rpctest/account_range_verify.go:107 err", err1)
		return
	}

	gethKey, gethVal, err2:=gethCursor.Next()
	if err2!=nil {
		fmt.Println("cmd/rpctest/rpctest/account_range_verify.go:107 err", err2)
		return
	}


	i:=0
	errsNum:=0
	tgMissed:=0
	gethMissed:=0
	for {
		cmp, br:=common.KeyCmp(tgKey, gethKey)
		if br {
			break
		}
		if cmp == 0 {
			if !bytes.Equal(tgVal,gethVal) {
				errsNum++
				fmt.Println(common.Bytes2Hex(tgKey))
				fmt.Println(string(tgVal))
				fmt.Println(string(gethVal))
			}

			tgKey, tgVal, err1=tgCursor.Next()
			if err1!=nil {
				fmt.Println("cmd/rpctest/rpctest/account_range_verify.go:107 err", err1)
				return
			}
			gethKey, gethVal, err2=gethCursor.Next()
			if err2!=nil {
				fmt.Println("cmd/rpctest/rpctest/account_range_verify.go:107 err", err2)
				return
			}
		} else if cmp<0 {
			gethMissed++
			tgKey, tgVal, err1=tgCursor.Next()
			if err1!=nil {
				fmt.Println("cmd/rpctest/rpctest/account_range_verify.go:107 err", err1)
				return
			}
		} else if cmp>0 {
			tgMissed++
			gethKey, gethVal, err2=gethCursor.Next()
			if err2!=nil {
				fmt.Println("cmd/rpctest/rpctest/account_range_verify.go:107 err", err2)
				return
			}
		}
		i++
	}

}

