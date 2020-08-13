package rpctest

import (
	"encoding/json"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"net"
	"net/http"
	"time"
)

func CompareAccountRange(tgURL, gethURL, tmpDataDir string, blockNum uint64)  {
	fmt.Println(tgURL)
	fmt.Println(gethURL)
	fmt.Println(tmpDataDir)
	fmt.Println(blockNum)
	fmt.Println(time.Time{}.IsZero())

	tmpDataDir="/media/b00ris/nvme/accrange"

	db:=ethdb.NewObjectDatabase(ethdb.NewLMDB().Path(tmpDataDir).MustOpen())
	//tgBucket:=[]byte("tgacc")
	//gethBucket:=[]byte("gethacc")

	fmt.Println("CompareAccountRange")
	var client = &http.Client{
		Timeout: time.Minute * 30,
		Transport: 		&http.Transport{
			MaxResponseHeaderBytes: 256*1024*1024,
			ReadBufferSize: 64*1024*1024,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Minute,
				KeepAlive: 30 * time.Minute,
				DualStack: true,
			}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Minute,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 90 * time.Minute,

		},


	}
	reqGen := &RequestGenerator{
		client: client,
	}

	type DebugAccountRange struct {
		CommonResponse
		Result state.IteratorDump `json:"result"`
	}
	i:=uint64(0)
	next:=[]byte{}
	for {
		reqGen.reqID++
		ar:=DebugAccountRange{}
		req:=reqGen.accountRange(int(blockNum),next, 256)
		fmt.Println(req)
		err:=post(client,tgURL, req,&ar)
		if err!=nil {
			fmt.Println(err)
			return
		}
		if ar.Error!=nil {
			fmt.Println("cmd/rpctest/rpctest/account_range_verify.go:63 ", err)
			fmt.Println(ar.Error)
			spew.Dump(ar)
			return
		}
		var addr common.Address
		var acc state.DumpAccount
		for addr,acc=range ar.Result.Accounts {
			i++
			b,err:=json.Marshal(acc)
			if err!=nil {
				fmt.Println("cmd/rpctest/rpctest/account_range_verify.go:70 ", err)
				return
			}
			err = db.Put(dbutils.AccountsHistoryBucket, addr.Bytes(), b)
			if err!=nil {
				fmt.Println("cmd/rpctest/rpctest/account_range_verify.go:76 ", err)
				return
			}
		}
		fmt.Println("request id", reqGen.reqID, "accounts", i, addr.String())
		if len(ar.Result.Next)==0 {
			break
		}
		next = ar.Result.Next
	}
}

