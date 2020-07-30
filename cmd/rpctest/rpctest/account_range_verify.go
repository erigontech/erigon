package rpctest

import (
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"net/http"
	"time"
)

func CompareAccountRange(tgURL, gethURL, tmpDataDir string, blockNum uint64)  {
	fmt.Println(tgURL)
	fmt.Println(gethURL)
	fmt.Println(tmpDataDir)
	fmt.Println(blockNum)
	db:=ethdb.NewObjectDatabase(ethdb.NewLMDB().Path(tmpDataDir).MustOpen())

	var client = &http.Client{
		Timeout: time.Second * 600,
	}
	reqGen := &RequestGenerator{
		client: client,
	}

	type DebugAccountRange struct {
		CommonResponse
		Result state.IteratorDump `json:"result"`
	}
	reqGen.reqID++
	ar:=DebugAccountRange{}
	req:=reqGen.accountRange(int(blockNum),[]byte{})
	fmt.Println(req)
	err:=post(client,tgURL, req,&ar)
	if err!=nil {
		fmt.Println(err)
		return
	}
	fmt.Println(ar.Result.Next)
	spew.Dump(ar)

_=db
}

