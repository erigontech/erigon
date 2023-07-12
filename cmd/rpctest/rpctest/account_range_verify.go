package rpctest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/davecgh/go-spew/spew"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/state"
)

func CompareAccountRange(logger log.Logger, erigonURL, gethURL, tmpDataDir, gethDataDir string, blockFrom uint64, notRegenerateGethData bool) {
	err := os.RemoveAll(tmpDataDir)
	if err != nil {
		log.Error(err.Error())
		return
	}

	if !notRegenerateGethData {
		err = os.RemoveAll(gethDataDir)
		if err != nil {
			log.Error(err.Error())
			return
		}
	}
	resultsKV := mdbx.NewMDBX(logger).Path(tmpDataDir).MustOpen()
	gethKV := mdbx.NewMDBX(logger).Path(gethDataDir).MustOpen()

	var client = &http.Client{
		Timeout: time.Minute * 60,
		Transport: &http.Transport{
			MaxResponseHeaderBytes: 256 * 1024 * 1024,
			ReadBufferSize:         64 * 1024 * 1024,
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

	f := func(url string, db kv.RwTx) error {
		i := uint64(0)
		reqGen := &RequestGenerator{
			client: client,
		}
		next := []byte{}
		for {
			reqGen.reqID++
			ar := DebugAccountRange{}
			req := reqGen.accountRange(blockFrom, next, 256)
			fmt.Println(req)
			err = post(client, url, req, &ar)
			if err != nil {
				return fmt.Errorf("post err %w", err)
			}
			if ar.Error != nil {
				spew.Dump(ar)
				return fmt.Errorf("response error %v", ar.Error)
			}
			var addr libcommon.Address
			var acc state.DumpAccount
			for addr, acc = range ar.Result.Accounts {
				i++
				b, innerErr := json.Marshal(acc)
				if innerErr != nil {
					return innerErr
				}
				err = db.Put(kv.E2AccountsHistory, addr.Bytes(), b)
				if err != nil {
					return err
				}
			}
			fmt.Println("request id", reqGen.reqID, "accounts", i, addr.String())
			if len(ar.Result.Next) == 0 {
				return nil
			}
			next = ar.Result.Next
		}
	}
	err = resultsKV.Update(context.Background(), func(tx kv.RwTx) error {
		return f(erigonURL, tx)
	})
	if err != nil {
		log.Error(err.Error())
		return

	}

	if !notRegenerateGethData {
		err = gethKV.Update(context.Background(), func(tx kv.RwTx) error {
			return f(erigonURL, tx)
		})
		if err != nil {
			log.Error(err.Error())
			return
		}
	}

	tgTx, err := resultsKV.BeginRo(context.Background())
	if err != nil {
		log.Error(err.Error())
		return
	}
	gethTx, err := gethKV.BeginRo(context.Background())
	if err != nil {
		log.Error(err.Error())
		return
	}
	tgCursor, err := tgTx.Cursor(kv.E2AccountsHistory)
	if err != nil {
		log.Error(err.Error())
		return
	}
	defer tgCursor.Close()
	gethCursor, err := gethTx.Cursor(kv.E2AccountsHistory)
	if err != nil {
		log.Error(err.Error())
		return
	}
	defer gethCursor.Close()

	tgKey, tgVal, err1 := tgCursor.Next()
	if err1 != nil {
		log.Error(err.Error())
		return
	}

	gethKey, gethVal, err2 := gethCursor.Next()
	if err2 != nil {
		log.Error(err.Error())
		return
	}

	i := 0
	errsNum := 0
	tgMissed := 0
	gethMissed := 0
	for {
		cmp, br := common.KeyCmp(tgKey, gethKey)
		if br {
			break
		}
		if cmp == 0 {
			if !bytes.Equal(tgVal, gethVal) {
				errsNum++
				fmt.Println(common.Bytes2Hex(tgKey))
				fmt.Println(string(tgVal))
				fmt.Println(string(gethVal))
			}

			tgKey, tgVal, err1 = tgCursor.Next()
			if err1 != nil {
				log.Error(err.Error())
				return

			}
			gethKey, gethVal, err2 = gethCursor.Next()
			if err2 != nil {
				log.Error(err.Error())
				return

			}
		} else if cmp < 0 {
			gethMissed++
			tgKey, tgVal, err1 = tgCursor.Next()
			if err1 != nil {
				log.Error(err.Error())
				return

			}
		} else if cmp > 0 {
			tgMissed++
			gethKey, gethVal, err2 = gethCursor.Next()
			if err2 != nil {
				log.Error(err.Error())
				return

			}
		}
		i++
	}
	fmt.Println("Errs", errsNum)
	fmt.Println("Missed", tgMissed)
	fmt.Println("geth Missed", gethMissed)
}
