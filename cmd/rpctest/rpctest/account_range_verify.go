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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/davecgh/go-spew/spew"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx"
)

func CompareAccountRange(logger log.Logger, erigonURL, gethURL, tmpDataDir, gethDataDir string, blockFrom uint64, notRegenerateGethData bool) {
	err := dir.RemoveAll(tmpDataDir)
	if err != nil {
		log.Error(err.Error())
		return
	}

	if !notRegenerateGethData {
		err = dir.RemoveAll(gethDataDir)
		if err != nil {
			log.Error(err.Error())
			return
		}
	}
	resultsKV := mdbx.New(kv.ChainDB, logger).Path(tmpDataDir).MustOpen()
	gethKV := mdbx.New(kv.ChainDB, logger).Path(gethDataDir).MustOpen()

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
		reqGen := &RequestGenerator{}
		next := []byte{}
		for {
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
			var addr common.Address
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
			fmt.Println("request id", reqGen.reqID.Load(), "accounts", i, addr.String())
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
	defer tgTx.Rollback()
	gethTx, err := gethKV.BeginRo(context.Background())
	if err != nil {
		log.Error(err.Error())
		return
	}
	defer gethTx.Rollback()
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
