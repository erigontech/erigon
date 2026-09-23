package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/c2h5oh/datasize"
	mdbxgo "github.com/erigontech/mdbx-go/mdbx"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
)

func main() {
	path := flag.String("path", "", "db path")
	syncBytes := flag.String("bytes", "0", "sync bytes threshold, 0 = durable")
	total := flag.String("total", "256MB", "total bytes to write")
	valSize := flag.Int("val", 4096, "value size")
	flag.Parse()

	var sb, tot datasize.ByteSize
	if err := sb.UnmarshalText([]byte(*syncBytes)); err != nil {
		panic(err)
	}
	if err := tot.UnmarshalText([]byte(*total)); err != nil {
		panic(err)
	}

	opts := mdbx.New(dbcfg.ChainDB, log.New()).Path(*path).
		WithTableCfg(func(kv.TableCfg) kv.TableCfg { return kv.ChaindataTablesCfg }).
		MapSize(16 * datasize.GB)
	if sb > 0 {
		opts = opts.Flags(func(f uint) uint { return f&^mdbxgo.Durable | mdbxgo.SafeNoSync }).
			SyncBytes(sb)
	}
	db := opts.MustOpen()
	defer db.Close()

	val := make([]byte, *valSize)
	rand.Read(val)
	key := make([]byte, 8)
	written := uint64(0)
	start := time.Now()
	for i := uint64(0); written < tot.Bytes(); i++ {
		if err := db.Update(context.Background(), func(tx kv.RwTx) error {
			for j := 0; j < 64; j++ {
				binary.BigEndian.PutUint64(key, i*64+uint64(j))
				if err := tx.Put(kv.HeaderTD, key, val); err != nil {
					return err
				}
				written += uint64(*valSize)
			}
			return nil
		}); err != nil {
			panic(err)
		}
	}
	fi, _ := os.Stat(*path + "/mdbx.dat")
	fmt.Printf("wrote=%s in %.1fs file=%s\n", datasize.ByteSize(written).HR(), time.Since(start).Seconds(), datasize.ByteSize(fi.Size()).HR())
}
