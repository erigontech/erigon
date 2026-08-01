// inspect_td: read-only dump of kv.HeaderTD, kv.HeaderCanonical, kv.Headers
// around a target block for post-mortem investigation of the "parent's
// total difficulty not found" wedge.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/version"
)

var _ = version.GitCommit

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "usage: %s <chaindata-dir> <block-number> [target-hash-hex]\n", os.Args[0])
		os.Exit(1)
	}
	dir := os.Args[1]
	var blockNum uint64
	fmt.Sscanf(os.Args[2], "%d", &blockNum)

	var target []byte
	if len(os.Args) >= 4 {
		h, err := hex.DecodeString(os.Args[3])
		if err != nil {
			fmt.Fprintf(os.Stderr, "hex decode target: %v\n", err)
			os.Exit(1)
		}
		target = h
	}

	logger := log.New()
	logger.SetHandler(log.DiscardHandler())

	ctx := context.Background()
	minimal := kv.TableCfg{
		kv.HeaderCanonical: kv.ChaindataTablesCfg[kv.HeaderCanonical],
		kv.HeaderTD:        kv.ChaindataTablesCfg[kv.HeaderTD],
		kv.Headers:         kv.ChaindataTablesCfg[kv.Headers],
		kv.HeaderNumber:    kv.ChaindataTablesCfg[kv.HeaderNumber],
	}
	db, err := mdbx.New(dbcfg.ChainDB, logger).Path(dir).Accede(true).Readonly(true).
		WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg { return minimal }).
		Open(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open db: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	tx, err := db.BeginRo(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "begin ro: %v\n", err)
		db.Close()
		os.Exit(1) //nolint:gocritic // explicit db.Close above; defer above this branch is a no-op
	}
	defer tx.Rollback()

	prefix := make([]byte, 8)
	binary.BigEndian.PutUint64(prefix, blockNum)

	fmt.Printf("=== block %d ===\n", blockNum)
	if target != nil {
		fmt.Printf("target hash: %x\n", target)
	}

	// kv.HeaderCanonical — number → hash
	{
		v, err := tx.GetOne(kv.HeaderCanonical, prefix)
		if err != nil {
			fmt.Printf("kv.HeaderCanonical GetOne: err=%v\n", err)
		} else if len(v) == 0 {
			fmt.Printf("kv.HeaderCanonical[%d] = <MISSING>\n", blockNum)
		} else {
			fmt.Printf("kv.HeaderCanonical[%d] = %x\n", blockNum, v)
			if target != nil {
				fmt.Printf("  matches target? %v\n", bytes.Equal(v, target))
			}
		}
	}

	// kv.HeaderTD — (number, hash) → TD-rlp. Walk every entry with matching 8-byte prefix.
	{
		c, err := tx.Cursor(kv.HeaderTD)
		if err != nil {
			fmt.Fprintf(os.Stderr, "cursor kv.HeaderTD: %v\n", err)
			os.Exit(1)
		}
		defer c.Close()
		found := 0
		targetFound := false
		for k, v, err := c.Seek(prefix); k != nil && err == nil; k, v, err = c.Next() {
			if len(k) < 8 {
				continue
			}
			bn := binary.BigEndian.Uint64(k[:8])
			if bn != blockNum {
				break
			}
			hash := k[8:]
			fmt.Printf("kv.HeaderTD[%d, %x] = %x\n", bn, hash, v)
			found++
			if target != nil && bytes.Equal(hash, target) {
				targetFound = true
			}
		}
		if found == 0 {
			fmt.Printf("kv.HeaderTD[%d, *] = <NONE>\n", blockNum)
		}
		if target != nil {
			fmt.Printf("  target-hash TD present? %v\n", targetFound)
		}
	}

	// kv.Headers — (number, hash) → header-rlp. Walk 8-byte prefix.
	{
		c, err := tx.Cursor(kv.Headers)
		if err != nil {
			fmt.Fprintf(os.Stderr, "cursor kv.Headers: %v\n", err)
			os.Exit(1)
		}
		defer c.Close()
		found := 0
		for k, v, err := c.Seek(prefix); k != nil && err == nil; k, _, err = c.Next() {
			if len(k) < 8 {
				continue
			}
			bn := binary.BigEndian.Uint64(k[:8])
			if bn != blockNum {
				break
			}
			hash := k[8:]
			fmt.Printf("kv.Headers[%d, %x] (%d bytes)\n", bn, hash, len(v))
			found++
		}
		if found == 0 {
			fmt.Printf("kv.Headers[%d, *] = <NONE>\n", blockNum)
		}
	}

	// kv.HeaderNumber — hash → number. Only queryable by hash.
	if target != nil {
		v, err := tx.GetOne(kv.HeaderNumber, target)
		if err != nil {
			fmt.Printf("kv.HeaderNumber GetOne: err=%v\n", err)
		} else if len(v) == 0 {
			fmt.Printf("kv.HeaderNumber[%x] = <MISSING>\n", target)
		} else if len(v) >= 8 {
			fmt.Printf("kv.HeaderNumber[%x] = %d\n", target, binary.BigEndian.Uint64(v))
		} else {
			fmt.Printf("kv.HeaderNumber[%x] = <SHORT %d bytes>\n", target, len(v))
		}
	}
}
