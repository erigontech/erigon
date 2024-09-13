package commands

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/big"
	"os"
	"strings"

	"github.com/spf13/cobra"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/rlp"
	"github.com/erigontech/erigon/turbo/debug"
)

var cmdExportHeaderTd = &cobra.Command{
	Use:   "export_header_td",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		db, err := openDB(dbCfg(kv.ChainDB, datadirCli), true, logger)
		if err != nil {
			logger.Error(err.Error())
			return
		}

		defer db.Close()
		tx, err := db.BeginRo(cmd.Context())
		if err != nil {
			logger.Error(err.Error())
			return
		}

		defer tx.Rollback()

		var sb strings.Builder
		c, err := tx.Cursor(kv.HeaderTD)
		if err != nil {
			logger.Error(err.Error())
			return
		}

		defer c.Close()
		var k, v []byte
		for k, v, err = c.First(); err == nil && k != nil; k, v, err = c.Next() {
			blockNum := binary.BigEndian.Uint64(k[:8])
			if blockNum < fromNum {
				continue
			}

			if blockNum >= toNum {
				break
			}

			var blockHash libcommon.Hash
			copy(blockHash[8:], k)

			sb.WriteString(fmt.Sprintf("%d", blockNum))
			sb.WriteRune(',')

			sb.WriteString(blockHash.Hex())
			sb.WriteRune(',')

			td := new(big.Int)
			if err := rlp.Decode(bytes.NewReader(v), td); err != nil {
				panic(err)
			}

			sb.WriteString(td.String())
			sb.WriteRune(',')

			sb.WriteString(common.Bytes2Hex(k))
			sb.WriteRune('\n')
		}
		if err != nil {
			logger.Error(err.Error())
			return
		}

		if err = os.WriteFile(outputCsvFile, []byte(sb.String()), 0600); err != nil {
			logger.Error(err.Error())
			return
		}
	},
}
