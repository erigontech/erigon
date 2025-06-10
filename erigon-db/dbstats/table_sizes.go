package dbstats

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
)

type TableSize struct {
	Name string
	Size uint64
}

func CollectTableSizes(ctx context.Context, db kv.RoDB) ([]TableSize, error) {
	allTablesCfg := db.AllTables()
	allTables := make([]string, 0, len(allTablesCfg))
	for table, cfg := range allTablesCfg {
		if cfg.IsDeprecated {
			continue
		}

		allTables = append(allTables, table)
	}

	var freeListSize uint64
	var err error
	tableSizes := make([]TableSize, 0, len(allTables))
	err = db.View(ctx, func(tx kv.Tx) error {
		for _, table := range allTables {
			sz, err := tx.BucketSize(table)
			if err != nil {
				return err
			}

			tableSizes = append(tableSizes, TableSize{Name: table, Size: sz})
		}

		freeListSize, err = tx.BucketSize("freelist")
		if err != nil {
			return err
		}

		tableSizes = append(tableSizes, TableSize{Name: "Freelist", Size: freeListSize})
		return nil
	})
	if err != nil {
		return nil, err
	}

	amountOfFreePagesInDb := freeListSize / 4 // page_id encoded as bigEndian_u32
	tableSizes = append(tableSizes, TableSize{
		Name: "ReclaimableSpace",
		Size: amountOfFreePagesInDb * db.PageSize().Bytes(),
	})

	sort.Slice(tableSizes, func(i, j int) bool {
		return tableSizes[i].Size > tableSizes[j].Size
	})

	return tableSizes, nil
}

func PrintTableSizesPeriodically(ctx context.Context, db kv.RoDB, logger log.Logger) {
	if !dbg.EnvBool("PRINT_TABLE_SIZES", false) {
		return
	}

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tableSizes, err := CollectTableSizes(ctx, db)
			if err != nil {
				logger.Error("[dbstats] failed to collect table sizes", "err", err)
				continue
			}

			var sb strings.Builder
			for _, t := range tableSizes {
				if t.Size == 0 {
					continue
				}

				sb.WriteString(t.Name)
				sb.WriteRune(':')
				sb.WriteString(common.ByteCount(t.Size))
				sb.WriteRune(',')
			}

			logger.Debug("[dbstats] table sizes", "all", sb.String())
		}
	}
}
