package kv

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/metrics"
)

var (
	collectTableSizesPeriodically = dbg.EnvBool("COLLECT_TABLE_SIZES", true)
	collectTableSizesFrequency    = dbg.EnvDuration("COLLECT_TABLE_SIZES_FREQUENCY", 5*time.Minute)
	dbTableSizeBytes              = metrics.GetOrCreateGaugeVec("db_table_size_bytes", []string{"db", "table"})
)

type TableSize struct {
	Name string
	Size uint64
}

func CollectTableSizes(ctx context.Context, db RoDB) ([]TableSize, error) {
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
	err = db.View(ctx, func(tx Tx) error {
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

func CollectTableSizesPeriodically(ctx context.Context, db TemporalRoDB, label Label, logger log.Logger) {
	if !collectTableSizesPeriodically {
		return
	}

	debugLogging := logger.Enabled(ctx, log.LvlDebug)
	ticker := time.NewTicker(collectTableSizesFrequency)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tableSizes, err := CollectTableSizes(ctx, db)
			if err != nil {
				logger.Error("[kv] failed to collect table sizes", "err", err)
				continue
			}

			var sb strings.Builder
			for _, t := range tableSizes {
				dbTableSizeBytes.WithLabelValues(string(label), t.Name).Set(float64(t.Size))
				if t.Size == 0 || !debugLogging {
					continue
				}

				sb.WriteString(t.Name)
				sb.WriteRune(':')
				sb.WriteString(common.ByteCount(t.Size))
				sb.WriteRune(',')
			}

			logger.Debug("[kv] table sizes", "all", sb.String())
		}
	}
}
