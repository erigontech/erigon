package node

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
)

func prepareBuckets(customBuckets dbutils.BucketsCfg) {
	if len(customBuckets) == 0 {
		return
	}

	currentBuckets := dbutils.DefaultBuckets()

	for k, v := range customBuckets {
		if _, ok := currentBuckets[k]; ok {
			panic(fmt.Errorf("overriding existing buckets is not supported (bucket key=%s)", k))
		}
		currentBuckets[k] = v
	}

	dbutils.UpdateBucketsList(currentBuckets)
}
