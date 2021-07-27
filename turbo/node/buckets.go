package node

import (
	"fmt"

	"github.com/ledgerwatch/erigon/ethdb/kv"
)

//nolint
func prepareBuckets(customBuckets kv.BucketsCfg) {
	if len(customBuckets) == 0 {
		return
	}

	currentBuckets := kv.DefaultBuckets()

	for k, v := range customBuckets {
		if _, ok := currentBuckets[k]; ok {
			panic(fmt.Errorf("overriding existing buckets is not supported (bucket key=%s)", k))
		}
		currentBuckets[k] = v
	}

	kv.UpdateBucketsList(currentBuckets)
}
