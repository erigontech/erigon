package verkletrie

import "github.com/ledgerwatch/erigon-lib/kv"

const (
	VerkleRoots = "VerkleRoots"
	VerkleTrie  = "VerkleTrie"
)

var ExtraBuckets = []string{
	VerkleTrie,
	VerkleRoots,
}

func InitDB(tx kv.RwTx) error {
	for _, b := range ExtraBuckets {
		if err := tx.CreateBucket(b); err != nil {
			return err
		}
	}
	return nil
}
