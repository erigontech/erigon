package das

import (
	"fmt"
	"testing"

	"github.com/spf13/afero"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
)

func BenchmarkImportColumnsFromBlobs(b *testing.B) {
	cfg := clparams.MainnetBeaconConfig
	initTestBeaconConfig(&cfg)
	for _, n := range []int{6, 21} {
		b.Run(fmt.Sprintf("blobs=%d", n), func(b *testing.B) {
			block, _, blobs, proofs := elBlobsFixture(b, &cfg, n)
			d := newELBlobsTestPeerDas(b, &cfg, &clparams.CaplinConfig{})
			for b.Loop() {
				d.columnStorage = blob_storage.NewDataColumnStore(afero.NewMemMapFs(), &cfg, beaconevents.NewEventEmitter())
				if _, err := d.ImportColumnsFromBlobs(b.Context(), block, blobs, proofs); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
