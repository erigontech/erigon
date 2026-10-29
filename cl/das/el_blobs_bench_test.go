package das

import (
	"fmt"
	"testing"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/spf13/afero"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	peerdasutils "github.com/erigontech/erigon/cl/das/utils"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/common/crypto/kzg"
)

func BenchmarkImportColumnsFromBlobs(b *testing.B) {
	cfg := clparams.MainnetBeaconConfig
	initTestBeaconConfig(&cfg)
	for _, n := range []int{6, 21} {
		b.Run(fmt.Sprintf("blobs=%d", n), func(b *testing.B) {
			block := cltypes.NewSignedBeaconBlock(&cfg, clparams.FuluVersion)
			block.Block.Slot = 100
			blobs := make([][]byte, n)
			proofs := make([][][]byte, n)
			for i := range blobs {
				blob := &goethkzg.Blob{}
				for j := 0; j < len(blob); j += 32 {
					blob[j+31], blob[j+30] = byte(i), byte(j)
				}
				commitment, err := kzg.Ctx().BlobToKZGCommitment(blob, 0)
				if err != nil {
					b.Fatal(err)
				}
				block.GetBlobKzgCommitments().Append((*cltypes.KZGCommitment)(&commitment))
				_, cellProofs, err := peerdasutils.ComputeCellsAndKZGProofs(blob[:])
				if err != nil {
					b.Fatal(err)
				}
				blobs[i] = blob[:]
				for j := range cellProofs {
					proofs[i] = append(proofs[i], cellProofs[j][:])
				}
			}
			if _, err := block.Block.HashSSZ(); err != nil {
				b.Fatal(err)
			}
			d := newELBlobsTestPeerDas(b, &cfg, &clparams.CaplinConfig{})
			for b.Loop() {
				d.columnStorage = blob_storage.NewDataColumnStore(afero.NewMemMapFs(), &cfg, beaconevents.NewEventEmitter())
				if err := d.ImportColumnsFromBlobs(b.Context(), block, blobs, proofs); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
