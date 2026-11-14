package blob_storage

import (
	"crypto/rand"
	"fmt"
	"sync"
	"testing"

	"github.com/spf13/afero"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

func newBenchColumnStore(b *testing.B, fs afero.Fs, cfg *clparams.BeaconChainConfig) DataColumnStorage {
	b.Helper()
	return NewDataColumnStore(fs, cfg, beaconevents.NewEventEmitter())
}

func benchColumnConfig() *clparams.BeaconChainConfig {
	cfg := *globalBeaconConfig
	cfg.NumberOfColumns = 128
	return &cfg
}

func benchColumnSidecars(b *testing.B, blobs int) []*cltypes.DataColumnSidecar {
	b.Helper()
	sidecars := make([]*cltypes.DataColumnSidecar, 128)
	for i := range sidecars {
		s := createTestDataColumnSidecar(0, int64(i))
		for range blobs {
			var cell cltypes.Cell
			_, _ = rand.Read(cell[:])
			s.Column.Append(&cell)
			s.KzgCommitments.Append(&cltypes.KZGCommitment{2})
			s.KzgProofs.Append(&cltypes.KZGProof{3})
		}
		sidecars[i] = s
	}
	return sidecars
}

func benchRoot(n int) common.Hash {
	var root common.Hash
	_, _ = rand.Read(root[:])
	root[0] = byte(n)
	return root
}

// BenchmarkColumnWriteBlock writes all 128 columns of one block per op, as a supernode or
// a --caplin.blobs-archive node does, onto a real on-disk fs.
func BenchmarkColumnWriteBlock(b *testing.B) {
	for _, blobs := range []int{6, 21} {
		for _, parallel := range []bool{false, true} {
			b.Run(fmt.Sprintf("blobs=%d/parallel=%v", blobs, parallel), func(b *testing.B) {
				fs := afero.NewBasePathFs(afero.NewOsFs(), b.TempDir())
				store := newBenchColumnStore(b, fs, benchColumnConfig())
				sidecars := benchColumnSidecars(b, blobs)
				slot := uint64(subdivisionSlot)
				for i := 0; b.Loop(); i++ {
					root := benchRoot(i)
					slot++
					for _, s := range sidecars {
						s.SignedBlockHeader.Header.Slot = slot
					}
					if !parallel {
						for c, s := range sidecars {
							if err := store.WriteColumnSidecars(b.Context(), root, int64(c), s); err != nil {
								b.Fatal(err)
							}
						}
						continue
					}
					var wg sync.WaitGroup
					for c, s := range sidecars {
						wg.Go(func() {
							if err := store.WriteColumnSidecars(b.Context(), root, int64(c), s); err != nil {
								b.Error(err)
							}
						})
					}
					wg.Wait()
				}
			})
		}
	}
}

// BenchmarkColumnLookups reads back a block that holds custody columns, the way the
// downloader and the recovery path query it.
func BenchmarkColumnLookups(b *testing.B) {
	for _, stored := range []int{8, 128} {
		fs := afero.NewBasePathFs(afero.NewOsFs(), b.TempDir())
		store := newBenchColumnStore(b, fs, benchColumnConfig())
		sidecars := benchColumnSidecars(b, 1)
		const slot = subdivisionSlot + 1
		root := benchRoot(0)
		for c := range stored {
			sidecars[c].SignedBlockHeader.Header.Slot = slot
			if err := store.WriteColumnSidecars(b.Context(), root, int64(c), sidecars[c]); err != nil {
				b.Fatal(err)
			}
		}
		// The first lookup seeds the root's bitmap; the sub-benchmarks measure the steady state,
		// whichever of them runs alone.
		if _, err := store.GetSavedColumnIndex(b.Context(), slot, root); err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("GetSavedColumnIndex/stored=%d", stored), func(b *testing.B) {
			for b.Loop() {
				saved, err := store.GetSavedColumnIndex(b.Context(), slot, root)
				if err != nil || len(saved) != stored {
					b.Fatal(err, len(saved))
				}
			}
		})
		b.Run(fmt.Sprintf("ColumnSidecarExists/stored=%d", stored), func(b *testing.B) {
			for i := 0; b.Loop(); i++ {
				if _, err := store.ColumnSidecarExists(b.Context(), slot, root, int64(i%128)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkColumnWrite writes the 8 custody columns of one block per op, one call each against
// one batch call, onto a real on-disk fs.
func BenchmarkColumnWrite(b *testing.B) {
	const custody = 8
	for _, batch := range []bool{false, true} {
		b.Run(fmt.Sprintf("columns=%d/batch=%v", custody, batch), func(b *testing.B) {
			fs := afero.NewBasePathFs(afero.NewOsFs(), b.TempDir())
			store := newBenchColumnStore(b, fs, benchColumnConfig())
			sidecars := benchColumnSidecars(b, 6)[:custody]
			for _, s := range sidecars {
				s.SignedBlockHeader.Header.Slot = subdivisionSlot + 1
			}
			for i := 0; b.Loop(); i++ {
				root := benchRoot(i)
				if batch {
					if err := store.WriteColumnSidecarsBatch(b.Context(), root, sidecars); err != nil {
						b.Fatal(err)
					}
					continue
				}
				for c, s := range sidecars {
					if err := store.WriteColumnSidecars(b.Context(), root, int64(c), s); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
