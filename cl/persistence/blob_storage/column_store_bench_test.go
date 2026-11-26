package blob_storage

import (
	"crypto/rand"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/spf13/afero"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

func newBenchColumnStore(b *testing.B, cfg *clparams.BeaconChainConfig) (DataColumnStorage, *countingFs) {
	b.Helper()
	fs := newCountingFs(afero.NewBasePathFs(afero.NewOsFs(), b.TempDir()))
	return NewDataColumnStore(fs, cfg, beaconevents.NewEventEmitter()), fs
}

func reportFsyncsPerOp(b *testing.B, fs *countingFs) {
	b.ReportMetric(float64(fs.count(opSync))/float64(b.N), "fsyncs/op")
}

func writeBenchRoot(b *testing.B, store DataColumnStorage, root common.Hash, sidecars []*cltypes.DataColumnSidecar, batch bool) {
	b.Helper()
	if batch {
		if err := store.WriteColumnSidecarsBatch(b.Context(), root, sidecars); err != nil {
			b.Fatal(err)
		}
		return
	}
	for c, s := range sidecars {
		if err := store.WriteColumnSidecars(b.Context(), root, int64(c), s); err != nil {
			b.Fatal(err)
		}
	}
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

func benchColumnSidecarsAt(b *testing.B, blobs int, slot uint64) []*cltypes.DataColumnSidecar {
	b.Helper()
	sidecars := benchColumnSidecars(b, blobs)
	for _, s := range sidecars {
		s.SignedBlockHeader.Header.Slot = slot
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
		for _, mode := range []string{"parallel=false", "parallel=true", "batch=true"} {
			b.Run(fmt.Sprintf("blobs=%d/%s", blobs, mode), func(b *testing.B) {
				store, fs := newBenchColumnStore(b, benchColumnConfig())
				sidecars := benchColumnSidecars(b, blobs)
				slot := uint64(subdivisionSlot)
				fs.reset()
				for i := 0; b.Loop(); i++ {
					root := benchRoot(i)
					slot++
					for _, s := range sidecars {
						s.SignedBlockHeader.Header.Slot = slot
					}
					switch mode {
					case "parallel=false":
						for c, s := range sidecars {
							if err := store.WriteColumnSidecars(b.Context(), root, int64(c), s); err != nil {
								b.Fatal(err)
							}
						}
						continue
					case "batch=true":
						if err := store.WriteColumnSidecarsBatch(b.Context(), root, sidecars); err != nil {
							b.Fatal(err)
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
				reportFsyncsPerOp(b, fs)
			})
		}
	}
}

// BenchmarkColumnLookups reads back a block that holds custody columns, the way the
// downloader and the recovery path query it. written names how the root was stored, not the
// layout, so runs of different layouts stay comparable.
func BenchmarkColumnLookups(b *testing.B) {
	for _, written := range []string{"single", "batch"} {
		for _, stored := range []int{8, 128} {
			b.Run(fmt.Sprintf("written=%s/stored=%d", written, stored), func(b *testing.B) {
				store, fs := newBenchColumnStore(b, benchColumnConfig())
				const slot = subdivisionSlot + 1
				root := benchRoot(0)
				writeBenchRoot(b, store, root, benchColumnSidecarsAt(b, 1, slot)[:stored], written == "batch")
				// The first lookup seeds the root's bitmap; the warm cases measure the steady state,
				// whichever of them runs alone.
				if _, err := store.GetSavedColumnIndex(b.Context(), slot, root); err != nil {
					b.Fatal(err)
				}
				b.Run("GetSavedColumnIndex", func(b *testing.B) {
					for b.Loop() {
						saved, err := store.GetSavedColumnIndex(b.Context(), slot, root)
						if err != nil || len(saved) != stored {
							b.Fatal(err, len(saved))
						}
					}
				})
				b.Run("GetSavedColumnIndex/cold", func(b *testing.B) {
					for b.Loop() {
						cold := NewDataColumnStore(fs, benchColumnConfig(), beaconevents.NewEventEmitter())
						saved, err := cold.GetSavedColumnIndex(b.Context(), slot, root)
						if err != nil || len(saved) != stored {
							b.Fatal(err, len(saved))
						}
					}
				})
				b.Run("ColumnSidecarExists", func(b *testing.B) {
					for i := 0; b.Loop(); i++ {
						if _, err := store.ColumnSidecarExists(b.Context(), slot, root, int64(i%128)); err != nil {
							b.Fatal(err)
						}
					}
				})
			})
		}
	}
}

// BenchmarkColumnWriteStream serves every stored column of a block to a discarded writer, the
// way a peer's column request is answered.
func BenchmarkColumnWriteStream(b *testing.B) {
	for _, written := range []string{"single", "batch"} {
		for _, stored := range []int{8, 128} {
			b.Run(fmt.Sprintf("written=%s/stored=%d", written, stored), func(b *testing.B) {
				store, _ := newBenchColumnStore(b, benchColumnConfig())
				const slot = subdivisionSlot + 1
				root := benchRoot(0)
				writeBenchRoot(b, store, root, benchColumnSidecarsAt(b, 1, slot)[:stored], written == "batch")
				for b.Loop() {
					for c := range stored {
						if err := store.WriteStream(io.Discard, slot, root, uint64(c)); err != nil {
							b.Fatal(err)
						}
					}
				}
			})
		}
	}
}

// BenchmarkColumnWrite writes the 8 custody columns of one block per op, one call each against
// one batch call, onto a real on-disk fs.
func BenchmarkColumnWrite(b *testing.B) {
	const custody = 8
	for _, batch := range []bool{false, true} {
		b.Run(fmt.Sprintf("columns=%d/batch=%v", custody, batch), func(b *testing.B) {
			store, fs := newBenchColumnStore(b, benchColumnConfig())
			sidecars := benchColumnSidecarsAt(b, 6, subdivisionSlot+1)[:custody]
			fs.reset()
			for i := 0; b.Loop(); i++ {
				writeBenchRoot(b, store, benchRoot(i), sidecars, batch)
			}
			reportFsyncsPerOp(b, fs)
		})
	}
}
