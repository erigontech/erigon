package cltypes

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes/solid"
)

func TestExecutionPayloadBidCopyCloneProgressiveCache(t *testing.T) {
	for _, mode := range []string{"progressive", "legacy", "unconfigured"} {
		for _, warm := range []bool{false, true} {
			for _, clone := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/warm=%t/clone=%t", mode, warm, clone), func(t *testing.T) {
					newSource := func() *ExecutionPayloadBid {
						source := &ExecutionPayloadBid{}
						if mode == "unconfigured" {
							return source
						}
						if mode == "legacy" {
							source.BlobKzgCommitments = *solid.NewStaticListSSZ[*KZGCommitment](16, 48)
						} else {
							source.BlobKzgCommitments.EnsureStaticProgressive(16, 48)
						}
						source.BlobKzgCommitments.Append(&KZGCommitment{1})
						return source
					}
					source := newSource()
					want, err := newSource().HashSSZ()
					require.NoError(t, err)
					if warm {
						got, err := source.HashSSZ()
						require.NoError(t, err)
						require.Equal(t, want, got)
					}
					var target *ExecutionPayloadBid
					if clone {
						target = source.Clone().(*ExecutionPayloadBid)
						require.Zero(t, target.BlobKzgCommitments.Len())
						want, err = (&ExecutionPayloadBid{}).HashSSZ()
						require.NoError(t, err)
					} else {
						target = source.Copy()
					}
					if warm && !clone && mode != "unconfigured" {
						// AllocsPerRun's warm-up call would fill an empty holder, so hash a fresh copy each run.
						copies := make([]*ExecutionPayloadBid, 101)
						for i := range copies {
							copies[i] = source.Copy()
						}
						next := 0
						allocs := testing.AllocsPerRun(100, func() {
							_, err = copies[next].BlobKzgCommitments.HashSSZProgressive(nil)
							next++
						})
						require.NoError(t, err)
						require.Zero(t, allocs, "a warm copy must keep the cached root")
					}
					for range 3 {
						got, err := target.HashSSZ()
						require.NoError(t, err)
						require.Equal(t, want, got)
					}
					allocs := testing.AllocsPerRun(100, func() {
						_, err = target.BlobKzgCommitments.HashSSZProgressive(nil)
					})
					require.NoError(t, err)
					require.Zero(t, allocs, "unchanged commitments must reuse their root")
				})
			}
		}
	}
}

func TestExecutionPayloadBidCopyCloneConcurrentHash(t *testing.T) {
	source := executionPayloadBidWithCommitments(3)
	wantCopy, err := source.HashSSZ()
	require.NoError(t, err)
	wantClone, err := (&ExecutionPayloadBid{}).HashSSZ()
	require.NoError(t, err)
	for _, clone := range []bool{false, true} {
		target := source.Copy()
		want := wantCopy
		if clone {
			target = source.Clone().(*ExecutionPayloadBid)
			want = wantClone
		}
		var wg sync.WaitGroup
		for range 16 {
			wg.Go(func() {
				for range 10 {
					got, err := target.HashSSZ()
					assert.NoError(t, err)
					assert.Equal(t, want, got)
				}
			})
		}
		wg.Wait()
	}
}

func TestExecutionPayloadBidCopyCacheInvalidation(t *testing.T) {
	source := executionPayloadBidWithCommitments(1)
	originalRoot, err := source.HashSSZ()
	require.NoError(t, err)
	target := source.Copy()
	got, err := target.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, originalRoot, got)
	target.BlobKzgCommitments.Set(0, &KZGCommitment{1})
	changedRoot, err := target.HashSSZ()
	require.NoError(t, err)
	require.NotEqual(t, originalRoot, changedRoot)
	got, err = source.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, originalRoot, got)
	source.BlobKzgCommitments.Append(&KZGCommitment{2})
	got, err = target.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, changedRoot, got)
}
