package solid

import (
	"fmt"
	"testing"
)

func BenchmarkValidatorSetColdHashSSZ(b *testing.B) {
	benchmarkValidatorSetCold(b, 1_000_000, false)
}

func BenchmarkValidatorSetColdHashSSZProgressive(b *testing.B) {
	benchmarkValidatorSetCold(b, 1_000_000, true)
}

func BenchmarkValidatorSetColdThreshold(b *testing.B) {
	for _, count := range []int{1_000, 4_000, 16_000, 64_000} {
		b.Run(fmt.Sprintf("%d", count), func(b *testing.B) {
			benchmarkValidatorSetCold(b, count, false)
		})
	}
}

func BenchmarkValidatorSetWarmDirty(b *testing.B) {
	for _, dirty := range []int{1, 100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("%d", dirty), func(b *testing.B) {
			oldWorkers := validatorRootWorkers
			validatorRootWorkers = newValidatorRootWorkerPool(1)
			b.Cleanup(func() { validatorRootWorkers = oldWorkers })

			validators := testValidatorSet(1_000_000)
			if _, err := validators.HashSSZ(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				b.StopTimer()
				for i := range dirty {
					validator := append(Validator(nil), validators.Get(i)...)
					validator[0] = byte(iteration + i)
					validators.Set(i, validator)
				}
				b.StartTimer()
				if _, err := validators.HashSSZ(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func benchmarkValidatorSetCold(b *testing.B, count int, progressive bool) {
	for _, benchmark := range []struct {
		name       string
		maxWorkers int
	}{
		{name: "serial", maxWorkers: 1},
		{name: "parallel", maxWorkers: validatorRootWorkers.workerCount()},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			oldWorkers := validatorRootWorkers
			workers := newValidatorRootWorkerPool(benchmark.maxWorkers)
			workers.minValidators = 1
			validatorRootWorkers = workers
			b.Cleanup(func() { validatorRootWorkers = oldWorkers })

			validators := testValidatorSet(count)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				validators.MerkleTree = nil
				validators.progressiveTrees = nil
				var err error
				if progressive {
					_, err = validators.HashSSZProgressive()
				} else {
					_, err = validators.HashSSZ()
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
