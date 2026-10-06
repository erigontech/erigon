package version

import (
	"fmt"
	"testing"
)

func BenchmarkMatchVersionedFile(b *testing.B) {
	// Simulate a large directory with thousands of snapshot files (realistic scenario)
	dirEntries := make([]string, 0, 2000)
	for i := range 500 {
		dirEntries = append(dirEntries,
			fmt.Sprintf("v1.0-accounts.%d-%d.kv", i, i+1),
			fmt.Sprintf("v1.0-storage.%d-%d.kv", i, i+1),
			fmt.Sprintf("v1.0-accounts.%d-%d.kvi", i, i+1),
			fmt.Sprintf("v1.0-storage.%d-%d.kvi", i, i+1),
		)
	}

	for b.Loop() {
		_, _, _, _ = MatchVersionedFile("*-accounts.0-1.kvi", dirEntries, "/tmp")
	}
}
