package state

import (
	"fmt"
	"testing"
)

// BenchmarkGetStateObjectAfterCodeRead measures the rebuild every account-field
// read falls through to. Cost growing with code size means the bytes are hashed.
func BenchmarkGetStateObjectAfterCodeRead(b *testing.B) {
	for _, codeLen := range []int{32, 1024, 24576} {
		b.Run(fmt.Sprintf("code=%dB", codeLen), func(b *testing.B) {
			ibs, addr := committedCodeIBS(b, codeLen, nil)
			b.ReportAllocs()
			for b.Loop() {
				if _, err := ibs.getStateObject(addr, false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
