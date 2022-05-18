//go:build integration

package enode

import "testing"

// This test checks fairness of FairMix in the happy case where all sources return nodes
// within the context's deadline.
func TestFairMix(t *testing.T) {
	for i := 0; i < 500; i++ {
		testMixerFairness(t)
	}
}
