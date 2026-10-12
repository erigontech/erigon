// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package commitmentflags

import (
	"reflect"
	"testing"

	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
)

func TestRestore(t *testing.T) {
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousParallel := statecfg.ExperimentalParallelCommitment
	previousHash := statecfg.BinCommitmentHash
	previousSchema := statecfg.Schema
	previousSuite := commitment.PBinHashSuiteName()

	t.Run("restores all commitment flags", func(t *testing.T) {
		Restore(t)
		statecfg.ExperimentalBinCommitment = !previousBin
		statecfg.ExperimentalHexBinCommitment = !previousHexBin
		statecfg.ExperimentalCommitmentV3 = !previousV3
		statecfg.ExperimentalParallelCommitment = !previousParallel
		statecfg.BinCommitmentHash = previousHash + "changed"
		statecfg.Schema = statecfg.SchemaGen{}
		suite := "blake3"
		if previousSuite == suite {
			suite = "keccak"
		}
		if err := commitment.SetPBinHashSuite(suite); err != nil {
			t.Fatal(err)
		}
	})

	if statecfg.ExperimentalBinCommitment != previousBin {
		t.Fatalf("binary commitment flag was not restored")
	}
	if statecfg.ExperimentalHexBinCommitment != previousHexBin {
		t.Fatalf("hex+bin commitment flag was not restored")
	}
	if statecfg.ExperimentalCommitmentV3 != previousV3 {
		t.Fatalf("v3 commitment flag was not restored")
	}
	if statecfg.ExperimentalParallelCommitment != previousParallel {
		t.Fatalf("parallel commitment flag was not restored")
	}
	if statecfg.BinCommitmentHash != previousHash {
		t.Fatalf("binary commitment hash was not restored")
	}
	restoredSchema := statecfg.Schema
	previousSchema.CommitmentDomain.KVWriteVersion = nil
	restoredSchema.CommitmentDomain.KVWriteVersion = nil
	if !reflect.DeepEqual(restoredSchema, previousSchema) {
		t.Fatalf("commitment schema was not restored")
	}
	if commitment.PBinHashSuiteName() != previousSuite {
		t.Fatalf("binary commitment hash suite was not restored")
	}
}
