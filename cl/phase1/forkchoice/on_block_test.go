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

package forkchoice

import "testing"

func TestHasCompleteBlobDataRejectsMissingBlob(t *testing.T) {
	blobs := make([][]byte, 1)
	proofs := [][][]byte{{{1}}}

	if hasCompleteBlobData(blobs, proofs, 1) {
		t.Fatal("missing blob must not report blob data as complete")
	}
}

func TestHasCompleteBlobDataRejectsMissingProofs(t *testing.T) {
	blobs := [][]byte{{1}}
	proofs := make([][][]byte, 1)

	if hasCompleteBlobData(blobs, proofs, 1) {
		t.Fatal("blob data without proofs must not report as complete")
	}
}

func TestHasCompleteBlobDataRejectsEmptyProof(t *testing.T) {
	blobs := [][]byte{{1}}
	proofs := [][][]byte{{nil}}

	if hasCompleteBlobData(blobs, proofs, 1) {
		t.Fatal("empty proof must not report blob data as complete")
	}
}

func TestHasCompleteBlobDataAcceptsCompleteEntries(t *testing.T) {
	blobs := [][]byte{{1}, {2}}
	proofs := [][][]byte{{{3}}, {{4}}}

	if !hasCompleteBlobData(blobs, proofs, 2) {
		t.Fatal("complete blob data must be accepted")
	}
}
