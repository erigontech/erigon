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

package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/snapshotsync"
	dlcomp "github.com/erigontech/erigon/node/components/downloader"
)

func mismatch(name, canonicalHash string) snapshotsync.AdvertisementMismatch {
	return snapshotsync.AdvertisementMismatch{Name: name, OwnHash: strings.Repeat("11", 20), CanonicalHash: canonicalHash}
}

// TestSaltFilesIn pins the salt-file predicate the adoption handler
// uses to abort: salt-<scope>.txt is a salt file, nothing else is.
func TestSaltFilesIn(t *testing.T) {
	t.Parallel()
	adopt := []snapshotsync.AdvertisementMismatch{
		mismatch("v1.0-accounts.0-2048.kv", ""),
		mismatch("salt-state.txt", ""),
		mismatch("v1.0-000000-000500-headers.seg", ""),
		mismatch("salt-blocks.txt", ""),
	}
	got := saltFilesIn(adopt)
	require.ElementsMatch(t, []string{"salt-state.txt", "salt-blocks.txt"}, got)

	require.Empty(t, saltFilesIn([]snapshotsync.AdvertisementMismatch{
		mismatch("v1.0-accounts.0-2048.kv", ""),
	}))
}

// TestCanonicalFilesFromVerdict checks the verdict → fetch-list
// conversion: a well-formed 40-hex canonical hash decodes to a 20-byte
// info-hash; a malformed one is a hard error so no batch is fetched.
func TestCanonicalFilesFromVerdict(t *testing.T) {
	t.Parallel()
	good := strings.Repeat("ab", 20)
	files, err := canonicalFilesFromVerdict(&snapshotsync.MinorityVerdict{
		Adopt: []snapshotsync.AdvertisementMismatch{mismatch("v1.0-accounts.0-2048.kv", good)},
	})
	require.NoError(t, err)
	require.Len(t, files, 1)
	require.Equal(t, "v1.0-accounts.0-2048.kv", files[0].Name)
	require.Equal(t, byte(0xab), files[0].InfoHash[0])

	_, err = canonicalFilesFromVerdict(&snapshotsync.MinorityVerdict{
		Adopt: []snapshotsync.AdvertisementMismatch{mismatch("v1.0-accounts.0-2048.kv", "not-hex")},
	})
	require.Error(t, err)

	_, err = canonicalFilesFromVerdict(&snapshotsync.MinorityVerdict{
		Adopt: []snapshotsync.AdvertisementMismatch{mismatch("v1.0-accounts.0-2048.kv", "abcd")},
	})
	require.Error(t, err, "a hash that is not 20 bytes must be rejected")
}

// TestRunStagedAdoption_Noop: an empty or nil verdict has nothing to
// adopt and must not fetch.
func TestRunStagedAdoption_Noop(t *testing.T) {
	t.Parallel()
	p := &Provider{}

	res, err := p.RunStagedAdoption(context.Background(), AdoptionRequest{Verdict: nil})
	require.NoError(t, err)
	require.Equal(t, AdoptionNoop, res.Outcome)

	res, err = p.RunStagedAdoption(context.Background(), AdoptionRequest{
		Verdict: &snapshotsync.MinorityVerdict{},
	})
	require.NoError(t, err)
	require.Equal(t, AdoptionNoop, res.Outcome)
}

// TestRunStagedAdoption_SaltDivergenceAborts: a salt file in the
// verdict aborts the run before any fetch — proven by a nil Downloader
// that would error if the fetch path were reached.
func TestRunStagedAdoption_SaltDivergenceAborts(t *testing.T) {
	t.Parallel()
	p := &Provider{}
	res, err := p.RunStagedAdoption(context.Background(), AdoptionRequest{
		Policy: snapshotsync.AdoptionAuto,
		Verdict: &snapshotsync.MinorityVerdict{Adopt: []snapshotsync.AdvertisementMismatch{
			mismatch("v1.0-accounts.0-2048.kv", strings.Repeat("ab", 20)),
			mismatch("salt-state.txt", strings.Repeat("cd", 20)),
		}},
	})
	require.NoError(t, err)
	require.Equal(t, AdoptionSaltDivergence, res.Outcome)
	require.Contains(t, res.Reason, "salt-state.txt")
}

// TestRunStagedAdoption_WarnPolicyNoop: policy=warn fetches nothing and
// nothing changes; the operator must trigger adoption explicitly.
func TestRunStagedAdoption_WarnPolicyNoop(t *testing.T) {
	t.Parallel()
	p := &Provider{}
	res, err := p.RunStagedAdoption(context.Background(), AdoptionRequest{
		Policy: snapshotsync.AdoptionWarn,
		Verdict: &snapshotsync.MinorityVerdict{Adopt: []snapshotsync.AdvertisementMismatch{
			mismatch("v1.0-accounts.0-2048.kv", strings.Repeat("ab", 20)),
		}},
	})
	require.NoError(t, err)
	require.Equal(t, AdoptionNoop, res.Outcome)
	require.Contains(t, res.Reason, "warn")
}

// TestRunStagedAdoption_BadHashRejected: a malformed canonical hash is
// caught before the fetch — no batch is requested for a verdict the
// handler cannot turn into info-hashes.
func TestRunStagedAdoption_BadHashRejected(t *testing.T) {
	t.Parallel()
	p := &Provider{}
	_, err := p.RunStagedAdoption(context.Background(), AdoptionRequest{
		Policy: snapshotsync.AdoptionAuto,
		Verdict: &snapshotsync.MinorityVerdict{Adopt: []snapshotsync.AdvertisementMismatch{
			mismatch("v1.0-accounts.0-2048.kv", "not-a-valid-hash"),
		}},
	})
	require.Error(t, err)
}

// TestRunStagedAdoption_NilDownloader: a well-formed verdict under an
// active policy needs a downloader to fetch the canonical batch.
func TestRunStagedAdoption_NilDownloader(t *testing.T) {
	t.Parallel()
	p := &Provider{}
	_, err := p.RunStagedAdoption(context.Background(), AdoptionRequest{
		Policy:     snapshotsync.AdoptionAuto,
		Downloader: nil,
		Verdict: &snapshotsync.MinorityVerdict{Adopt: []snapshotsync.AdvertisementMismatch{
			mismatch("v1.0-accounts.0-2048.kv", strings.Repeat("ab", 20)),
		}},
	})
	require.ErrorContains(t, err, "downloader")
}

// TestAdoptionOutcome_String pins the operator-facing names.
func TestAdoptionOutcome_String(t *testing.T) {
	t.Parallel()
	require.Equal(t, "noop", AdoptionNoop.String())
	require.Equal(t, "salt-divergence", AdoptionSaltDivergence.String())
	require.Equal(t, "staged", AdoptionStaged.String())
	require.Equal(t, "cut-over", AdoptionCutOver.String())
}

// TestCutoverStagedBatch_RequiresAggregator: the running-node cutover
// needs the state Aggregator (for its commit barrier) and the block
// snapshots — a Provider without them must refuse rather than swap
// files with no reader barrier.
func TestCutoverStagedBatch_RequiresAggregator(t *testing.T) {
	t.Parallel()
	p := &Provider{}
	err := p.cutoverStagedBatch(&dlcomp.StagedBatch{Dir: t.TempDir()}, nil)
	require.ErrorContains(t, err, "Aggregator")
}
