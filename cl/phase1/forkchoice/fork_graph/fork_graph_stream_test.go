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

package fork_graph

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/golang/snappy"
	dto "github.com/prometheus/client_model/go"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/diagnostics/metrics"
)

var streamTestRoot = common.Hash{0xab}

type namedState struct {
	name  string
	state *state.CachingBeaconState
}

func copyState(t *testing.T, st *state.CachingBeaconState) *state.CachingBeaconState {
	t.Helper()
	out, err := st.Copy()
	require.NoError(t, err)
	return out
}

// streamTestStates returns one state per fork, reaching forks without a fixture through the upgrade chain.
func streamTestStates(t *testing.T) []namedState {
	t.Helper()
	_, phase0, _ := tests.GetPhase0Random()
	altair := copyState(t, phase0)
	require.NoError(t, altair.UpgradeToAltair())
	bellatrix := copyState(t, altair)
	require.NoError(t, bellatrix.UpgradeToBellatrix())
	_, capella, _ := tests.GetCapellaRandom()
	deneb := copyState(t, capella)
	require.NoError(t, deneb.UpgradeToDeneb())
	_, electra, _ := tests.GetElectraRandom()
	fulu := copyState(t, electra)
	require.NoError(t, fulu.UpgradeToFulu())
	gloas := copyState(t, fulu)
	require.NoError(t, gloas.UpgradeToGloas())
	fixture := tests.GetGloasFixture("empty_epoch_transition", false, 0)
	return []namedState{
		{"phase0", phase0},
		{"altair", altair},
		{"bellatrix", bellatrix},
		{"capella", capella},
		{"deneb", deneb},
		{"electra", electra},
		{"fulu", fulu},
		{"gloas upgraded", gloas},
		{"gloas fixture", fixture.PreState},
	}
}

func newStreamGraph(t testing.TB, anchor *state.CachingBeaconState, fs afero.Fs) *forkGraphDisk {
	t.Helper()
	graph, err := NewForkGraphDisk(anchor, nil, fs, beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)
	return graph.(*forkGraphDisk)
}

// readStateFile returns the snappy-decoded file: version, big-endian length, ssz body and the optional state root.
func readStateFile(t testing.TB, fs afero.Fs, root common.Hash) []byte {
	t.Helper()
	file, err := fs.Open(getBeaconStateFilename(root))
	require.NoError(t, err)
	defer file.Close()
	raw, err := io.ReadAll(snappy.NewReader(file))
	require.NoError(t, err)
	return raw
}

func writeStateFile(t testing.TB, fs afero.Fs, root common.Hash, raw []byte) {
	t.Helper()
	file, err := fs.Create(getBeaconStateFilename(root))
	require.NoError(t, err)
	sw := snappy.NewBufferedWriter(file)
	_, err = sw.Write(raw)
	require.NoError(t, err)
	require.NoError(t, sw.Close())
	require.NoError(t, file.Close())
}

func requireSameState(t *testing.T, want, got *state.CachingBeaconState) {
	t.Helper()
	wantRoot, err := want.HashSSZ()
	require.NoError(t, err)
	gotRoot, err := got.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, wantRoot, gotRoot)
	wantSSZ, err := want.EncodeSSZ(nil)
	require.NoError(t, err)
	gotSSZ, err := got.EncodeSSZ(nil)
	require.NoError(t, err)
	require.True(t, bytes.Equal(wantSSZ, gotSSZ), "re-encoded state differs")
}

func TestDumpedStateFileIsByteIdenticalToEncodeSSZ(t *testing.T) {
	for _, tc := range streamTestStates(t) {
		t.Run(tc.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			graph := newStreamGraph(t, tc.state, fs)
			require.NoError(t, graph.DumpBeaconStateOnDisk(streamTestRoot, tc.state, true))

			raw := readStateFile(t, fs, streamTestRoot)
			body, err := tc.state.EncodeSSZ(nil)
			require.NoError(t, err)
			require.Equal(t, byte(tc.state.Version()), raw[0])
			require.Equal(t, uint64(len(body)), binary.BigEndian.Uint64(raw[1:9]))
			require.True(t, bytes.Equal(body, raw[9:9+len(body)]), "ssz body differs from EncodeSSZ")
			require.Len(t, raw, 9+len(body)+32)

			got, err := graph.readBeaconStateFromDisk(streamTestRoot)
			require.NoError(t, err)
			requireSameState(t, tc.state, got)
		})
	}
}

func TestDumpedStateRootBranches(t *testing.T) {
	prevRoot, headerRoot := common.Hash{0x01}, common.Hash{0x02}
	readPrevRoot := func(t *testing.T, graph *forkGraphDisk) common.Hash {
		t.Helper()
		_, prev, err := graph.decodeBeaconStateFile(streamTestRoot)
		require.NoError(t, err)
		require.NotNil(t, prev)
		return *prev
	}
	fixture := tests.GetGloasFixture("empty_epoch_transition", false, 0).PreState

	t.Run("header known", func(t *testing.T) {
		st := copyState(t, fixture)
		graph := newStreamGraph(t, st, afero.NewMemMapFs())
		st.SetPreviousStateRoot(prevRoot)
		graph.headers.Store(streamTestRoot, &cltypes.BeaconBlockHeader{Slot: st.Slot(), Root: headerRoot})
		require.NoError(t, graph.DumpBeaconStateOnDisk(streamTestRoot, st, true))
		require.Equal(t, headerRoot, readPrevRoot(t, graph))
	})
	t.Run("header unknown", func(t *testing.T) {
		st := copyState(t, fixture)
		graph := newStreamGraph(t, st, afero.NewMemMapFs())
		st.SetPreviousStateRoot(prevRoot)
		require.NoError(t, graph.DumpBeaconStateOnDisk(streamTestRoot, st, true))
		require.Equal(t, prevRoot, readPrevRoot(t, graph))
	})
	t.Run("gloas skipped slot", func(t *testing.T) {
		st := copyState(t, fixture)
		graph := newStreamGraph(t, st, afero.NewMemMapFs())
		st.SetPreviousStateRoot(prevRoot)
		require.NoError(t, st.SetSlot(st.LatestBlockHeader().Slot+1))
		graph.headers.Store(streamTestRoot, &cltypes.BeaconBlockHeader{Slot: st.Slot(), Root: headerRoot})
		require.NoError(t, graph.DumpBeaconStateOnDisk(streamTestRoot, st, true))
		require.Equal(t, prevRoot, readPrevRoot(t, graph))
	})
}

func TestDumpedStateFileReplacesPreviousFile(t *testing.T) {
	fs := afero.NewMemMapFs()
	first := shareTestState(t)
	graph := newStreamGraph(t, first, fs)
	require.NoError(t, graph.DumpBeaconStateOnDisk(streamTestRoot, first, true))
	second := aheadOf(t, first)
	require.NoError(t, second.SetSlot(first.Slot()+1))
	require.NoError(t, graph.DumpBeaconStateOnDisk(streamTestRoot, second, true))

	got, err := graph.readBeaconStateFromDisk(streamTestRoot)
	require.NoError(t, err)
	requireSameState(t, second, got)
	exists, err := afero.Exists(fs, getBeaconStateTempFilename(streamTestRoot))
	require.NoError(t, err)
	require.False(t, exists)
}

var errInjected = errors.New("injected fault")

// faultFs fails the k-th write to a temporary state file, or its close or rename.
type faultFs struct {
	afero.Fs
	failWrite  int
	shortWrite bool
	closeErr   error
	renameErr  error
	writes     int
}

func (f *faultFs) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	file, err := f.Fs.OpenFile(name, flag, perm)
	if err != nil || !strings.HasSuffix(name, ".tmp") {
		return file, err
	}
	return &faultFile{File: file, fs: f}, nil
}

func (f *faultFs) Rename(oldName, newName string) error {
	if f.renameErr != nil {
		return f.renameErr
	}
	return f.Fs.Rename(oldName, newName)
}

type faultFile struct {
	afero.File
	fs *faultFs
}

func (f *faultFile) Write(p []byte) (int, error) {
	f.fs.writes++
	if f.fs.writes != f.fs.failWrite {
		return f.File.Write(p)
	}
	if f.fs.shortWrite {
		return f.File.Write(p[:len(p)-1])
	}
	return 0, errInjected
}

func (f *faultFile) Close() error {
	err := f.File.Close()
	if f.fs.closeErr != nil {
		return f.fs.closeErr
	}
	return err
}

func TestDumpFailureKeepsPreviousFile(t *testing.T) {
	first := shareTestState(t)
	second := aheadOf(t, first)
	require.NoError(t, second.SetSlot(first.Slot()+1))

	countWrites := &faultFs{Fs: afero.NewMemMapFs()}
	graph := newStreamGraph(t, first, countWrites)
	countWrites.writes = 0
	require.NoError(t, graph.DumpBeaconStateOnDisk(streamTestRoot, second, true))
	totalWrites := countWrites.writes
	require.Greater(t, totalWrites, 3)

	type fault struct {
		name string
		set  func(*faultFs)
		want error
	}
	var faults []fault
	for _, k := range []int{1, 2, 3, totalWrites / 2, totalWrites - 1, totalWrites} {
		faults = append(faults,
			fault{"write error", func(f *faultFs) { f.failWrite = k }, errInjected},
			fault{"short write", func(f *faultFs) { f.failWrite, f.shortWrite = k, true }, io.ErrShortWrite},
		)
	}
	faults = append(faults,
		fault{"close error", func(f *faultFs) { f.closeErr = errInjected }, errInjected},
		fault{"rename error", func(f *faultFs) { f.renameErr = errInjected }, errInjected},
	)

	for _, tc := range faults {
		t.Run(tc.name, func(t *testing.T) {
			fs := &faultFs{Fs: afero.NewMemMapFs()}
			graph := newStreamGraph(t, first, fs)
			require.NoError(t, graph.DumpBeaconStateOnDisk(streamTestRoot, first, true))
			fs.writes = 0
			tc.set(fs)

			err := graph.DumpBeaconStateOnDisk(streamTestRoot, second, true)

			require.ErrorIs(t, err, tc.want)
			require.Equal(t, common.Hash{}, graph.lastDumpedRoot)
			exists, existsErr := afero.Exists(fs, getBeaconStateTempFilename(streamTestRoot))
			require.NoError(t, existsErr)
			require.False(t, exists, "temp file left behind")
			got, readErr := graph.readBeaconStateFromDisk(streamTestRoot)
			require.NoError(t, readErr)
			requireSameState(t, first, got)

			*fs = faultFs{Fs: fs.Fs}
			require.NoError(t, graph.DumpBeaconStateOnDisk(streamTestRoot, second, true))
			require.Equal(t, streamTestRoot, graph.lastDumpedRoot)
			got, readErr = graph.readBeaconStateFromDisk(streamTestRoot)
			require.NoError(t, readErr)
			requireSameState(t, second, got)
		})
	}
}

func TestPruneRemovesStateTempFile(t *testing.T) {
	f := &forkGraphDisk{fs: afero.NewMemMapFs(), beaconCfg: &clparams.MainnetBeaconConfig}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	block.Block.Slot = 100
	f.blocks.Store(streamTestRoot, block)
	newer := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	newer.Block.Slot = 200
	f.blocks.Store(common.Hash{2}, newer)
	require.NoError(t, afero.WriteFile(f.fs, getBeaconStateFilename(common.Hash{2}), []byte{1}, 0o644))
	require.NoError(t, afero.WriteFile(f.fs, getBeaconStateFilename(streamTestRoot), []byte{1}, 0o644))
	require.NoError(t, afero.WriteFile(f.fs, getBeaconStateTempFilename(streamTestRoot), []byte{1}, 0o644))

	require.NoError(t, f.Prune(150))

	for _, name := range []string{getBeaconStateFilename(streamTestRoot), getBeaconStateTempFilename(streamTestRoot)} {
		exists, err := afero.Exists(f.fs, name)
		require.NoError(t, err)
		require.False(t, exists, name)
	}
}

// streamCorruptionState builds a Fulu state with n validators under cfg.
func streamCorruptionState(t testing.TB, cfg *clparams.BeaconChainConfig, n int) *state.CachingBeaconState {
	t.Helper()
	st := state.New(cfg)
	st.SetVersion(clparams.FuluVersion)
	st.SetLatestExecutionPayloadHeader(cltypes.NewEth1Header(clparams.FuluVersion))
	limit := int(cfg.ValidatorRegistryLimit)
	validators := solid.NewValidatorSet(limit)
	balances := make([]uint64, n)
	for i := range n {
		validators.Append(shareTestValidator(uint64(i)))
		balances[i] = 32_000_000_000
	}
	st.SetValidators(validators)
	st.SetBalances(solid.NewUint64ListSSZFromSlice(limit, balances))
	st.SetInactivityScores(make([]uint64, n))
	st.SetPreviousEpochParticipation(solid.NewParticipationBitList(n, limit))
	st.SetCurrentEpochParticipation(solid.NewParticipationBitList(n, limit))
	require.NoError(t, st.InitBeaconState())
	return st
}

func TestCorruptStateFileIsRejected(t *testing.T) {
	anchor := shareTestState(t)
	validFs := afero.NewMemMapFs()
	validGraph := newStreamGraph(t, anchor, validFs)
	valid := readStateFile(t, validFs, validGraph.anchorRoot)
	bodyLen := len(valid) - 9 - 32
	firstOffsetPos := 9 + 8 + 32 + 8 + 16 + 112 + 2*8192*32

	withLength := func(raw []byte, length uint64) []byte {
		out := append([]byte(nil), raw...)
		binary.BigEndian.PutUint64(out[1:9], length)
		return out
	}
	withFirstOffset := func(delta uint32) []byte {
		out := append([]byte(nil), valid...)
		binary.LittleEndian.PutUint32(out[firstOffsetPos:], binary.LittleEndian.Uint32(out[firstOffsetPos:])+delta)
		return out
	}
	trailing := append(withLength(valid[:9+bodyLen], uint64(bodyLen+3)), 1, 2, 3)

	cases := map[string][]byte{
		"truncated body":              valid[:9+bodyLen-10],
		"missing header":              valid[:1],
		"truncated length":            valid[:5],
		"empty file":                  nil,
		"length above max":            withLength(valid[:9+100], maxSSZObjectSize+1),
		"length below fixed part":     withLength(valid[:9+100], 100),
		"first offset past fixed":     withFirstOffset(1),
		"trailing bytes in length":    trailing,
		"length shorter than content": withLength(valid, uint64(bodyLen-1)),
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			graph := newStreamGraph(t, anchor, fs)
			writeStateFile(t, fs, graph.anchorRoot, raw)

			_, err := graph.readBeaconStateFromDisk(graph.anchorRoot)
			require.Error(t, err)
			replayed, err := graph.ReplayState(graph.anchorRoot)
			require.NoError(t, err)
			require.Nil(t, replayed)
		})
	}
}

func TestStateFileWithTooManyValidatorsIsRejected(t *testing.T) {
	big := streamCorruptionState(t, &clparams.MainnetBeaconConfig, 1100)
	bigFs := afero.NewMemMapFs()
	bigGraph := newStreamGraph(t, big, bigFs)
	raw := readStateFile(t, bigFs, bigGraph.anchorRoot)

	cfg := clparams.MainnetBeaconConfig
	cfg.ValidatorRegistryLimit = 1000
	small := streamCorruptionState(t, &cfg, 10)
	fs := afero.NewMemMapFs()
	graph := newStreamGraph(t, small, fs)
	writeStateFile(t, fs, graph.anchorRoot, raw)

	_, err := graph.readBeaconStateFromDisk(graph.anchorRoot)
	require.ErrorContains(t, err, "exceeds limit")
	replayed, err := graph.ReplayState(graph.anchorRoot)
	require.NoError(t, err)
	require.Nil(t, replayed)
}

func TestDumpDoesNotBufferTheWholeState(t *testing.T) {
	st := streamCorruptionState(t, &clparams.MainnetBeaconConfig, 300_000)
	graph := newStreamGraph(t, st, afero.NewBasePathFs(afero.NewOsFs(), t.TempDir()))
	size := st.EncodingSizeSSZ()

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	require.NoError(t, graph.DumpBeaconStateOnDisk(streamTestRoot, st, true))
	runtime.ReadMemStats(&after)

	require.Less(t, int(after.TotalAlloc-before.TotalAlloc), size/4)
}

func TestDumpBeaconStateWriterPanicLeavesNoTempFile(t *testing.T) {
	anchor := shareTestState(t)
	baseFs := afero.NewMemMapFs()
	f := &forkGraphDisk{
		fs:        envelopeWriteFailureFs{Fs: baseFs, stage: "panic"},
		beaconCfg: &clparams.MainnetBeaconConfig,
	}

	require.PanicsWithValue(t, errTestEnvelopeIO, func() {
		_ = f.DumpBeaconStateOnDisk(streamTestRoot, anchor, true)
	})

	exists, err := afero.Exists(baseFs, getBeaconStateTempFilename(streamTestRoot))
	require.NoError(t, err)
	require.False(t, exists, "temp file left behind after a writer panic")
}

func TestDumpBeaconStateObservesTheEncodedSize(t *testing.T) {
	anchor := shareTestState(t)
	graph := shareTestGraph(t, anchor, nil)
	size := metrics.GetOrCreateHistogram("encode_ssz_beacon_state_size")
	read := func() (count uint64, sum float64) {
		metric := &dto.Metric{}
		require.NoError(t, size.Write(metric))
		return metric.GetHistogram().GetSampleCount(), metric.GetHistogram().GetSampleSum()
	}
	countBefore, sumBefore := read()

	require.NoError(t, graph.DumpBeaconStateOnDisk(streamTestRoot, anchor, true))

	countAfter, sumAfter := read()
	require.Equal(t, uint64(1), countAfter-countBefore)
	require.Equal(t, float64(anchor.EncodingSizeSSZ()), sumAfter-sumBefore)
}
