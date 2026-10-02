package commitmentdb

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/erigontech/erigon/internal/commitmenttest"
	"github.com/stretchr/testify/require"
)

func Test_EncodeCommitmentState(t *testing.T) {
	t.Parallel()
	cs := commitmentState{
		txNum:     rand.Uint64(),
		trieState: make([]byte, 1024),
	}
	commitmenttest.Read(t, cs.trieState, rand.Read)

	buf, err := cs.Encode()
	require.NoError(t, err)
	require.NotEmpty(t, buf)

	var dec commitmentState
	err = dec.Decode(buf)
	require.NoError(t, err)
	require.Equal(t, cs.txNum, dec.txNum)
	require.Equal(t, cs.trieState, dec.trieState)
}

func TestCommitmentV3StateDispatch(t *testing.T) {
	t.Parallel()

	trie := &stateCodecTrie{}
	sdc := &SharedDomainsCommitmentContext{patriciaTrie: trie}
	state, err := sdc.encodeCommitmentState(12, 34)
	require.NoError(t, err)
	require.Equal(t, commitment.CommitmentV3StateMarker, state[0])
	require.Equal(t, commitment.KeyCommitmentV3State, sdc.commitmentStateKey())

	blockNum, txNum, err := sdc.restorePatriciaState(state)
	require.NoError(t, err)
	require.Equal(t, uint64(12), blockNum)
	require.Equal(t, uint64(34), txNum)
}

func TestLegacyCommitmentStateDispatch(t *testing.T) {
	t.Parallel()

	variants := []commitment.Trie{
		commitment.NewHexPatriciaHashed(length.Addr, nil, commitment.DefaultTrieConfig()),
		commitment.NewParallelPatriciaHashed(nil, length.Addr, commitment.DefaultTrieConfig()),
	}
	for _, trie := range variants {
		sdc := &SharedDomainsCommitmentContext{patriciaTrie: trie}
		state, err := sdc.encodeCommitmentState(12, 34)
		require.NoError(t, err)
		var decoded commitmentState
		require.NoError(t, decoded.Decode(state))
		require.Equal(t, uint64(34), decoded.txNum)
		require.Equal(t, uint64(12), decoded.blockNum)
	}
}

func TestCommitmentV3StateRejectsLegacyBlob(t *testing.T) {
	t.Parallel()

	cs := commitmentState{txNum: 2, blockNum: 1, trieState: []byte{3}}
	legacy, err := cs.Encode()
	require.NoError(t, err)

	sdc := &SharedDomainsCommitmentContext{patriciaTrie: &stateCodecTrie{}}
	_, _, err = sdc.restorePatriciaState(legacy)
	require.ErrorContains(t, err, "invalid state variant marker")
}

type stateCodecTrie struct{ testTrie }

type testTrie struct{}

func (*testTrie) RootHash() ([]byte, error) { return nil, nil }

func (*testTrie) SetTraceWriter(io.Writer) {}

func (*testTrie) Variant() commitment.TrieVariant { return commitment.VariantCommitmentV3 }

func (*testTrie) Reset() {}

func (*testTrie) ResetContext(commitment.PatriciaContext) {}

func (*testTrie) Process(context.Context, *commitment.Updates, string, func(*commitment.CommitProgress), commitment.WarmupConfig) ([]byte, error) {
	return nil, nil
}

func (*testTrie) Release() {}

func (*stateCodecTrie) EncodeState(blockNum, txNum uint64, dst []byte) ([]byte, error) {
	return append(dst, commitment.CommitmentV3StateMarker, byte(blockNum), byte(txNum)), nil
}

func (*stateCodecTrie) RestoreState(value []byte) (uint64, uint64, error) {
	if len(value) != 3 || value[0] != commitment.CommitmentV3StateMarker {
		return 0, 0, errors.New("commitment v3: invalid state variant marker")
	}
	return uint64(value[1]), uint64(value[2]), nil
}

type testStateReader struct {
	branchData       []byte
	step             kv.Step
	commitmentDomain kv.Domain
	readDomain       kv.Domain
	readKey          []byte
	readStepSize     uint64
	readCalls        int
	withHistory      bool
}

type seekStateReader struct {
	domain kv.Domain
	state  []byte
}

func (r *seekStateReader) WithHistory() bool { return false }

func (r *seekStateReader) CheckDataAvailable(kv.Domain, kv.Step) error { return nil }

func (r *seekStateReader) Read(domain kv.Domain, key []byte, _ uint64) ([]byte, kv.Step, error) {
	if domain == r.domain && bytes.Equal(key, KeyCommitmentState) {
		return r.state, 0, nil
	}
	return nil, 0, nil
}

func (r *seekStateReader) Clone(kv.TemporalTx) StateReader { return r }

func (r *seekStateReader) CloneForWorker(context.Context, kv.TemporalTx) StateReader { return r }

type seekSharedDomains struct {
	sd
}

func (seekSharedDomains) StepSize() uint64 { return 1 }

func (seekSharedDomains) AsPutDel(kv.TemporalTx) kv.TemporalPutDel { return &fakePutDel{} }

func (seekSharedDomains) HasSharedBranchCache() bool { return false }

type seekTemporalTx struct {
	kv.TemporalTx
	progress []byte
	agg      any
}

func (tx *seekTemporalTx) GetOne(string, []byte) ([]byte, error) { return tx.progress, nil }
func (tx *seekTemporalTx) AggTx() any                            { return tx.agg }

type seekCommitmentLifecycle struct {
	frozen  map[kv.Domain]uint64
	stopped map[kv.Domain]bool
}

func (s seekCommitmentLifecycle) IsDomainFrozen(domain kv.Domain) (uint64, bool) {
	txNum, ok := s.frozen[domain]
	return txNum, ok
}

func (s seekCommitmentLifecycle) CommitmentDomainStopped(domain kv.Domain) bool {
	return s.stopped[domain]
}

func TestSeekCommitmentsRestoresFrozenHexAndAdvancedBin(t *testing.T) {
	t.Parallel()
	hexCtx := seekContext(t, kv.CommitmentDomain, commitment.VariantHexPatriciaTrie, 7, 22, true)
	binCtx := seekContext(t, kv.CommitmentBinDomain, commitment.VariantBinPatriciaTrie, 9, 28, true)
	tx := &seekTemporalTx{agg: seekCommitmentLifecycle{frozen: map[kv.Domain]uint64{kv.CommitmentDomain: 22}}}

	txNum, blockNum, err := SeekCommitments(t.Context(), tx, hexCtx, binCtx)
	require.NoError(t, err)
	require.EqualValues(t, 28, txNum)
	require.EqualValues(t, 9, blockNum)
	require.True(t, hexCtx.justRestored.Load())
	require.True(t, binCtx.justRestored.Load())
}

func TestSeekCommitmentsIgnoresStoppedShadowOnRecreation(t *testing.T) {
	t.Parallel()
	hexCtx := seekContext(t, kv.CommitmentDomain, commitment.VariantHexPatriciaTrie, 9, 28, true)
	binCtx := seekContext(t, kv.CommitmentBinDomain, commitment.VariantBinPatriciaTrie, 7, 22, true)
	tx := &seekTemporalTx{agg: seekCommitmentLifecycle{stopped: map[kv.Domain]bool{kv.CommitmentBinDomain: true}}}

	txNum, blockNum, err := SeekCommitments(t.Context(), tx, hexCtx, binCtx)
	require.NoError(t, err)
	require.EqualValues(t, 28, txNum)
	require.EqualValues(t, 9, blockNum)
	require.True(t, hexCtx.justRestored.Load())
	require.False(t, binCtx.justRestored.Load())
}

func TestSeekCommitmentsIgnoresStoppedHexShadowOnRecreation(t *testing.T) {
	hexCtx := seekContext(t, kv.CommitmentDomain, commitment.VariantHexPatriciaTrie, 7, 22, true)
	binCtx := seekContext(t, kv.CommitmentBinDomain, commitment.VariantBinPatriciaTrie, 9, 28, true)
	tx := &seekTemporalTx{agg: seekCommitmentLifecycle{stopped: map[kv.Domain]bool{kv.CommitmentDomain: true}}}

	txNum, blockNum, err := SeekCommitments(t.Context(), tx, hexCtx, binCtx)
	require.NoError(t, err)
	require.EqualValues(t, 28, txNum)
	require.EqualValues(t, 9, blockNum)
	require.False(t, hexCtx.justRestored.Load())
	require.True(t, binCtx.justRestored.Load())
}

func TestSeekCommitmentsRejectsInvalidFrozenCheckpoint(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		txNum     uint64
		withState bool
	}{
		{name: "before freeze", txNum: 21, withState: true},
		{name: "after freeze", txNum: 23, withState: true},
		{name: "missing state"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hexCtx := seekContext(t, kv.CommitmentDomain, commitment.VariantHexPatriciaTrie, 7, tc.txNum, tc.withState)
			binCtx := seekContext(t, kv.CommitmentBinDomain, commitment.VariantBinPatriciaTrie, 9, 28, true)
			tx := &seekTemporalTx{agg: seekCommitmentLifecycle{frozen: map[kv.Domain]uint64{kv.CommitmentDomain: 22}}}
			_, _, err := SeekCommitments(t.Context(), tx, hexCtx, binCtx)
			require.ErrorIs(t, err, ErrTornCommitmentDatadir)
			require.ErrorContains(t, err, "frozen domain commitment must have commitment state at tx 22")
			require.False(t, hexCtx.justRestored.Load())
			require.False(t, binCtx.justRestored.Load())
		})
	}
}

func TestSeekCommitmentsRejectsLiveStateBehindFreeze(t *testing.T) {
	t.Parallel()
	hexCtx := seekContext(t, kv.CommitmentDomain, commitment.VariantHexPatriciaTrie, 7, 22, true)
	binCtx := seekContext(t, kv.CommitmentBinDomain, commitment.VariantBinPatriciaTrie, 6, 19, true)
	tx := &seekTemporalTx{agg: seekCommitmentLifecycle{frozen: map[kv.Domain]uint64{kv.CommitmentDomain: 22}}}
	_, _, err := SeekCommitments(t.Context(), tx, hexCtx, binCtx)
	require.ErrorIs(t, err, ErrTornCommitmentDatadir)
	require.ErrorContains(t, err, "ahead of live commitment")
}

func seekContext(t *testing.T, domain kv.Domain, variant commitment.TrieVariant, blockNum, txNum uint64, withState bool) *SharedDomainsCommitmentContext {
	t.Helper()
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = variant
	sdc, err := NewSharedDomainsCommitmentContext(seekSharedDomains{}, domain, commitment.ModeDirect, t.TempDir(), cfg)
	require.NoError(t, err)
	if withState {
		stateful, ok := sdc.patriciaTrie.(commitment.StatefulTrie)
		require.True(t, ok)
		trieState, err := stateful.EncodeCurrentState(nil)
		require.NoError(t, err)
		storedState, err := NewCommitmentState(txNum, blockNum, trieState).Encode()
		require.NoError(t, err)
		sdc.SetStateReader(&seekStateReader{domain: domain, state: storedState})
	} else {
		sdc.SetStateReader(&seekStateReader{domain: domain})
	}
	t.Cleanup(sdc.Close)
	return sdc
}

func TestSeekCommitmentsRestoresBothDomainsAtSamePosition(t *testing.T) {
	t.Parallel()
	hexCtx := seekContext(t, kv.CommitmentDomain, commitment.VariantHexPatriciaTrie, 7, 22, true)
	binCtx := seekContext(t, kv.CommitmentBinDomain, commitment.VariantBinPatriciaTrie, 7, 22, true)

	txNum, blockNum, err := SeekCommitments(t.Context(), &seekTemporalTx{}, hexCtx, binCtx)
	require.NoError(t, err)
	require.EqualValues(t, 22, txNum)
	require.EqualValues(t, 7, blockNum)
	require.True(t, hexCtx.justRestored.Load())
	require.True(t, binCtx.justRestored.Load())
}

func TestSeekCommitmentsRejectsMismatchedPositions(t *testing.T) {
	t.Parallel()
	hexCtx := seekContext(t, kv.CommitmentDomain, commitment.VariantHexPatriciaTrie, 7, 22, true)
	binCtx := seekContext(t, kv.CommitmentBinDomain, commitment.VariantBinPatriciaTrie, 8, 22, true)

	_, _, err := SeekCommitments(t.Context(), &seekTemporalTx{}, hexCtx, binCtx)
	require.ErrorIs(t, err, ErrTornCommitmentDatadir)
	require.False(t, hexCtx.justRestored.Load())
	require.False(t, binCtx.justRestored.Load())
}

func TestSeekCommitmentsTreatsMissingProgressAsFresh(t *testing.T) {
	t.Parallel()
	hexCtx := seekContext(t, kv.CommitmentDomain, commitment.VariantHexPatriciaTrie, 0, 0, false)
	binCtx := seekContext(t, kv.CommitmentBinDomain, commitment.VariantBinPatriciaTrie, 0, 0, false)

	txNum, blockNum, err := SeekCommitments(t.Context(), &seekTemporalTx{}, hexCtx, binCtx)
	require.NoError(t, err)
	require.Zero(t, txNum)
	require.Zero(t, blockNum)
}

func TestSeekCommitmentsRejectsOneDomainMissingState(t *testing.T) {
	t.Parallel()
	hexCtx := seekContext(t, kv.CommitmentDomain, commitment.VariantHexPatriciaTrie, 7, 22, true)
	binCtx := seekContext(t, kv.CommitmentBinDomain, commitment.VariantBinPatriciaTrie, 0, 0, false)

	_, _, err := SeekCommitments(t.Context(), &seekTemporalTx{}, hexCtx, binCtx)
	require.ErrorIs(t, err, ErrTornCommitmentDatadir)
}

var _ StateReader = (*testStateReader)(nil)

func (r *testStateReader) WithHistory() bool { return r.withHistory }

func (r *testStateReader) CheckDataAvailable(kv.Domain, kv.Step) error { return nil }

func (r *testStateReader) Read(d kv.Domain, key []byte, stepSize uint64) ([]byte, kv.Step, error) {
	r.readCalls++
	r.readDomain = d
	r.readKey = append(r.readKey[:0], key...)
	r.readStepSize = stepSize
	commitmentDomain := r.commitmentDomain
	if commitmentDomain == kv.AccountsDomain {
		commitmentDomain = kv.CommitmentDomain
	}
	if r.readDomain != commitmentDomain {
		return nil, 0, nil
	}
	return r.branchData, r.step, nil
}

func (r *testStateReader) Clone(kv.TemporalTx) StateReader { return r }

func (r *testStateReader) CloneForWorker(context.Context, kv.TemporalTx) StateReader { return r }

func Test_TrieContext_BranchCopiesData(t *testing.T) {
	t.Parallel()

	prefix := []byte{0xaa}
	expectedBranchData := []byte{1, 2, 3}
	reader := &testStateReader{
		branchData: append([]byte(nil), expectedBranchData...),
		step:       42,
	}
	ctx := NewTrieContextRo(reader, 1)

	branch, step, err := ctx.Branch(prefix)
	require.NoError(t, err)
	require.Equal(t, reader.step, step)
	require.Equal(t, expectedBranchData, branch)
	require.Equal(t, kv.CommitmentDomain, reader.readDomain)
	require.Equal(t, prefix, reader.readKey)
	require.Equal(t, uint64(1), reader.readStepSize)

	reader.branchData[0] = 9
	require.Equal(t, expectedBranchData, branch)

	branch[1] = 8
	require.Equal(t, []byte{9, 2, 3}, reader.branchData)
}

// Test_NewSharedDomainsCommitmentContext_AcceptsBinVariant pins that the bin
// variant constructs like any other stateful trie and carries its own variant
// tag instead of the hex default.
func Test_NewSharedDomainsCommitmentContext_AcceptsBinVariant(t *testing.T) {
	t.Parallel()

	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantBinPatriciaTrie
	sdc, err := NewSharedDomainsCommitmentContext(nil, kv.CommitmentBinDomain, commitment.ModeDirect, t.TempDir(), cfg)
	require.NoError(t, err)
	defer sdc.Close()
	require.Equal(t, commitment.VariantBinPatriciaTrie, sdc.Trie().Variant())
	require.Equal(t, commitment.VariantBinPatriciaTrie, sdc.variant)
}

func TestCommitmentContextUsesBoundBinDomain(t *testing.T) {
	t.Parallel()

	reader := &testStateReader{branchData: []byte{1, 2, 3}, commitmentDomain: kv.CommitmentBinDomain}
	putter := &fakePutDel{}
	sdc := &SharedDomainsCommitmentContext{commitmentDomain: kv.CommitmentBinDomain, stateReader: reader}
	trieContext := &TrieContext{commitmentDomain: sdc.CommitmentDomain(), stateReader: reader, putter: putter, txNum: 7}

	branch, _, err := trieContext.Branch([]byte{0xaa})
	require.NoError(t, err)
	require.Equal(t, []byte{1, 2, 3}, branch)
	require.Equal(t, kv.CommitmentBinDomain, reader.readDomain)

	require.NoError(t, trieContext.PutBranch([]byte{0xbb}, []byte{4}, []byte{5}))
	require.Len(t, putter.puts, 1)
	require.Equal(t, kv.CommitmentBinDomain, putter.puts[0].domain)
}

func TestSharedDomainsCodeKeysFollowTouchedUpdates(t *testing.T) {
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantBinPatriciaTrie
	sdc, err := NewSharedDomainsCommitmentContext(nil, kv.CommitmentBinDomain, commitment.ModeDirect, t.TempDir(), cfg)
	require.NoError(t, err)
	t.Cleanup(sdc.Close)
	address := make([]byte, 20)
	sdc.TouchKey(kv.CodeDomain, string(address), []byte{1})
	require.Equal(t, map[string]struct{}{string(address): {}}, sdc.CodeKeys())
	sdc.SetUpdates(commitment.NewUpdates(commitment.ModeDirect, "", commitment.KeyToHexNibbleHash))
	require.Empty(t, sdc.CodeKeys())
}

func TestSharedDomainsCodeKeysClearOnResetPaths(t *testing.T) {
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantBinPatriciaTrie
	sdc, err := NewSharedDomainsCommitmentContext(nil, kv.CommitmentBinDomain, commitment.ModeDirect, t.TempDir(), cfg)
	require.NoError(t, err)
	t.Cleanup(sdc.Close)
	address := make([]byte, 20)
	sdc.TouchKey(kv.CodeDomain, string(address), []byte{1})
	sdc.Reset()
	require.Empty(t, sdc.CodeKeys())
	sdc.TouchKey(kv.CodeDomain, string(address), []byte{1})
	sdc.ResetPendingUpdates()
	require.Empty(t, sdc.CodeKeys())
}

func TestSharedDomainsCodeKeysClearAfterCompute(t *testing.T) {
	rec := &commitmentTimeRecorder{}
	sdc, err := NewSharedDomainsCommitmentContext(rec, kv.CommitmentDomain, commitment.ModeDirect, t.TempDir(), commitment.TrieConfig{})
	require.NoError(t, err)
	t.Cleanup(sdc.Close)
	sdc.SetStateReader(&testStateReader{})
	sdc.TouchKey(kv.CodeDomain, string(make([]byte, 20)), []byte{1})
	require.NotEmpty(t, sdc.CodeKeys())
	_, err = sdc.ComputeCommitment(t.Context(), nil, false, 0, 0, "", nil)
	require.NoError(t, err)
	require.Empty(t, sdc.CodeKeys())
}

type branchChildCountDomains struct {
	stubSharedDomains
	value   []byte
	ok      bool
	bound   bool
	maxStep kv.Step
	calls   int
	key     []byte
}

func (d *branchChildCountDomains) GetLatestFromMemory(domain kv.Domain, key []byte) ([]byte, kv.Step, bool) {
	d.calls++
	d.key = append(d.key[:0], key...)
	if domain != kv.CommitmentDomain || !d.ok {
		if d.bound {
			return nil, d.maxStep, false
		}
		return nil, kv.NoStepBound, false
	}
	return d.value, kv.NoStepBound, true
}

func TestBranchChildCountReadsPostComputeView(t *testing.T) {
	t.Parallel()

	prefix := []byte{0x0a}
	compactKey := nibbles.HexToCompact(prefix)

	t.Run("changed branch comes from memory", func(t *testing.T) {
		domains := &branchChildCountDomains{value: []byte{0, 0, 0, 0b0000_0111}, ok: true}
		reader := &testStateReader{branchData: []byte{0, 0, 0, 0b0000_0011}}
		sdc := &SharedDomainsCommitmentContext{
			sharedDomains: domains,
			stateReader:   reader,
		}

		count, err := sdc.BranchChildCount(prefix)
		require.NoError(t, err)
		require.Equal(t, 3, count)
		require.Equal(t, 1, domains.calls)
		require.Equal(t, compactKey, domains.key)
		require.Zero(t, reader.readCalls)
	})

	t.Run("unchanged branch comes from installed reader", func(t *testing.T) {
		domains := &branchChildCountDomains{}
		reader := &testStateReader{branchData: []byte{0, 0, 0, 0b0000_0011}}
		sdc := &SharedDomainsCommitmentContext{
			sharedDomains: domains,
			stateReader:   reader,
		}

		count, err := sdc.BranchChildCount(prefix)
		require.NoError(t, err)
		require.Equal(t, 2, count)
		require.Equal(t, 1, domains.calls)
		require.Equal(t, compactKey, domains.key)
		require.Equal(t, 1, reader.readCalls)
		require.Equal(t, kv.CommitmentDomain, reader.readDomain)
		require.Equal(t, compactKey, reader.readKey)
		require.Equal(t, uint64(1), reader.readStepSize)
	})
}

func TestBranchChildCountRejectsIncompleteComputedView(t *testing.T) {
	t.Parallel()

	prefix := []byte{0x0a}
	branch := []byte{0, 0, 0, 0b0000_0011}

	t.Run("missing state reader", func(t *testing.T) {
		sdc := &SharedDomainsCommitmentContext{
			sharedDomains: &branchChildCountDomains{},
		}

		_, err := sdc.BranchChildCount(prefix)
		require.ErrorContains(t, err, "installed state reader")
	})

	t.Run("history reader suppresses branch writes", func(t *testing.T) {
		reader := &testStateReader{branchData: branch, withHistory: true}
		sdc := &SharedDomainsCommitmentContext{
			sharedDomains: &branchChildCountDomains{},
			stateReader:   reader,
		}

		_, err := sdc.BranchChildCount(prefix)
		require.ErrorContains(t, err, "reader that permits branch writes")
	})

	t.Run("deferred branch updates are pending", func(t *testing.T) {
		reader := &testStateReader{branchData: branch}
		sdc := &SharedDomainsCommitmentContext{
			sharedDomains: &branchChildCountDomains{},
			stateReader:   reader,
			pendingUpdate: &commitment.PendingCommitmentUpdate{},
		}

		_, err := sdc.BranchChildCount(prefix)
		require.ErrorContains(t, err, "deferred branch updates are pending")
	})

	t.Run("staged unwind bounds the fallback", func(t *testing.T) {
		reader := &testStateReader{branchData: branch}
		sdc := &SharedDomainsCommitmentContext{
			sharedDomains: &branchChildCountDomains{bound: true, maxStep: 1},
			stateReader:   reader,
		}

		_, err := sdc.BranchChildCount(prefix)
		require.ErrorContains(t, err, "staged unwind")
	})
}

func Test_TrieContext_BranchReusesBufferAcrossReads(t *testing.T) {
	t.Parallel()

	reader := &testStateReader{branchData: []byte{1, 2, 3}, step: 7}
	ctx := NewTrieContextRo(reader, 1)

	got1, _, err := ctx.Branch([]byte{0xaa})
	require.NoError(t, err)
	require.Equal(t, []byte{1, 2, 3}, got1)

	reader.branchData = []byte{4, 5, 6}
	got2, _, err := ctx.Branch([]byte{0xbb})
	require.NoError(t, err)
	require.Equal(t, []byte{4, 5, 6}, got2)

	// The contract Branch's callers rely on: the returned bytes live only until the
	// next read on this context. A caller that keeps them sees the newer branch.
	require.Equal(t, []byte{4, 5, 6}, got1, "second read must land in the same buffer")
	require.Equal(t, &got1[0], &got2[0], "second read must not allocate a new buffer")
}

func Test_TrieContext_BranchKeepsNilAndEmptyDistinct(t *testing.T) {
	t.Parallel()

	// A nil branch means "absent"; callers test it with == nil, so reusing a buffer
	// must not turn it into an empty non-nil slice.
	reader := &testStateReader{}
	ctx := NewTrieContextRo(reader, 1)

	got, _, err := ctx.Branch([]byte{0xaa})
	require.NoError(t, err)
	require.Nil(t, got, "absent branch must stay nil")

	reader.branchData = []byte{1, 2, 3}
	if _, _, err = ctx.Branch([]byte{0xbb}); err != nil {
		t.Fatal(err)
	}

	reader.branchData = []byte{}
	got, _, err = ctx.Branch([]byte{0xcc})
	require.NoError(t, err)
	require.NotNil(t, got, "present-but-empty branch must stay non-nil after the buffer is warm")
	require.Empty(t, got)

	reader.branchData = nil
	got, _, err = ctx.Branch([]byte{0xdd})
	require.NoError(t, err)
	require.Nil(t, got, "absent branch must stay nil after the buffer is warm")
}

type commitmentTimeRecorder struct {
	pbinStateStubSD
	calls int
}

func (r *commitmentTimeRecorder) AddCommitmentTime(time.Duration) { r.calls++ }

func TestComputeCommitmentReportsItsDuration(t *testing.T) {
	t.Parallel()
	rec := &commitmentTimeRecorder{}
	sdc, err := NewSharedDomainsCommitmentContext(rec, kv.CommitmentDomain, commitment.ModeDirect, t.TempDir(), commitment.TrieConfig{})
	require.NoError(t, err)

	_, err = sdc.ComputeCommitment(t.Context(), nil, false, 0, 0, "", nil)
	require.NoError(t, err)
	require.Equal(t, 1, rec.calls)
}

type ownedTestStateReader struct{ *testStateReader }

func (ownedTestStateReader) ReadsOwnedBranches() {}

func TestTrieContextBranchOwnedCopiesOnlyBorrowedBytes(t *testing.T) {
	t.Parallel()
	data := []byte{1, 2, 3}
	borrowed := &TrieContext{stateReader: &testStateReader{branchData: data}}
	got, _, err := borrowed.BranchOwned([]byte{0xaa})
	require.NoError(t, err)
	require.Equal(t, data, got)
	require.NotSame(t, &data[0], &got[0])

	owned := &TrieContext{stateReader: ownedTestStateReader{&testStateReader{branchData: data}}}
	got, _, err = owned.BranchOwned([]byte{0xaa})
	require.NoError(t, err)
	require.Same(t, &data[0], &got[0])
}
