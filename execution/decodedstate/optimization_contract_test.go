package decodedstate

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/tracing"
)

func repoRoot(t *testing.T) string {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)

	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func readRepoFile(t *testing.T, elems ...string) string {
	t.Helper()

	path := filepath.Join(append([]string{repoRoot(t)}, elems...)...)
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	return string(data)
}

func compactSource(s string) string {
	return strings.Join(strings.Fields(s), "")
}

func functionSource(t *testing.T, rel string, funcName string) string {
	t.Helper()

	root := repoRoot(t)
	path := filepath.Join(root, rel)
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, data, parser.ParseComments)
	require.NoError(t, err)

	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != funcName {
			continue
		}

		start := fset.Position(fn.Pos()).Offset
		end := fset.Position(fn.End()).Offset
		return string(data[start:end])
	}

	t.Fatalf("function %s not found in %s", funcName, rel)
	return ""
}

func TestS_WL_05_RuntimeCollectorUsesConfiguredWhitelistWithoutLinearRescans(t *testing.T) {
	t.Run("exec3-config", func(t *testing.T) {
		exec3Src := compactSource(readRepoFile(t, "execution", "stagedsync", "exec3.go"))
		require.NotContains(
			t,
			exec3Src,
			"decodedstate.Config{Enabled:true,FullMode:true}",
			"ExecV3 must construct decoded-state collectors from the runtime decoded config rather than hardcoding full mode",
		)
	})

	t.Run("collector-membership-check", func(t *testing.T) {
		collectorSrc := compactSource(readRepoFile(t, "execution", "decodedstate", "collector.go"))
		require.NotContains(
			t,
			collectorSrc,
			"for_,a:=rangec.cfg.Whitelist{",
			"collector hot-path tracking checks must use a precompiled whitelist index instead of rescanning the whitelist slice",
		)
	})
}

func TestS_NODE_12_CollectorDoesNotLeakDecodedCandidatesAcrossTransactions(t *testing.T) {
	c := NewCollector(enabledFullConfig())

	slot := slotHash(0)
	key := keyHash(42)
	loc := mappingLocation(key, slot)

	c.OnEnterCall(addrA, addrA, false)
	c.OnKeccak256(addrA, mappingInput(key, slot), loc)
	c.OnExitCall(false)
	require.Empty(t, c.Entries(), "keccak-only transaction must not emit decoded entries")

	c.OnEnterCall(addrA, addrA, false)
	c.OnSStore(addrA, loc, u256(100))
	c.OnExitCall(false)

	require.Empty(
		t,
		c.Entries(),
		"a later transaction must not reuse decoded candidates that were discovered by an earlier transaction",
	)
}

func TestS_SHA3_05_RuntimeUsesDedicatedKeccakHookInsteadOfGenericOpcodeTracing(t *testing.T) {
	t.Run("hooks-struct", func(t *testing.T) {
		hooksType := reflect.TypeOf(tracing.Hooks{})
		_, ok := hooksType.FieldByName("OnKeccak256")
		require.True(
			t,
			ok,
			"tracing.Hooks must expose a dedicated OnKeccak256 callback so decoded-state capture does not depend on generic OnOpcode tracing",
		)
	})

	t.Run("native-opcode-path", func(t *testing.T) {
		vmSrc := readRepoFile(t, "execution", "vm", "instructions.go")
		require.Contains(
			t,
			vmSrc,
			"OnKeccak256",
			"the KECCAK256 opcode implementation must call the dedicated keccak hook from the native opcode path",
		)
	})

	t.Run("decoded-hooks", func(t *testing.T) {
		decodedHooksSrc := compactSource(readRepoFile(t, "execution", "decodedstate", "hooks.go"))
		require.NotContains(
			t,
			decodedHooksSrc,
			"OnOpcode:h.onOpcode",
			"decoded-state hooks must no longer install KECCAK capture through the generic opcode tracer",
		)
	})
}

func TestS_SHA3_06_NativeKeccakHookAvoidsPerHashInputCopies(t *testing.T) {
	opSrc := compactSource(functionSource(t, filepath.Join("execution", "vm", "instructions.go"), "opKeccak256"))

	require.NotContains(
		t,
		opSrc,
		"make([]byte,len(data))",
		"the native KECCAK256 opcode path must not allocate a fresh input buffer for every traced hash",
	)
	require.NotContains(
		t,
		opSrc,
		"copy(input,data)",
		"the native KECCAK256 opcode path must not copy the preimage before dispatching the keccak hook",
	)
}

func TestS_ARR_04_ArrayCandidatesAreScopedToStorageOwner(t *testing.T) {
	c := NewCollector(enabledFullConfig())

	slot := slotHash(3)
	base := arrayBase(slot)
	foreignArrayLocation := arrayElementLocation(slot, 1)

	c.OnKeccak256(addrB, arrayBaseInput(slot), base)
	c.OnSStore(addrA, foreignArrayLocation, u256(77))

	require.Empty(
		t,
		c.Entries(),
		"array candidates learned from another contract must not decode writes for this contract",
	)
}

func TestS_EDGE_07_CollectorEntriesTolerateNilFrames(t *testing.T) {
	c := &collector{
		frames: []*callFrame{
			nil,
			{
				candidates: make(map[common.Hash]*hashCandidate),
				arrayBases: make(map[common.Hash]arrayBaseInfo),
			},
		},
	}

	require.NotPanics(
		t,
		func() { _ = c.Entries() },
		"collector entry aggregation must skip nil frames instead of panicking on partially-cleared frame stacks",
	)
}

func TestS_SNAP_11_LiveDecodedWritesReuseActiveDomainsAndFixedBinaryEncoding(t *testing.T) {
	storeSrc := readRepoFile(t, "execution", "decodedstate", "store.go")
	compactStoreSrc := compactSource(storeSrc)

	t.Run("fixed-binary-encoding", func(t *testing.T) {
		require.NotContains(
			t,
			storeSrc,
			"\"encoding/gob\"",
			"decoded-state persistence must use a fixed binary encoding rather than gob",
		)
		require.NotContains(t, compactStoreSrc, "gob.NewEncoder(", "decoded-state persistence must not encode records through gob")
		require.NotContains(t, compactStoreSrc, "gob.NewDecoder(", "decoded-state persistence must not decode records through gob")
	})

	t.Run("reuse-shared-domains", func(t *testing.T) {
		require.NotContains(
			t,
			compactStoreSrc,
			"execctx.NewSharedDomains(",
			"live decoded-state writes must reuse the already-open shared domains instead of creating a fresh SharedDomains instance",
		)
	})
}

func TestS_SNAP_12_StagedExecutionReusesOpenDomainsForDecodedWrites(t *testing.T) {
	t.Run("serial-executor", func(t *testing.T) {
		serialSrc := compactSource(readRepoFile(t, "execution", "stagedsync", "exec3_serial.go"))
		require.Contains(
			t,
			serialSrc,
			"decodedstate.WriteEntriesToDomains(",
			"serial staged execution must flush decoded entries through the already-open shared domains",
		)
		require.NotContains(
			t,
			serialSrc,
			"decodedstate.WriteEntriesToTx(",
			"serial staged execution must not route decoded writes through the generic WriteEntriesToTx helper when shared domains are already open",
		)
	})

	t.Run("parallel-executor", func(t *testing.T) {
		parallelSrc := compactSource(readRepoFile(t, "execution", "stagedsync", "exec3_parallel.go"))
		require.Contains(
			t,
			parallelSrc,
			"decodedstate.WriteEntriesToDomains(",
			"parallel staged execution must flush decoded entries through the already-open shared domains",
		)
		require.NotContains(
			t,
			parallelSrc,
			"decodedstate.WriteEntriesToTx(",
			"parallel staged execution must not route decoded writes through the generic WriteEntriesToTx helper when shared domains are already open",
		)
	})
}

func TestS_HIST_06_HistoricalQueriesAvoidFullScanReplayPerKey(t *testing.T) {
	t.Run("enumerate-keys-asof", func(t *testing.T) {
		enumerateSrc := compactSource(functionSource(t, filepath.Join("execution", "decodedstate", "query.go"), "QueryEnumerateKeysAsOf"))
		require.NotContains(
			t,
			enumerateSrc,
			"allKeys:=make(map[string]storeKey)",
			"historical key enumeration must not reconstruct the entire keyspace in-memory from flat-table scans",
		)
		require.NotContains(
			t,
			enumerateSrc,
			"getAsOf(tx,sk,blockNum)",
			"historical key enumeration must not replay getAsOf for every candidate key discovered by the flat-table scan",
		)
	})

	t.Run("decoded-storage-asof", func(t *testing.T) {
		storageSrc := compactSource(functionSource(t, filepath.Join("execution", "decodedstate", "query.go"), "QueryDecodedStorageAsOf"))
		require.NotContains(
			t,
			storageSrc,
			"all:=make(map[string]skInfo)",
			"historical decoded-storage queries must not materialize the full candidate set from flat Latest/History scans",
		)
		require.NotContains(
			t,
			storageSrc,
			"getAsOf(tx,info.sk,blockNum)",
			"historical decoded-storage queries must not replay getAsOf once per candidate key after the flat-table scan",
		)
	})
}

func TestS_NODE_13_SerialExecUsesTransactionScopedDecodedCollectors(t *testing.T) {
	serialSrc := compactSource(readRepoFile(t, "execution", "stagedsync", "exec3_serial.go"))

	require.Contains(
		t,
		serialSrc,
		"DecodedCollector:",
		"serial ExecV3 must attach a decoded collector to each transaction task so decoded-state collection is scoped per transaction",
	)
	require.NotContains(
		t,
		serialSrc,
		"se.decodedStateCollector.Entries()",
		"serial ExecV3 must not rely on a single block-scoped decoded collector when transaction-scoped collectors are required",
	)
	require.NotContains(
		t,
		serialSrc,
		"se.decodedStateCollector.Reset()",
		"serial ExecV3 must not reset a single shared decoded collector at block end once collection is transaction-scoped",
	)
}

func TestS_NODE_14_ParallelRetriesRefreshDecodedCollectorsAcrossIncarnations(t *testing.T) {
	resetSrc := compactSource(functionSource(t, filepath.Join("execution", "exec", "txtask.go"), "Reset"))
	scheduleSrc := compactSource(functionSource(t, filepath.Join("execution", "stagedsync", "exec3_parallel.go"), "scheduleExecution"))

	resetPath := strings.Contains(resetSrc, "DecodedCollector") && strings.Contains(resetSrc, ".Reset(")
	retryRecreatePath := strings.Contains(scheduleSrc, "DecodedCollector") && strings.Contains(scheduleSrc, "decodedstate.NewCollector(")

	require.True(
		t,
		resetPath || retryRecreatePath,
		"parallel retry must either reset an existing decoded collector in TxTask.Reset or replace it with a fresh collector before rescheduling a new incarnation",
	)
}
