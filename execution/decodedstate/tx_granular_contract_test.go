package decodedstate

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestS_HIST_07_CanonicalHistoryAndProgressMarkersUseTxNum(t *testing.T) {
	storeSrc := readRepoFile(t, "execution", "decodedstate", "store.go")
	encodeHistoryKeySrc := functionSource(t, filepath.Join("execution", "decodedstate", "store.go"), "encodeHistoryKey")

	require.Contains(
		t,
		storeSrc,
		"metaLatestTxNum",
		"decoded meta must track a canonical latestTxNum marker alongside latestBlock",
	)
	require.Contains(
		t,
		storeSrc,
		"LatestTxNumTx",
		"decoded-state storage must expose a reader for the canonical latestTxNum marker",
	)
	require.Contains(
		t,
		storeSrc,
		"AdvanceLatestTxNumTx",
		"decoded-state storage must advance latestTxNum independently from latestBlock",
	)
	require.Contains(
		t,
		encodeHistoryKeySrc,
		"txNum uint64",
		"decoded history keys must be encoded with txNum, not blockNum",
	)
	require.NotContains(
		t,
		encodeHistoryKeySrc,
		"blockNum uint64",
		"decoded history key encoding must not use blockNum as the canonical history axis",
	)
}

func TestS_SNAP_13_TemporalDecodedWritersReceiveCanonicalTxNumAcrossAllPaths(t *testing.T) {
	writerSrc := functionSource(t, filepath.Join("execution", "decodedstate", "temporal.go"), "WriteEntriesToDomains")
	compactWriterSrc := compactSource(writerSrc)
	serialSrc := compactSource(readRepoFile(t, "execution", "stagedsync", "exec3_serial.go"))
	parallelSrc := compactSource(readRepoFile(t, "execution", "stagedsync", "exec3_parallel.go"))
	replaySrc := compactSource(readRepoFile(t, "execution", "stagedsync", "stage_decoded_state.go"))

	require.Contains(
		t,
		writerSrc,
		"txNum uint64",
		"decoded temporal-domain writer must accept canonical txNum explicitly",
	)
	require.NotContains(
		t,
		compactWriterSrc,
		"txNum:=blockNum",
		"decoded temporal-domain writes must not derive txNum by copying blockNum",
	)
	require.NotContains(
		t,
		serialSrc,
		"WriteEntriesToDomains(se.doms,ttx,blockNum,entries)",
		"serial execution must pass block-final canonical txNum to the decoded temporal writer",
	)
	require.NotContains(
		t,
		parallelSrc,
		"WriteEntriesToDomains(pe.doms,rwTx,applyResult.BlockNum,applyResult.decodedEntries)",
		"parallel execution must pass canonical applyResult.lastTxNum to the decoded temporal writer",
	)
	require.NotContains(
		t,
		replaySrc,
		"WriteEntriesToDomains(doms,tx,currentBlock,blockEntries)",
		"decoded replay must flush through the temporal writer using block-end canonical txNum rather than currentBlock",
	)
}

func TestS_SNAP_14_ReplayStepCalculationsUseLatestTxNumNotBlockNumbers(t *testing.T) {
	replaySrc := readRepoFile(t, "execution", "stagedsync", "stage_decoded_state.go")
	compactReplaySrc := compactSource(replaySrc)

	require.Contains(
		t,
		replaySrc,
		"LatestTxNumTx",
		"decoded replay must read latestTxNum so temporal alignment and snapshot-freeze progress stay in tx space",
	)
	require.NotContains(
		t,
		compactReplaySrc,
		"toStep=kv.Step(lastProcessedBlock/agg.StepSize())",
		"decoded replay must not derive snapshot steps from blockNumber/stepSize",
	)
}

func TestS_RPC_12_ExplicitAtTxMethodsExistAcrossDecodedRPCLayers(t *testing.T) {
	typesSrc := readRepoFile(t, "execution", "decodedstate", "types.go")
	handlerSrc := readRepoFile(t, "execution", "decodedstate", "rpc_handler.go")
	jsonrpcSrc := readRepoFile(t, "rpc", "jsonrpc", "erigon_decodedstate.go")

	for _, method := range []string{
		"EnumerateMappingKeysAtTx",
		"GetDecodedStorageAtTx",
		"GetMappingValueAtTx",
	} {
		require.Contains(
			t,
			typesSrc,
			method,
			"decoded RPC handler contract must expose explicit tx-granular methods",
		)
		require.Contains(
			t,
			handlerSrc,
			method,
			"decoded in-process RPC handler must implement explicit tx-granular methods",
		)
		require.Contains(
			t,
			jsonrpcSrc,
			method,
			"live JSON-RPC service must register explicit tx-granular decoded-state methods",
		)
	}
}

func TestS_RPC_13_BlockCompatibleRPCResolvesBlocksToCanonicalTxBoundaries(t *testing.T) {
	jsonrpcSrc := readRepoFile(t, "rpc", "jsonrpc", "erigon_decodedstate.go")
	compactJSONRPCSrc := compactSource(jsonrpcSrc)

	require.Contains(
		t,
		jsonrpcSrc,
		"_txNumReader",
		"block-compatible decoded RPC methods must resolve block selectors through the canonical tx boundary reader",
	)
	require.NotContains(
		t,
		compactJSONRPCSrc,
		"decodedstate.QueryEnumerateKeysAsOf(tx,contract,mappingSlot,blockNum)",
		"block-compatible decoded RPC methods must not pass raw block numbers straight into tx-granular history lookup",
	)
	require.NotContains(
		t,
		compactJSONRPCSrc,
		"decodedstate.QueryDecodedStorageAsOf(tx,contract,blockNum)",
		"block-compatible decoded storage RPC must resolve block selectors to tx boundaries before querying history",
	)
	require.NotContains(
		t,
		compactJSONRPCSrc,
		"decodedstate.QueryMappingValueAsOf(tx,contract,mappingSlot,keyHash,blockNum)",
		"block-compatible decoded mapping-value RPC must resolve block selectors to tx boundaries before querying history",
	)
}

func TestS_UNW_08_UnwindUsesCanonicalTxHelperWithBlockCompatibilityWrapper(t *testing.T) {
	storeSrc := readRepoFile(t, "execution", "decodedstate", "store.go")

	require.Contains(
		t,
		storeSrc,
		"UnwindDecodedStateToTxNum",
		"decoded unwind must expose a canonical tx-granular helper",
	)
	require.Contains(
		t,
		storeSrc,
		"UnwindDecodedStateTx",
		"decoded unwind must keep a block-oriented compatibility wrapper",
	)
}

func TestS_PRUNE_07_PruneUsesCanonicalTxHelperWithBlockCompatibilityWrapper(t *testing.T) {
	storeSrc := readRepoFile(t, "execution", "decodedstate", "store.go")

	require.Contains(
		t,
		storeSrc,
		"PruneDecodedHistoryToTxNum",
		"decoded prune must expose a canonical tx-granular helper",
	)
	require.Contains(
		t,
		storeSrc,
		"PruneDecodedHistoryTx",
		"decoded prune must keep a block-oriented compatibility wrapper",
	)
}
