package instrumentation_test

/*
import (
	"context"
	"encoding/json"
	"io"
	"os"
	"testing"

	"github.com/ledgerwatch/erigon/zkevm/db"
	"github.com/ledgerwatch/erigon/zkevm/log"
	"github.com/ledgerwatch/erigon/zkevm/state"
	"github.com/ledgerwatch/erigon/zkevm/state/runtime/instrumentation"
	"github.com/ledgerwatch/erigon/zkevm/state/tree"
	"github.com/ledgerwatch/erigon/zkevm/test/dbutils"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/require"
)

var (
	ctx = context.Background()
)

var cfg = dbutils.NewConfigFromEnv()

var stateCfg = state.Config{
	DefaultChainID:       1000,
	MaxCumulativeGasUsed: 800000,
}

func TestSCTxs(t *testing.T) {
	var sequencerAddress = common.HexToAddress("0x617b3a3528F9cDd6630fd3301B9c8911F7Bf063D")
	var transactionHash = "0x73487b885a5f595b48a9e4227f30c7d538cd89e8b54d565c7c559d2e073062a0"
	// var transactionHash = "0x4a1b960d5fcf8107a41704e6d5cb3ec9a75bb0cb5c37b898b718c74a6bcab5bb"
	// var transactionHash = "0xc63446b6277b82b8be92c3cbe57db9beb94c91063d6d0667e6e23aed178b836e"
	// var transactionHash = "0xd071ac9bd2209febbfdc447834d7a763150f1f50ba444ef87fb838cdd060741a"
	// var transactionHash = "0x2dc633855d6494fc846d5ab332b5c016998327654c38627434a26c7c2317deeb"
	// var transactionHash = "0x152105092be3ec6fb42c4970b9b90d57e739b0351c64efe12e8510332ef495d6"
	// var transactionHash = "0x125258172bbcfcc83c4dc4f81fe1858fcce1adc76ded265081f12e8b293ab449"
	// var transactionHash = "0x545531ac79f04c010bb9d25789e8c6306a08efaa1e15e5bdc5c27c1f4bab181c"
	// var transactionHash = "0x508c2cbfc84b8c3f6a54ccf4ef1fd56c9ce31fad4b05e843582a566e5a85e460"
	// var transactionHash = "0x30632f89439ab8074d8252ee44f49c929e2e8eb5364708441c8d121f89a76c88"
	// var transactionHash = "0xc532a6d8a85d50950d853a8d8cf66ac18741a40a3cd6666e55ef172756e1156d"
	// var transactionHash = "0xd9bf4f0c140c06348e1231ccdc4f393a6475889a28b00f0ad01c8f6b3f9bf792"

	// Create State db
	stateDb, err := db.NewSQLDB(cfg)
	require.NoError(t, err)

	// Create State tree
	store := tree.NewPostgresStore(stateDb)
	mt := tree.NewMerkleTree(store, tree.DefaultMerkleTreeArity)
	scCodeStore := tree.NewPostgresSCCodeStore(stateDb)
	stateTree := tree.NewStateTree(mt, scCodeStore)

	// Create state
	st := state.NewState(stateCfg, state.NewPostgresStorage(stateDb), stateTree)

	lastBatch, err := st.GetLastBatch(ctx, true, "")
	require.NoError(t, err)

	// Create Batch Processor
	bp, err := st.NewBatchProcessor(ctx, sequencerAddress, lastBatch.Header.Root[:], "")
	require.NoError(t, err)

	// Execution Trace
	receipt, err := bp.Host.State.GetTransactionReceipt(ctx, common.HexToHash(transactionHash), "")
	require.NoError(t, err)

	// Read tracer from filesystem
	var tracer instrumentation.Tracer
	tracerFile, err := os.Open("../../../test/tracers/tracer.json")
	require.NoError(t, err)
	defer tracerFile.Close()

	byteCode, err := io.ReadAll(tracerFile)
	require.NoError(t, err)

	err = json.Unmarshal(byteCode, &tracer)
	require.NoError(t, err)

	result, err := st.DebugTransaction(context.Background(), receipt.TxHash, tracer.Code)
	require.NoError(t, err)

	// j, err := json.Marshal(result.ExecutorTrace)
	// require.NoError(t, err)
	// log.Debug(string(j))

	file, _ := json.MarshalIndent(result.ExecutorTrace, "", " ")
	err = os.WriteFile("trace.json", file, 0644)
	require.NoError(t, err)

	log.Debug(string(result.ExecutorTraceResult))
}
*/
