package native

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/turbo/jsonrpc"
)

func TestParityOpenEthereumTracer_TraceBlock(t *testing.T) {
	jsonBytes, err := os.ReadFile("/Users/taratorio/trace_block_16494085_response.json")
	require.NoError(t, err)

	var response ParityTracesResponse
	err = json.Unmarshal(jsonBytes, &response)
	require.NoError(t, err)

	//address := "0xbaae833f06dac8ef2ab8dea7b707dbcfa048a5f6"
	address := ""
	transaction := "0x536434786ace02697118c44abf2835f188bf79902807c61a523ca3a6200bc350"
	for _, parityTrace := range response.Result {
		var log bool
		log = parityTrace.TransactionHash.String() == transaction
		action := parityTrace.Action.(map[string]interface{})
		from, ok := action["from"].(string)
		if ok {
			log = log || from == address
		}
		to, ok := action["to"].(string)
		if ok {
			log = log || to == address
		}
		if log {
			fmt.Printf("Call from %s to %s: Trace Address = %v\n", from, to, parityTrace.TraceAddress)
		}
	}
}

type ParityTracesResponse struct {
	Result jsonrpc.ParityTraces `json:"result"`
}

func TestCallTracer_DebugTraceBlock(t *testing.T) {
	jsonBytes, err := os.ReadFile("/Users/taratorio/debug_traceBlockByNumber_16494085_response.json")
	require.NoError(t, err)

	var response DebugTraceBlockResponse
	err = json.Unmarshal(jsonBytes, &response)
	require.NoError(t, err)

	topLevelCfs := response.TopLevelCallFrames()
	allCfs := response.AllCallFrames()
	traceAddrSeq := BuildTraceAddrSeq(topLevelCfs)
	require.Equal(t, len(allCfs), len(traceAddrSeq))

	address := common.HexToAddress("0xbaae833f06dac8ef2ab8dea7b707dbcfa048a5f6")
	for i, traceAddr := range traceAddrSeq {
		cf := allCfs[i]
		if cf.From == address || cf.To == address {
			fmt.Printf("Call from %s to %s: Trace Address = %v\n", cf.From, cf.To, traceAddr)
		}
	}
}

func TestCallTracer_DebugTraceBlock_OnlyTransactionOfInterest(t *testing.T) {
	jsonBytes, err := os.ReadFile("/Users/taratorio/debug_traceBlockByNumber_16494085_response.json")
	require.NoError(t, err)

	var response DebugTraceBlockResponse
	err = json.Unmarshal(jsonBytes, &response)
	require.NoError(t, err)

	var topLevelCfs []callFrame
	for _, txCf := range response.Result {
		if txCf.TxHash.String() == "0x536434786ace02697118c44abf2835f188bf79902807c61a523ca3a6200bc350" {
			topLevelCfs = append(topLevelCfs, txCf.TopLevelCallFrame)
			break
		}
	}
	require.Len(t, topLevelCfs, 1)
	var allCfs []callFrame
	GatherCallFrames(topLevelCfs[0], &allCfs)

	traceAddrSeq := BuildTraceAddrSeq(topLevelCfs)
	require.Equal(t, len(allCfs), len(traceAddrSeq))

	for i, traceAddr := range traceAddrSeq {
		cf := allCfs[i]
		fmt.Printf("Call from %s to %s: Trace Address = %v\n", cf.From, cf.To, traceAddr)
	}
}

type DebugTraceBlockResponse struct {
	Result []TxCallFrameResult `json:"result"`
}

func (r DebugTraceBlockResponse) TopLevelCallFrames() []callFrame {
	var cfs []callFrame
	for _, txRes := range r.Result {
		cfs = append(cfs, txRes.TopLevelCallFrame)
	}
	return cfs
}

func (r DebugTraceBlockResponse) AllCallFrames() []callFrame {
	var cfs []callFrame
	for _, txRes := range r.Result {
		GatherCallFrames(txRes.TopLevelCallFrame, &cfs)
	}
	return cfs
}

type TxCallFrameResult struct {
	TxHash            common.Hash `json:"txHash"`
	TopLevelCallFrame callFrame   `json:"result"`
}

func GatherCallFrames(cf callFrame, cfs *[]callFrame) {
	*cfs = append(*cfs, cf)
	for _, cf := range cf.Calls {
		GatherCallFrames(cf, cfs)
	}
}

func BuildTraceAddrSeq(callFrames []callFrame) [][]int {
	var traceAddrSeq [][]int
	for _, callFrame := range callFrames {
		BuildTraceAddrSeqRec(callFrame, nil, -1, &traceAddrSeq)
	}
	return traceAddrSeq
}

func BuildTraceAddrSeqRec(cf callFrame, parentTraceAddr []int, i int, traceAddrSeq *[][]int) {
	if i == -1 {
		*traceAddrSeq = append(*traceAddrSeq, []int{})
	} else {
		parentTraceAddr = append([]int{}, parentTraceAddr...)
		parentTraceAddr = append(parentTraceAddr, i)
		*traceAddrSeq = append(*traceAddrSeq, parentTraceAddr)
	}

	for i, cf := range cf.Calls {
		BuildTraceAddrSeqRec(cf, parentTraceAddr, i, traceAddrSeq)
	}
}
