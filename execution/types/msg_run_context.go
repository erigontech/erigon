package types

import "github.com/erigontech/erigon/arb/ethdb/wasmdb"

type MessageRunMode uint8

const (
	MessageCommitMode MessageRunMode = iota
	MessageGasEstimationMode
	MessageEthcallMode
	MessageReplayMode
	MessageRecordingMode
)

// these message modes are executed onchain so cannot make any gas shortcuts
func (m MessageRunMode) ExecutedOnChain() bool { // can use isFree for that??
	return m == MessageCommitMode || m == MessageReplayMode
}

type MessageRunContext struct {
	runMode MessageRunMode

	wasmCacheTag uint32
	wasmTargets  []wasmdb.WasmTarget
}

func NewMessageCommitContext(wasmTargets []wasmdb.WasmTarget) *MessageRunContext {
	if len(wasmTargets) == 0 {
		wasmTargets = []wasmdb.WasmTarget{wasmdb.LocalTarget()}
	}
	return &MessageRunContext{
		runMode:      MessageCommitMode,
		wasmCacheTag: 1,
		wasmTargets:  wasmTargets,
	}
}

func NewMessageReplayContext() *MessageRunContext {
	return &MessageRunContext{
		runMode:     MessageReplayMode,
		wasmTargets: []wasmdb.WasmTarget{wasmdb.LocalTarget()},
	}
}

func NewMessageRecordingContext(wasmTargets []wasmdb.WasmTarget) *MessageRunContext {
	if len(wasmTargets) == 0 {
		wasmTargets = []wasmdb.WasmTarget{wasmdb.LocalTarget()}
	}
	return &MessageRunContext{
		runMode:     MessageRecordingMode,
		wasmTargets: wasmTargets,
	}
}

func NewMessagePrefetchContext() *MessageRunContext {
	return NewMessageReplayContext()
}

func NewMessageEthcallContext() *MessageRunContext {
	return &MessageRunContext{
		runMode:     MessageEthcallMode,
		wasmTargets: []wasmdb.WasmTarget{wasmdb.LocalTarget()},
	}
}

func NewMessageGasEstimationContext() *MessageRunContext {
	return &MessageRunContext{
		runMode:     MessageGasEstimationMode,
		wasmTargets: []wasmdb.WasmTarget{wasmdb.LocalTarget()},
	}
}

func (c *MessageRunContext) IsCommitMode() bool {
	return c.runMode == MessageCommitMode
}

// these message modes are executed onchain so cannot make any gas shortcuts
func (c *MessageRunContext) IsExecutedOnChain() bool {
	return c.runMode == MessageCommitMode || c.runMode == MessageReplayMode || c.runMode == MessageRecordingMode
}

func (c *MessageRunContext) IsGasEstimation() bool {
	return c.runMode == MessageGasEstimationMode
}

func (c *MessageRunContext) IsNonMutating() bool {
	return c.runMode == MessageGasEstimationMode || c.runMode == MessageEthcallMode
}

func (c *MessageRunContext) IsEthcall() bool {
	return c.runMode == MessageEthcallMode
}

func (c *MessageRunContext) IsRecording() bool {
	return c.runMode == MessageRecordingMode
}

func (c *MessageRunContext) WasmCacheTag() uint32 {
	return c.wasmCacheTag
}

func (c *MessageRunContext) WasmTargets() []wasmdb.WasmTarget {
	return c.wasmTargets
}

func (c *MessageRunContext) RunModeMetricName() string {
	switch c.runMode {
	case MessageCommitMode:
		return "commit_runmode"
	case MessageGasEstimationMode:
		return "gas_estimation_runmode"
	case MessageEthcallMode:
		return "eth_call_runmode"
	case MessageReplayMode:
		return "replay_runmode"
	case MessageRecordingMode:
		return "recording_runmode"
	default:
		return "unknown_runmode"
	}
}
