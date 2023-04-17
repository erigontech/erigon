package instrumentation

// StoreDiff contains modified storage data
type StoreDiff struct {
	Location uint64 `json:"location"`
	Value    uint64 `json:"value"`
}
