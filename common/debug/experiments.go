package debug

import (
	"os"
	"sync"
)

var gerEnv sync.Once
var DataLayoutExperiment bool
func IsDataLayoutExperiment() bool  {
	gerEnv.Do(func() {
		_,DataLayoutExperiment = os.LookupEnv("DATA_LAYOUT_EXPERIMENT")
	})
	return DataLayoutExperiment
}