package validatorapi

import (
	"net/http"

	"github.com/gfx-labs/sse"
)

func (v *ValidatorApiHandler) GetEthV1Events(w http.ResponseWriter, r *http.Request) {
	sink, err := sse.DefaultUpgrader.Upgrade(w, r)
	if err != nil {
		// OK to ignore this error.
		return
	}
	topics := r.URL.Query()["topics"]
	for _, topic := range topics {
		sink.Encode(&sse.Event{
			Event: []byte(topic),
			Data:  nil,
		})
		// OK to ignore this error. maybe should log it later
	}
}
