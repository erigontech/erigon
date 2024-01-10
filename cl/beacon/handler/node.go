package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
)

func (a *ApiHandler) GetEthV1NodeHealth(w http.ResponseWriter, r *http.Request) {
	syncingStatus, err := uint64FromQueryParams(r, "syncing_status")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	syncingCode := http.StatusOK
	if syncingStatus != nil {
		syncingCode = int(*syncingStatus)
	}
	if a.syncedData.Syncing() {
		w.WriteHeader(syncingCode)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (a *ApiHandler) GetEthV1NodeVersion(w http.ResponseWriter, r *http.Request) {
	// Get OS and Arch
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"data": map[string]interface{}{
			"version": fmt.Sprintf("Caplin/%s %s/%s", a.version, runtime.GOOS, runtime.GOARCH),
		},
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
