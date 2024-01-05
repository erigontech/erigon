package handler

import "net/http"

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
