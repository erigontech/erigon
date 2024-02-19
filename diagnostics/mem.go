package diagnostics

import (
	"encoding/json"
	"github.com/shirou/gopsutil/v3/process"
	"net/http"
	"os"
)

func SetupMemAccess(metricsMux *http.ServeMux) {
	metricsMux.HandleFunc("/mem", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Content-Type", "application/json")
		writeMem(w)
	})
}

func writeMem(w http.ResponseWriter) {
	memStats, err := GetMemUsage()

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(memStats)
}

func GetMemUsage() (process.MemoryMapsStat, error) {
	pid := os.Getpid()
	proc, err := process.NewProcess(int32(pid))

	if err != nil {
		return process.MemoryMapsStat{}, err
	}

	memoryMaps, err := proc.MemoryMaps(true)

	if err != nil {
		return process.MemoryMapsStat{}, err
	}

	return (*memoryMaps)[0], nil
}
