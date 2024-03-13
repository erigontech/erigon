package diagnostics

import (
	"net/http"
	"sync"
)

type DiagnosticClient struct {
	metricsMux  *http.ServeMux
	dataDirPath string

	syncStats        SyncStatistics
	snapshotFileList SnapshoFilesList
	mu               sync.Mutex
	hardwareInfo     HardwareInfo
	peersSyncMap     sync.Map
}

func NewDiagnosticClient(metricsMux *http.ServeMux, dataDirPath string) *DiagnosticClient {
	return &DiagnosticClient{
		metricsMux:       metricsMux,
		dataDirPath:      dataDirPath,
		syncStats:        SyncStatistics{},
		hardwareInfo:     HardwareInfo{},
		snapshotFileList: SnapshoFilesList{},
	}
}

func (d *DiagnosticClient) Setup() {
	d.setupSnapshotDiagnostics()
	d.setupStagesDiagnostics()
	d.setupSysInfoDiagnostics()
	d.setupNetworkDiagnostics()
	d.setupBlockExecutionDiagnostics()

	//d.logDiagMsgs()
}

/*func (d *DiagnosticClient) logDiagMsgs() {
	ticker := time.NewTicker(20 * time.Second)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				d.logStr()
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()
}
func (d *DiagnosticClient) logStr() {
	d.mu.Lock()
	defer d.mu.Unlock()
	log.Info("SyncStatistics", "stats", interfaceToJSONString(d.syncStats))
}

func interfaceToJSONString(i interface{}) string {
	b, err := json.Marshal(i)
	if err != nil {
		return ""
	}
	return string(b)
}*/
