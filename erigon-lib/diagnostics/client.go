package diagnostics

import (
	"net/http"
	"sync"

	"github.com/ledgerwatch/erigon-lib/common"
)

type DiagnosticClient struct {
	metricsMux  *http.ServeMux
	dataDirPath string

	syncStats           SyncStatistics
	snapshotFileList    SnapshoFilesList
	mu                  sync.Mutex
	headerMutex         sync.Mutex
	hardwareInfo        HardwareInfo
	peersStats          *PeerStats
	headers             Headers
	bodies              BodiesInfo
	bodiesMutex         sync.Mutex
	resourcesUsage      ResourcesUsage
	resourcesUsageMutex sync.Mutex
	networkSpeed        NetworkSpeedTestResult
	networkSpeedMutex   sync.Mutex
}

func NewDiagnosticClient(metricsMux *http.ServeMux, dataDirPath string) *DiagnosticClient {
	return &DiagnosticClient{
		metricsMux:       metricsMux,
		dataDirPath:      dataDirPath,
		syncStats:        SyncStatistics{},
		hardwareInfo:     HardwareInfo{},
		snapshotFileList: SnapshoFilesList{},
		bodies:           BodiesInfo{},
		resourcesUsage: ResourcesUsage{
			MemoryUsage: []MemoryStats{},
		},
		peersStats: NewPeerStats(1000), // 1000 is the limit of peers; TODO: make it configurable through a flag
	}
}

func (d *DiagnosticClient) Setup() {

	rootCtx, _ := common.RootContext()

	d.setupSnapshotDiagnostics(rootCtx)
	d.setupStagesDiagnostics(rootCtx)
	d.setupSysInfoDiagnostics()
	d.setupNetworkDiagnostics(rootCtx)
	d.setupBlockExecutionDiagnostics(rootCtx)
	d.setupHeadersDiagnostics(rootCtx)
	d.setupBodiesDiagnostics(rootCtx)
	d.setupResourcesUsageDiagnostics(rootCtx)
	d.setupSpeedtestDiagnostics(rootCtx)

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
