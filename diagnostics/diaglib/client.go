// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package diaglib

import (
	"context"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/gorilla/websocket"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx"
)

type DiagnosticClient struct {
	ctx         context.Context
	db          kv.RwDB
	metricsMux  *http.ServeMux
	dataDirPath string
	speedTest   bool

	syncStages          []SyncStage
	syncStats           SyncStatistics
	BlockExecution      BlockEexcStatsData
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
	webseedsList        []string
	conn                *websocket.Conn
}

var (
	instance *DiagnosticClient
	once     sync.Once
)

// Client returns the singleton instance of DiagnosticClient
func Client() *DiagnosticClient {
	if instance == nil {
		return &DiagnosticClient{}
	}
	return instance
}

func NewDiagnosticClient(ctx context.Context, metricsMux *http.ServeMux, dataDirPath string, speedTest bool, webseedsList []string) (*DiagnosticClient, error) {
	dirPath := filepath.Join(dataDirPath, "diagnostics")
	db, err := createDb(ctx, dirPath)
	if err != nil {
		return nil, err
	}

	hInfo, ss, snpdwl, snpidx, snpfd := ReadSavedData(db)

	once.Do(func() {
		instance = &DiagnosticClient{
			ctx:         ctx,
			db:          db,
			metricsMux:  metricsMux,
			dataDirPath: dataDirPath,
			speedTest:   speedTest,
			syncStages:  ss,
			syncStats: SyncStatistics{
				SnapshotDownload: snpdwl,
				SnapshotIndexing: snpidx,
				SnapshotFillDB:   snpfd,
			},
			hardwareInfo:     hInfo,
			snapshotFileList: SnapshoFilesList{},
			bodies:           BodiesInfo{},
			resourcesUsage: ResourcesUsage{
				MemoryUsage: []MemoryStats{},
			},
			peersStats:   NewPeerStats(1000), // 1000 is the limit of peers; TODO: make it configurable through a flag
			webseedsList: webseedsList,
		}
	})

	return instance, nil
}

func createDb(ctx context.Context, dbDir string) (db kv.RwDB, err error) {
	db, err = mdbx.New(kv.DiagnosticsDB, log.New()).
		WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg { return kv.DiagnosticsTablesCfg }).
		GrowthStep(4 * datasize.MB).
		MapSize(16 * datasize.GB).
		PageSize(4 * datasize.KB).
		RoTxsLimiter(semaphore.NewWeighted(9_000)).
		Path(dbDir).
		Open(ctx)
	if err != nil {
		return nil, err
	}

	return db, nil
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

	d.setupTxPoolDiagnostics(rootCtx)

	d.runSaveProcess(rootCtx)

	//d.logDiagMsgs()
}

// Save diagnostic data by time interval to reduce save events
func (d *DiagnosticClient) runSaveProcess(rootCtx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				d.SaveData()
			case <-rootCtx.Done():
				d.SaveData()
				return
			}
		}
	}()
}

func (d *DiagnosticClient) SaveData() {
	d.mu.Lock()
	defer d.mu.Unlock()

	var funcs []func(tx kv.RwTx) error
	funcs = append(funcs, SnapshotDownloadUpdater(d.syncStats.SnapshotDownload), StagesListUpdater(d.syncStages), SnapshotIndexingUpdater(d.syncStats.SnapshotIndexing))

	err := d.db.Update(d.ctx, func(tx kv.RwTx) error {
		for _, updater := range funcs {
			updErr := updater(tx)
			if updErr != nil {
				return updErr
			}
		}

		return nil
	})

	if err != nil {
		log.Warn("Failed to save diagnostics data", "err", err)
	}
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

func ReadSavedData(db kv.RoDB) (hinfo HardwareInfo, ssinfo []SyncStage, snpdwl SnapshotDownloadStatistics, snpidx SnapshotIndexingStatistics, snpfd SnapshotFillDBStatistics) {
	var ramBytes []byte
	var cpuBytes []byte
	var diskBytes []byte
	var ssinfoData []byte
	var snpdwlData []byte
	var snpidxData []byte
	var snpfdData []byte
	var err error

	if err := db.View(context.Background(), func(tx kv.Tx) error {
		ramBytes, err = ReadRAMInfoFromTx(tx)
		if err != nil {
			return err
		}

		cpuBytes, err = ReadCPUInfoFromTx(tx)
		if err != nil {
			return err
		}

		diskBytes, err = ReadDiskInfoFromTx(tx)
		if err != nil {
			return err
		}

		ssinfoData, err = SyncStagesFromTX(tx)
		if err != nil {
			return err
		}

		snpdwlData, err = SnapshotDownloadInfoFromTx(tx)
		if err != nil {
			return err
		}

		snpidxData, err = SnapshotIndexingInfoFromTx(tx)
		if err != nil {
			return err
		}

		snpfdData, err = SnapshotFillDBInfoFromTx(tx)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		return HardwareInfo{}, []SyncStage{}, SnapshotDownloadStatistics{}, SnapshotIndexingStatistics{}, SnapshotFillDBStatistics{}
	}

	var ramInfo RAMInfo
	var cpuInfo []CPUInfo
	var diskInfo DiskInfo
	ParseData(ramBytes, &ramInfo)
	ParseData(cpuBytes, &cpuInfo)
	ParseData(diskBytes, &diskInfo)

	hinfo = HardwareInfo{
		RAM:  ramInfo,
		CPU:  cpuInfo,
		Disk: diskInfo,
	}

	ParseData(ssinfoData, &ssinfo)
	ParseData(snpdwlData, &snpdwl)
	ParseData(snpidxData, &snpidx)
	ParseData(snpfdData, &snpfd)

	return hinfo, ssinfo, snpdwl, snpidx, snpfd
}
