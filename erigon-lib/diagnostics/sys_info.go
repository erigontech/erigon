package diagnostics

import (
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/diskutils"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"
)

var (
	SystemRamInfoKey  = []byte("diagSystemRamInfo")
	SystemCpuInfoKey  = []byte("diagSystemCpuInfo")
	SystemDiskInfoKey = []byte("diagSystemDiskInfo")
)

func (d *DiagnosticClient) setupSysInfoDiagnostics() {
	sysInfo := GetSysInfo(d.dataDirPath)

	var funcs []func(tx kv.RwTx) error
	funcs = append(funcs, RAMInfoUpdater(sysInfo.RAM), CPUInfoUpdater(sysInfo.CPU), DiskInfoUpdater(sysInfo.Disk))

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
		log.Warn("[Diagnostics] Failed to update system info", "err", err)
	}

	d.mu.Lock()
	d.hardwareInfo = sysInfo
	d.mu.Unlock()
}

func (d *DiagnosticClient) HardwareInfo() HardwareInfo {
	return d.hardwareInfo
}

func findNodeDisk(dirPath string) string {
	mountPoint := diskutils.MountPointForDirPath(dirPath)

	return mountPoint
}

func GetSysInfo(dirPath string) HardwareInfo {
	nodeDisk := findNodeDisk(dirPath)

	ramInfo := GetRAMInfo()
	diskInfo := GetDiskInfo(nodeDisk)
	cpuInfo := GetCPUInfo()

	return HardwareInfo{
		RAM:  ramInfo,
		Disk: diskInfo,
		CPU:  cpuInfo,
	}
}

func GetRAMInfo() RAMInfo {
	totalRAM := uint64(0)
	freeRAM := uint64(0)

	vmStat, err := mem.VirtualMemory()
	if err == nil {
		totalRAM = vmStat.Total
		freeRAM = vmStat.Free
	}

	return RAMInfo{
		Total: totalRAM,
		Free:  freeRAM,
	}
}

func GetDiskInfo(nodeDisk string) DiskInfo {
	fsType := ""
	total := uint64(0)
	free := uint64(0)

	partitions, err := disk.Partitions(false)

	if err == nil {
		for _, partition := range partitions {
			if partition.Mountpoint == nodeDisk {
				iocounters, err := disk.Usage(partition.Mountpoint)
				if err == nil {
					fsType = partition.Fstype
					total = iocounters.Total
					free = iocounters.Free

					break
				}
			}
		}
	}

	return DiskInfo{
		FsType: fsType,
		Total:  total,
		Free:   free,
	}
}

func GetCPUInfo() CPUInfo {
	modelName := ""
	cores := 0
	mhz := float64(0)

	cpuInfo, err := cpu.Info()
	if err == nil {
		for _, info := range cpuInfo {
			modelName = info.ModelName
			cores = int(info.Cores)
			mhz = info.Mhz

			break
		}
	}

	return CPUInfo{
		ModelName: modelName,
		Cores:     cores,
		Mhz:       mhz,
	}
}

func ReadRAMInfoFromTx(tx kv.Tx) ([]byte, error) {
	bytes, err := ReadDataFromTable(tx, kv.DiagSystemInfo, SystemRamInfoKey)
	if err != nil {
		return nil, err
	}

	return common.CopyBytes(bytes), nil
}

func ReadCPUInfoFromTx(tx kv.Tx) ([]byte, error) {
	bytes, err := ReadDataFromTable(tx, kv.DiagSystemInfo, SystemCpuInfoKey)
	if err != nil {
		return nil, err
	}

	return common.CopyBytes(bytes), nil
}

func ReadDiskInfoFromTx(tx kv.Tx) ([]byte, error) {
	bytes, err := ReadDataFromTable(tx, kv.DiagSystemInfo, SystemDiskInfoKey)
	if err != nil {
		return nil, err
	}

	return common.CopyBytes(bytes), nil
}

func RAMInfoUpdater(info RAMInfo) func(tx kv.RwTx) error {
	return PutDataToTable(kv.DiagSystemInfo, SystemRamInfoKey, info)
}

func CPUInfoUpdater(info CPUInfo) func(tx kv.RwTx) error {
	return PutDataToTable(kv.DiagSystemInfo, SystemCpuInfoKey, info)
}

func DiskInfoUpdater(info DiskInfo) func(tx kv.RwTx) error {
	return PutDataToTable(kv.DiagSystemInfo, SystemDiskInfoKey, info)
}
