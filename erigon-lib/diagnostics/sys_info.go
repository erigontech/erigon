package diagnostics

import (
	"encoding/json"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"

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
	if err := d.db.Update(d.ctx, RAMInfoUpdater(sysInfo.RAM)); err != nil {
		log.Error("[Diagnostics] Failed to update RAM info", "err", err)
	}

	if err := d.db.Update(d.ctx, CPUInfoUpdater(sysInfo.CPU)); err != nil {
		log.Error("[Diagnostics] Failed to update CPU info", "err", err)
	}

	if err := d.db.Update(d.ctx, DiskInfoUpdater(sysInfo.Disk)); err != nil {
		log.Error("[Diagnostics] Failed to update Disk info", "err", err)
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

func ReadSysInfo(db kv.RoDB) (info HardwareInfo) {
	ram := ReadRAMInfo(db)
	cpu := ReadCPUInfo(db)
	disk := ReadDickInfo(db)

	return HardwareInfo{
		RAM:  ram,
		CPU:  cpu,
		Disk: disk,
	}
}

func ReadRAMInfo(db kv.RoDB) RAMInfo {
	data := ReadDataFromTable(db, kv.DiagSystemInfo, SystemRamInfoKey)

	if len(data) == 0 {
		return RAMInfo{}
	}

	var info RAMInfo
	err := json.Unmarshal(data, &info)

	if err != nil {
		log.Error("[Diagnostics] Failed to read RAM info", "err", err)
		return RAMInfo{}
	} else {
		return info
	}
}

func ReadCPUInfo(db kv.RoDB) CPUInfo {
	data := ReadDataFromTable(db, kv.DiagSystemInfo, SystemCpuInfoKey)

	if len(data) == 0 {
		return CPUInfo{}
	}

	var info CPUInfo
	err := json.Unmarshal(data, &info)

	if err != nil {
		log.Error("[Diagnostics] Failed to read CPU info", "err", err)
		return CPUInfo{}
	} else {
		return info
	}
}

func ReadDickInfo(db kv.RoDB) DiskInfo {
	data := ReadDataFromTable(db, kv.DiagSystemInfo, SystemDiskInfoKey)

	if len(data) == 0 {
		return DiskInfo{}
	}

	var info DiskInfo
	err := json.Unmarshal(data, &info)

	if err != nil {
		log.Error("[Diagnostics] Failed to read Disk info", "err", err)
		return DiskInfo{}
	} else {
		return info
	}
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
