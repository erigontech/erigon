package diagnostics

import (
	"context"
	"encoding/json"

	"github.com/ledgerwatch/erigon-lib/diskutils"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
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
	data := ReadInfoBytes(db, SystemRamInfoKey)
	var info RAMInfo
	err := json.Unmarshal(data, &info)

	if err != nil {
		return RAMInfo{}
	} else {
		return info
	}
}

func ReadCPUInfo(db kv.RoDB) CPUInfo {
	data := ReadInfoBytes(db, SystemCpuInfoKey)
	var info CPUInfo
	err := json.Unmarshal(data, &info)

	if err != nil {
		return CPUInfo{}
	} else {
		return info
	}
}

func ReadDickInfo(db kv.RoDB) DiskInfo {
	data := ReadInfoBytes(db, SystemDiskInfoKey)
	var info DiskInfo
	err := json.Unmarshal(data, &info)

	if err != nil {
		return DiskInfo{}
	} else {
		return info
	}
}

func ReadInfoBytes(db kv.RoDB, key []byte) (data []byte) {
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		bytes, err := tx.GetOne(kv.DiagSystemInfo, key)

		if err != nil {
			return err
		}

		data = bytes

		return nil
	}); err != nil {
		return []byte{}
	}
	return data
}

func RAMInfoUpdater(info RAMInfo) func(tx kv.RwTx) error {
	return UpdateDiagTable(kv.DiagSystemInfo, SystemRamInfoKey, info)
}

func CPUInfoUpdater(info CPUInfo) func(tx kv.RwTx) error {
	return UpdateDiagTable(kv.DiagSystemInfo, SystemCpuInfoKey, info)
}

func DiskInfoUpdater(info DiskInfo) func(tx kv.RwTx) error {
	return UpdateDiagTable(kv.DiagSystemInfo, SystemDiskInfoKey, info)
}

func UpdateDiagTable(table string, key []byte, info any) func(tx kv.RwTx) error {
	return func(tx kv.RwTx) error {
		infoBytes, err := json.Marshal(info)

		if err != nil {
			return err
		}

		return tx.Put(table, key, infoBytes)
	}
}
