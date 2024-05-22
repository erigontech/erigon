package diagnostics

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/diskutils"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
)

func (d *DiagnosticClient) setupSysInfoDiagnostics() {
	sysInfo := GetSysInfo(d.dataDirPath)
	if err := d.db.Update(d.ctx, SystemInfoUpdater(sysInfo)); err != nil {
		fmt.Println("Error updating sysinfo")
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
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		infoBytes, err := tx.GetOne(kv.HardwareInfo, []byte("diagHardwareInfo"))

		if err != nil {
			return err
		}

		err = json.Unmarshal(infoBytes, &info)

		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		return HardwareInfo{}
	}
	return info
}

func SystemInfoUpdater(info HardwareInfo) func(tx kv.RwTx) error {
	return func(tx kv.RwTx) error {
		infoBytes, err := json.Marshal(info)

		if err != nil {
			return err
		}

		return tx.Put(kv.HardwareInfo, []byte("diagHardwareInfo"), infoBytes)
	}
}
