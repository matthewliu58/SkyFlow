package collector

import (
	model "data-plane/telemetry/model"

	"github.com/shirou/gopsutil/v3/disk"
)

// collectDisk collects root partition/system disk info
func collectDisk() (model.DiskInfo, error) {
	// Default root partition (Linux: /, Windows: C:\)
	path := "/"
	// Windows adaptation
	// if runtime.GOOS == "windows" {
	// 	path = "C:\\"
	// }

	stat, err := disk.Usage(path)
	if err != nil {
		return model.DiskInfo{}, err
	}

	return model.DiskInfo{
		Total: stat.Total,
		Used:  stat.Used,
		Free:  stat.Free,
		Usage: stat.UsedPercent,
		Path:  path,
	}, nil
}
