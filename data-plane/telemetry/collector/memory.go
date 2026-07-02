package collector

import (
	model "data-plane/telemetry/model"
	"github.com/shirou/gopsutil/v3/mem"
)

// collectMemory collects memory info
func collectMemory() (model.MemoryInfo, error) {
	v, err := mem.VirtualMemory()
	if err != nil {
		return model.MemoryInfo{}, err
	}

	return model.MemoryInfo{
		Total: v.Total,
		Used:  v.Used,
		Free:  v.Free,
		Usage: v.UsedPercent,
	}, nil
}
