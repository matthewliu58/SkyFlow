package collector

import (
	model "data-plane/telemetry/model"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/load"
)

// collectCPU collects CPU info
// collectCPU collects CPU info (compatible with gopsutil v3+ cross-platform)
func collectCPU() (model.CPUInfo, error) {
	// 1. Get CPU core counts
	cpuCounts, err := cpu.Counts(true) // Logical core count
	if err != nil {
		return model.CPUInfo{}, err
	}
	physicalCounts, err := cpu.Counts(false) // Physical core count
	if err != nil {
		return model.CPUInfo{}, err
	}

	// 2. Get CPU usage (sample for 1 second)
	percent, err := cpu.Percent(1*time.Second, false)
	if err != nil {
		return model.CPUInfo{}, err
	}
	usage := 0.0
	if len(percent) > 0 {
		usage = percent[0]
	}

	// 3. Get system load (Linux/macOS only, returns 0 on Windows)
	var load1Min float64 = 0.0
	loadStat, err := load.Avg() // v3 uses load.Avg() instead of cpu.LoadAvg()
	if err == nil {             // Only assign if no error (Windows may fail, use 0)
		load1Min = loadStat.Load1
	}

	return model.CPUInfo{
		PhysicalCore: physicalCounts,
		LogicalCore:  cpuCounts,
		Usage:        usage,
		Load1Min:     load1Min, // 1-minute load average
	}, nil
}
