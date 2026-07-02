package collector

import (
	model "data-plane/telemetry/model"
	"sort"

	"github.com/shirou/gopsutil/v3/process"
)

// collectProcess collects process info (active count, TOP3 CPU/memory processes)
func collectProcess() (model.ProcessInfo, error) {
	// 1. Get all processes
	processes, err := process.Processes()
	if err != nil {
		return model.ProcessInfo{}, err
	}
	activeCount := len(processes)

	// 2. Filter TOP3 CPU processes
	cpuProcesses := make([]model.ProcessDetail, 0)
	for _, p := range processes {
		name, _ := p.Name()
		cpu, _ := p.CPUPercent()
		if name == "" || cpu == 0 {
			continue
		}
		cpuProcesses = append(cpuProcesses, model.ProcessDetail{
			PID:   int(p.Pid),
			Name:  name,
			Usage: cpu,
		})
	}
	// Sort by CPU usage
	sort.Slice(cpuProcesses, func(i, j int) bool {
		return cpuProcesses[i].Usage > cpuProcesses[j].Usage
	})
	// Take top 3
	if len(cpuProcesses) > 3 {
		cpuProcesses = cpuProcesses[:3]
	}

	// 3. Filter TOP3 memory processes
	memProcesses := make([]model.ProcessDetail, 0)
	for _, p := range processes {
		name, _ := p.Name()
		mem, _ := p.MemoryPercent()
		if name == "" || mem == 0 {
			continue
		}
		memProcesses = append(memProcesses, model.ProcessDetail{
			PID:   int(p.Pid),
			Name:  name,
			Usage: float64(mem),
		})
	}
	// Sort by memory usage
	sort.Slice(memProcesses, func(i, j int) bool {
		return memProcesses[i].Usage > memProcesses[j].Usage
	})
	// Take top 3
	if len(memProcesses) > 3 {
		memProcesses = memProcesses[:3]
	}

	return model.ProcessInfo{
		ActiveCount: activeCount,
		TopCPU:      cpuProcesses,
		TopMem:      memProcesses,
	}, nil
}
