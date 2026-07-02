package collector

import (
	model "data-plane/telemetry/model"
	"github.com/shirou/gopsutil/v3/host"
)

// collectOS collects system basic info
func collectOS() (model.OSInfo, error) {
	info, err := host.Info()
	if err != nil {
		return model.OSInfo{}, err
	}

	return model.OSInfo{
		OSName:    info.OS,
		KernelVer: info.KernelVersion,
		Hostname:  info.Hostname,
		BootTime:  int64(info.BootTime),
	}, nil
}
