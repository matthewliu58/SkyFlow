package collector

import (
	model "data-plane/telemetry/model"
	"github.com/shirou/gopsutil/v3/host"
)

// collectOS 采集系统基础信息
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
