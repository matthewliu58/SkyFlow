package collector

import (
	"data-plane/probing"
	model "data-plane/telemetry/model"
)

// BuildLinkCongestion 从最新探测结果生成链路拥塞信息
func collectLink() []model.LinkInfo {
	results := probing.GetLatestResults()
	var links []model.LinkInfo

	for target, r := range results {
		links = append(links, model.LinkInfo{
			TargetIP:   target,
			Target:     r.Target,
			PacketLoss: r.LossRate,
			//WeightedCache:  0,                                // 如果有缓存数据可以填入
			AverageLatency: float64(r.AvgRTT.Milliseconds()), // 毫秒
			//BandwidthUsage: 0,                                // 如果有带宽数据可以填入
		})
	}

	return links
}
