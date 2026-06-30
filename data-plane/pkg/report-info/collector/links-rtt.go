package collector

import (
	model "data-plane/pkg/report-info"
	"data-plane/probing"
)

// BuildLinkCongestion 从最新探测结果生成链路拥塞信息
func BuildLinkCongestion() []model.LinkCongestionInfo {
	results := probing.GetLatestResults()
	var links []model.LinkCongestionInfo

	for target, r := range results {
		links = append(links, model.LinkCongestionInfo{
			TargetIP:       target,
			Target:         r.Target,
			PacketLoss:     r.LossRate,
			WeightedCache:  0,                                // 如果有缓存数据可以填入
			AverageLatency: float64(r.AvgRTT.Milliseconds()), // 毫秒
			BandwidthUsage: 0,                                // 如果有带宽数据可以填入
		})
	}

	return links
}
