package collector

import (
	"data-plane/probing"
	model "data-plane/telemetry/model"
)

// collectLink generates link congestion info from latest probe results
func collectLink() []model.LinkInfo {
	results := probing.GetLatestResults()
	var links []model.LinkInfo

	for target, r := range results {
		links = append(links, model.LinkInfo{
			TargetIP:   target,
			Target:     r.Target,
			PacketLoss: r.LossRate,
			//WeightedCache:  0,                                // Fill if cache data is available
			AverageLatency: float64(r.AvgRTT.Milliseconds()), // Milliseconds
			//BandwidthUsage: 0,                                // Fill if bandwidth data is available
		})
	}

	return links
}
