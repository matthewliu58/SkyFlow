package collector

import (
	model "data-plane/telemetry/model"
	"net/http"
	"strings"

	net1 "net"

	"github.com/shirou/gopsutil/v3/net"
)

// collectNetwork collects network info (public IP, port count, traffic)
func collectNetwork() (model.NetworkInfo, error) {
	// 1. Get public IP
	publicIP, err := getPublicIP()
	if err != nil {
		publicIP = "no-public-ip"
	}

	// 2. Get private IP (first non-loopback address)
	privateIP := getPrivateIP()

	// 3. Get port count
	ports, err := net.Connections("all")
	if err != nil {
		return model.NetworkInfo{}, err
	}
	portCount := len(ports)

	// 4. Get network interface traffic
	ioStat, err := net.IOCounters(true)
	if err != nil {
		return model.NetworkInfo{}, err
	}
	trafficIn := uint64(0)
	trafficOut := uint64(0)
	for _, stat := range ioStat {
		trafficIn += stat.BytesRecv
		trafficOut += stat.BytesSent
	}

	return model.NetworkInfo{
		PublicIP:   publicIP,
		PrivateIP:  privateIP,
		PortCount:  portCount,
		TrafficIn:  trafficIn,
		TrafficOut: trafficOut,
	}, nil
}

// getPublicIP gets public IP
func getPublicIP() (string, error) {
	resp, err := http.Get("https://icanhazip.com")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	buf := make([]byte, 1024)
	n, err := resp.Body.Read(buf)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(buf[:n])), nil
}

// getPrivateIP gets private IPv4 address (skip loopback, only first valid IPv4)
// Dependencies: standard library net + strings, no third-party dependencies
func getPrivateIP() string {
	// 1. Get all network interfaces (standard library net.Interface list)
	interfaces, err := net1.Interfaces()
	if err != nil {
		return ""
	}

	// 2. Iterate through each network interface
	for _, iface := range interfaces {
		// Skip loopback interface (FlagLoopback is standard library net constant)
		if iface.Flags&net1.FlagLoopback != 0 {
			continue
		}
		// Skip inactive interface (must have UP flag)
		if iface.Flags&net1.FlagUp == 0 {
			continue
		}

		// 3. Get all addresses for current interface (standard library net.Addr list)
		addrList, err := iface.Addrs()
		if err != nil {
			continue
		}

		// 4. Iterate through addresses, extract IPv4
		for _, addr := range addrList {
			// Type assertion: only handle IPNet type (exclude unix socket etc.)
			ipNet, ok := addr.(*net1.IPNet)
			if !ok || ipNet.IP.IsLoopback() {
				continue
			}

			// Extract pure IPv4 address (skip IPv6)
			ipv4 := ipNet.IP.To4()
			if ipv4 != nil {
				return ipv4.String() // Return first valid IPv4
			}
		}
	}

	// No valid private IP
	return ""
}
