package probing

import (
	"context"
	"data-plane/util"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"
)

// Result stores probe results
type Result struct {
	Target   ProbeTask     `json:"target"`    // Probe target IP/host
	Attempts int           `json:"attempts"`  // Number of attempts
	Failures int           `json:"failures"`  // Number of failures
	LossRate float64       `json:"loss_rate"` // Packet loss rate
	AvgRTT   time.Duration `json:"avg_rtt"`   // Average RTT for successful connections
}

// Config probing configuration
type Config struct {
	Concurrency int           // Concurrency count
	Timeout     time.Duration // TCP Dial timeout
	Interval    time.Duration // Probe interval
	Attempts    int           // Number of attempts per probe round
	BufferSize  int           // Optional: channel buffer size (not used now)
}

// ----------------- Global storage for latest round results -----------------

var (
	mu            sync.RWMutex
	latestResults = make(map[string]Result)
)

// Update global latest results
func updateLatestResults(results []Result) {
	mu.Lock()
	defer mu.Unlock()
	for _, r := range results {
		latestResults[r.Target.IP] = r
	}
}

// External call: get latest probe results
func GetLatestResults() map[string]Result {
	mu.RLock()
	defer mu.RUnlock()

	copied := make(map[string]Result, len(latestResults))
	for k, v := range latestResults {
		copied[k] = v
	}
	return copied
}

// ----------------- Core periodic probing function -----------------

// StartProbePeriodically starts infinite periodic probing
// ctx passed by caller for stopping
// controlHost: probe task source API (returns target node list)
// cfg: configuration
// logger: logger
func StartProbePeriodically(ctx context.Context, controlHost string, cfg Config, pre string, logger *slog.Logger) {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 4
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 2 * time.Second
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 10 * time.Second
	}
	if cfg.Attempts <= 0 {
		cfg.Attempts = 5
	}

	logger.Info("StartProbePeriodically", slog.String("pre", pre))

	go func() {
		ticker := time.NewTicker(cfg.Interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				logger.Info("periodic probing stopped")
				return
			default:
			}

			pre_ := util.GenerateRandomLetters(5)

			// Get probe tasks
			targets, err := GetProbeTasks(pre, controlHost)
			if err != nil {
				logger.Error("get probe tasks failed", slog.Any("err", err))
				time.Sleep(time.Second) // Prevent rapid retry in case of error
				continue
			}
			logger.Info("get probing tasks", slog.String("pre", pre_), slog.Any("targets", targets))

			// Execute one round of probing
			doProbeLossRTT(targets, cfg, pre, logger)

			// Wait for next period
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

// ----------------- Single round probing function -----------------

func doProbeLossRTT(targets []ProbeTask, cfg Config, pre string, logger *slog.Logger) {
	jobs := make(chan ProbeTask)
	var wg sync.WaitGroup
	roundResults := make([]Result, 0, len(targets))
	var roundMu sync.Mutex

	// worker
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for target := range jobs {
				failures := 0
				var totalRTT time.Duration
				successes := 0

				dialer := net.Dialer{
					Timeout: cfg.Timeout,
				}

				for a := 0; a < cfg.Attempts; a++ {
					start := time.Now()
					conn, err := dialer.Dial("tcp", target.IP+":"+strconv.Itoa(target.Port))
					rtt := time.Since(start)

					if err != nil {
						// Key: distinguish error types
						if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
							// Network unreachable / packet loss
							failures++
							continue
						}

						// Non-timeout (usually RST)
						// Network is reachable, but no service on port
						successes++
						totalRTT += rtt
						continue
					}

					// Connected successfully
					successes++
					totalRTT += rtt
					conn.Close()
				}

				avgRTT := time.Duration(0)
				if successes > 0 {
					avgRTT = totalRTT / time.Duration(successes)
				}

				result := Result{
					Target:   target,
					Attempts: cfg.Attempts,
					Failures: failures,
					LossRate: float64(failures) / float64(cfg.Attempts),
					AvgRTT:   avgRTT,
				}

				logger.Info(
					"probe result", slog.String("pre", pre),
					slog.String("ip", result.Target.IP),
					slog.Int("port", result.Target.Port),
					slog.String("provider", result.Target.Provider),
					slog.String("target_type", result.Target.TargetType),
					slog.Any("result", result),
				)

				// Collect to current round results
				roundMu.Lock()
				roundResults = append(roundResults, result)
				roundMu.Unlock()
			}
		}()
	}

	// Dispatch tasks
	go func() {
		for _, t := range targets {
			jobs <- t
		}
		close(jobs)
	}()

	wg.Wait()

	// Update global latest results
	updateLatestResults(roundResults)
}
