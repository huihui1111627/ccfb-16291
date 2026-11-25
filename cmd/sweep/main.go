// Command sweep precomputes the parameter explorer used by the web page: a
// grid of entry timeouts and latency scales, each run in both modes, so the UI
// can show how the wasted work responds without a server round trip.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"scriptkill/sim/internal/sim"
)

type rowMetrics struct {
	WastedProcMs       float64 `json:"wasted_proc_ms"`
	RoomsWastedMs      float64 `json:"rooms_wasted_ms"`
	StartedAfterGiveUp int     `json:"started_after_give_up"`
	OrphanBookings     int     `json:"orphan_bookings"`
	SuccessRate        float64 `json:"success_rate"`
	P99Ms              float64 `json:"p99_ms"`
}

type sweepRow struct {
	EntryTimeoutMs int        `json:"entry_timeout_ms"`
	LatScale       float64    `json:"lat_scale"`
	LockScale      float64    `json:"lock_scale"`
	PayScale       float64    `json:"pay_scale"`
	Propagate      rowMetrics `json:"propagate"`
	Baseline       rowMetrics `json:"baseline"`
}

type sweepFile struct {
	Seed        int64      `json:"seed"`
	Requests    int        `json:"requests"`
	RatePerSec  float64    `json:"rate_per_sec"`
	GeneratedAt string     `json:"generated_at"`
	Timeouts    []int      `json:"entry_timeouts_ms"`
	LatScales   []float64  `json:"lat_scales"`
	Rows        []sweepRow `json:"rows"`
}

func summarize(m *sim.Metrics) rowMetrics {
	total := m.Requests
	if total == 0 {
		total = 1
	}
	return rowMetrics{
		WastedProcMs:       m.WastedProc,
		RoomsWastedMs:      m.RoomsWastedMs,
		StartedAfterGiveUp: m.StartedAfterGiveUp,
		OrphanBookings:     m.OrphanBookings,
		SuccessRate:        100 * float64(m.Outcomes["success"]) / float64(total),
		P99Ms:              m.Latency.P99,
	}
}

func main() {
	seed := flag.Int64("seed", 20240517, "PRNG seed")
	requests := flag.Int("n", 8000, "requests per sweep point")
	rate := flag.Float64("rate", 1500, "arrival rate")
	out := flag.String("out", "web/sweep.json", "output path")
	flag.Parse()

	timeouts := []int{400, 600, 800, 1000, 1500, 2000, 3000, 4000, 6000, 8000}
	latScales := []float64{0.5, 0.75, 1.0, 1.5, 2.0, 3.0}

	type job struct {
		to, lat float64
		toMs    int
	}
	var jobs []job
	for _, to := range timeouts {
		for _, ls := range latScales {
			jobs = append(jobs, job{to: float64(to), lat: ls, toMs: to})
		}
	}

	results := make([]sweepRow, len(jobs))
	var wg sync.WaitGroup
	sem := make(chan struct{}, 4)
	for i, j := range jobs {
		wg.Add(1)
		go func(i int, j job) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			run := func(prop bool) *sim.Metrics {
				cfg := sim.DefaultConfig(prop)
				cfg.Seed = *seed
				cfg.Requests = *requests
				cfg.RatePerSec = *rate
				cfg.EntryTimeout = time.Duration(j.toMs) * time.Millisecond
				cfg.ServiceScale = map[string]float64{
					"lock-service": j.lat,
					"payment":      j.lat,
					"pay-gateway":  j.lat,
					"bank-sim":     j.lat,
				}
				return sim.New(cfg, sim.DefaultGraph()).Run()
			}
			// Run the two modes in parallel as well.
			var pm, bm *sim.Metrics
			var wg2 sync.WaitGroup
			wg2.Add(2)
			go func() { defer wg2.Done(); pm = run(true) }()
			go func() { defer wg2.Done(); bm = run(false) }()
			wg2.Wait()
			results[i] = sweepRow{
				EntryTimeoutMs: j.toMs,
				LatScale:       j.lat,
				LockScale:      j.lat,
				PayScale:       j.lat,
				Propagate:      summarize(pm),
				Baseline:       summarize(bm),
			}
			fmt.Printf("完成 timeout=%dms lat=%.2f\n", j.toMs, j.lat)
		}(i, j)
	}
	wg.Wait()

	f := sweepFile{
		Seed:        *seed,
		Requests:    *requests,
		RatePerSec:  *rate,
		GeneratedAt: time.Now().Format(time.RFC3339),
		Timeouts:    timeouts,
		LatScales:   latScales,
		Rows:        results,
	}
	if err := os.MkdirAll(dir(*out), 0o755); err != nil {
		fail(err)
	}
	fh, err := os.Create(*out)
	if err != nil {
		fail(err)
	}
	enc := json.NewEncoder(fh)
	enc.SetIndent("", "  ")
	if err := enc.Encode(&f); err != nil {
		fail(err)
	}
	if err := fh.Close(); err != nil {
		fail(err)
	}
	fmt.Printf("扫描结果写入 %s（%d 个点）\n", *out, len(results))
}

func dir(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			return p[:i]
		}
	}
	return "."
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
