// Command sim runs the deadline-propagation simulation and writes the report
// consumed by the web UI.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"scriptkill/sim/internal/sim"
)

type report struct {
	Generated string      `json:"generated"`
	Seed      int64       `json:"seed"`
	Graph     *sim.Graph  `json:"graph"`
	Propagate sim.Metrics `json:"propagate"`
	Baseline  sim.Metrics `json:"baseline"`
	Compare   compare     `json:"compare"`
	Checks    checks      `json:"checks"`
}

type compare struct {
	WastedProcMs          float64 `json:"wasted_proc_ms"`
	WastedProcSavedMs     float64 `json:"wasted_proc_saved_ms"`
	WastedProcSavedPct    float64 `json:"wasted_proc_saved_pct"`
	RoomsWastedMs         float64 `json:"rooms_wasted_ms"`
	RoomsWastedSavedMs    float64 `json:"rooms_wasted_saved_ms"`
	RoomsWastedSavedPct   float64 `json:"rooms_wasted_saved_pct"`
	RoomMaxHoldMs         float64 `json:"room_max_hold_ms"`
	RoomMaxHoldBaselineMs float64 `json:"room_max_hold_baseline_ms"`
	StaleNotifies         int     `json:"stale_notifies"`
	StaleNotifiesBaseline int     `json:"stale_notifies_baseline"`
}

type checks struct {
	Propagate sim.CheckResult `json:"propagate"`
	Baseline  sim.CheckResult `json:"baseline"`
}

func main() {
	var (
		seed     = flag.Int64("seed", 20240517, "PRNG seed")
		requests = flag.Int("n", 200000, "number of client requests")
		rate     = flag.Float64("rate", 1200, "arrival rate (req/s)")
		timeout  = flag.Duration("timeout", 3*time.Second, "entry timeout")
		out      = flag.String("out", "web/report.json", "report path")
		checkDir = flag.String("checkdir", "results", "directory for offline check logs")
		skipBase = flag.Bool("skip-baseline", false, "skip the non-propagating run")
	)
	flag.Parse()

	g := sim.DefaultGraph()

	run := func(propagate bool, withCheck bool) (*sim.Metrics, *sim.CheckResult) {
		cfg := sim.DefaultConfig(propagate)
		cfg.Seed = *seed
		cfg.Requests = *requests
		cfg.RatePerSec = *rate
		cfg.EntryTimeout = *timeout

		s := sim.New(cfg, g)
		var writer *sim.CheckWriter
		var prefix string
		if withCheck {
			prefix = filepath.Join(*checkDir, mode(propagate))
			_ = os.MkdirAll(*checkDir, 0o755)
			w, err := sim.NewCheckWriter(prefix)
			if err != nil {
				fatal(err)
			}
			s.SetCheckWriter(w)
			writer = w
		}
		start := time.Now()
		m := s.Run()
		elapsed := time.Since(start)
		if writer != nil {
			if err := writer.Close(); err != nil {
				fatal(err)
			}
		}
		var res *sim.CheckResult
		if withCheck {
			var err error
			res, err = sim.Check(prefix, uint32((*timeout)/time.Millisecond))
			if err != nil {
				fatal(err)
			}
		}
		fmt.Printf("%-14s requests=%d spans=%d  elapsed=%s\n", mode(propagate), m.Requests, m.Spans, elapsed.Round(time.Millisecond))
		return m, res
	}

	propM, propCheck := run(true, true)
	var baseM sim.Metrics
	var baseCheck sim.CheckResult
	if !*skipBase {
		bm, bc := run(false, true)
		baseM = *bm
		baseCheck = *bc
	}

	rep := report{
		Generated: time.Now().Format(time.RFC3339),
		Seed:      *seed,
		Graph:     g,
		Propagate: *propM,
		Baseline:  baseM,
		Checks: checks{
			Propagate: *propCheck,
			Baseline:  baseCheck,
		},
	}
	rep.Compare.WastedProcMs = propM.WastedProc
	rep.Compare.RoomsWastedMs = propM.RoomsWastedMs
	rep.Compare.RoomMaxHoldMs = propM.RoomMaxHoldMs
	if !*skipBase {
		rep.Compare.WastedProcSavedMs = baseM.WastedProc - propM.WastedProc
		if baseM.WastedProc > 0 {
			rep.Compare.WastedProcSavedPct = 100 * (baseM.WastedProc - propM.WastedProc) / baseM.WastedProc
		}
		rep.Compare.RoomsWastedSavedMs = baseM.RoomsWastedMs - propM.RoomsWastedMs
		if baseM.RoomsWastedMs > 0 {
			rep.Compare.RoomsWastedSavedPct = 100 * (baseM.RoomsWastedMs - propM.RoomsWastedMs) / baseM.RoomsWastedMs
		}
		rep.Compare.RoomMaxHoldBaselineMs = baseM.RoomMaxHoldMs
		rep.Compare.StaleNotifies = propM.NotifyAfterGiveUp
		rep.Compare.StaleNotifiesBaseline = baseM.NotifyAfterGiveUp
	}

	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		fatal(err)
	}
	f, err := os.Create(*out)
	if err != nil {
		fatal(err)
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(&rep); err != nil {
		fatal(err)
	}
	if err := f.Close(); err != nil {
		fatal(err)
	}
	printSummary(rep, f.Name())
}

func mode(p bool) string {
	if p {
		return "propagate"
	}
	return "no-propagation"
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

func printSummary(r report, path string) {
	p, b := r.Propagate, r.Baseline
	fmt.Println()
	fmt.Println("== 结果 ==")
	fmt.Printf("%-26s %14s %14s\n", "", "传截止时间", "不传")
	row := func(name string, a, b float64, unit string) {
		fmt.Printf("%-26s %12.1f%s %12.1f%s\n", name, a, unit, b, unit)
	}
	row("浪费的处理时间", p.WastedProc/1000, b.WastedProc/1000, "s")
	row("总处理时间", p.ProcTotal/1000, b.ProcTotal/1000, "s")
	row("房间白锁时长", p.RoomsWastedMs/1000, b.RoomsWastedMs/1000, "s")
	row("单次房间最长占用", p.RoomMaxHoldMs/1000, b.RoomMaxHoldMs/1000, "s")
	fmt.Printf("%-26s %14d %14d\n", "过期后仍开始处理", p.StartedAfterDeadline, b.StartedAfterDeadline)
	fmt.Printf("%-26s %14d %14d\n", "客户端走后仍开始处理", p.StartedAfterGiveUp, b.StartedAfterGiveUp)
	fmt.Printf("%-26s %14d %14d\n", "过期后仍发出的通知", p.NotifyAfterGiveUp, b.NotifyAfterGiveUp)
	fmt.Printf("%-26s %14d %14d\n", "已取消但缺清理", p.CleanupMissed, b.CleanupMissed)
	fmt.Printf("%-26s %14d %14d\n", "后端到端 p99 (ms)", func() int { return int(p.Latency.P99) }(), func() int { return int(b.Latency.P99) }())
	fmt.Println()
	fmt.Printf("离线核对(传):   启动 %d, 过期后启动 %d, 客户端走后启动 %d, 清理 %d/%d 缺 %d 超限 %d, 异步过期 %d\n",
		r.Checks.Propagate.Starts, r.Checks.Propagate.StartedAfterDeadline, r.Checks.Propagate.StartedAfterClientLeft,
		r.Checks.Propagate.CleanupRan, r.Checks.Propagate.CleanupNeeded, r.Checks.Propagate.CleanupMissing,
		r.Checks.Propagate.CleanupOverCap, r.Checks.Propagate.AsyncDroppedStale)
	fmt.Printf("离线核对(不传): 启动 %d, 过期后启动 %d, 客户端走后启动 %d\n",
		r.Checks.Baseline.Starts, r.Checks.Baseline.StartedAfterDeadline, r.Checks.Baseline.StartedAfterClientLeft)
	fmt.Printf("报告写入 %s\n", path)
}
