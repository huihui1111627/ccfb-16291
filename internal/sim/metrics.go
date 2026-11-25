package sim

import (
	"sort"
	"time"
)

// hist is a deterministic, bounded latency histogram with reservoir sampling.
type hist struct {
	cap     int
	n       int
	sum     time.Duration
	min     time.Duration
	max     time.Duration
	samples []time.Duration
}

func newHist(capacity int) *hist {
	return &hist{cap: capacity, samples: make([]time.Duration, 0, 4096)}
}

func (h *hist) Add(d time.Duration) {
	if d < 0 {
		d = 0
	}
	h.n++
	h.sum += d
	if h.n == 1 || d < h.min {
		h.min = d
	}
	if d > h.max {
		h.max = d
	}
	if len(h.samples) < h.cap {
		h.samples = append(h.samples, d)
	} else {
		h.samples[h.n%h.cap] = d
	}
}

func (h *hist) Percentile(p float64) time.Duration {
	if len(h.samples) == 0 {
		return 0
	}
	cp := make([]time.Duration, len(h.samples))
	copy(cp, h.samples)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	idx := int(p * float64(len(cp)-1))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(cp) {
		idx = len(cp) - 1
	}
	return cp[idx]
}

func (h *hist) Mean() time.Duration {
	if h.n == 0 {
		return 0
	}
	return h.sum / time.Duration(h.n)
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// Summary is the JSON-friendly view of a histogram.
type Summary struct {
	Count int     `json:"count"`
	Mean  float64 `json:"mean_ms"`
	P50   float64 `json:"p50_ms"`
	P90   float64 `json:"p90_ms"`
	P99   float64 `json:"p99_ms"`
	Max   float64 `json:"max_ms"`
}

func (h *hist) Summary() Summary {
	return Summary{
		Count: h.n,
		Mean:  ms(h.Mean()),
		P50:   ms(h.Percentile(0.50)),
		P90:   ms(h.Percentile(0.90)),
		P99:   ms(h.Percentile(0.99)),
		Max:   ms(h.max),
	}
}

// ServiceMetrics aggregates one service.
type ServiceMetrics struct {
	Name string `json:"name"`

	Arrivals       int `json:"arrivals"`
	Starts         int `json:"starts"`
	Done           int `json:"done"`
	Failed         int `json:"failed"`
	Cancelled      int `json:"cancelled"`
	Gated          int `json:"gated"`           // never sent because the budget did not fit
	QueueExpired   int `json:"queue_expired"`   // dequeued after the deadline, never ran
	QueueCancelled int `json:"queue_cancelled"` // dropped from the queue by an upstream cancel

	ProcMs   float64 `json:"proc_ms"`
	WastedMs float64 `json:"wasted_proc_ms"`
	Segments int     `json:"segments"`

	CleanupDue int `json:"cleanup_due"`
	CleanupRan int `json:"cleanup_ran"`

	RoomWastedMs  float64 `json:"room_wasted_ms,omitempty"`
	OrphanCommits int     `json:"orphan_commits"`

	QueueDepthMax int     `json:"queue_depth_max"`
	QueueWait     Summary `json:"queue_wait"`
	Service       Summary `json:"service_time"`
	Utilization   float64 `json:"utilization"`
}

// CancelOrigin rolls up "who cancelled what".
type CancelOrigin struct {
	Kind  string `json:"kind"`
	Host  string `json:"host"`
	Note  string `json:"note,omitempty"`
	Count int    `json:"count"`
}

// TraceNode is one span of a sampled call tree. Times are milliseconds
// relative to the client request start.
type TraceNode struct {
	ID      string `json:"id"`
	Parent  string `json:"parent,omitempty"`
	Service string `json:"service"`
	Kind    string `json:"kind"`
	State   string `json:"state"`
	Depth   int    `json:"depth"`
	Link    string `json:"link"`

	StartMs float64 `json:"start_ms"`
	EndMs   float64 `json:"end_ms"`
	QueueMs float64 `json:"queue_ms"`
	ProcMs  float64 `json:"proc_ms"`

	HasDeadline      bool    `json:"has_deadline"`
	RemainingStartMs float64 `json:"remaining_start_ms"`
	RemainingEndMs   float64 `json:"remaining_end_ms"`
	DeadlineMs       float64 `json:"deadline_ms"`

	CancelKind string   `json:"cancel_kind,omitempty"`
	CancelHost string   `json:"cancel_host,omitempty"`
	Lineage    []string `json:"lineage,omitempty"`
	Gated      bool     `json:"gated,omitempty"`

	Note     string       `json:"note,omitempty"`
	Cleanup  *CleanupJSON `json:"cleanup,omitempty"`
	LockRoom *int         `json:"lock_room,omitempty"`
}

// CleanupJSON reports a compensating action.
type CleanupJSON struct {
	Needed     bool    `json:"needed"`
	Ran        bool    `json:"ran"`
	StartMs    float64 `json:"start_ms"`
	DurationMs float64 `json:"duration_ms"`
	Capped     bool    `json:"capped"`
}

// TraceSample is one request's full call tree.
type TraceSample struct {
	ID             uint32      `json:"id"`
	Outcome        string      `json:"outcome"`
	ArrivalMs      float64     `json:"arrival_ms"`
	GiveUpMs       float64     `json:"give_up_ms"`
	EndToEndMs     float64     `json:"end_to_end_ms"`
	EntryTimeoutMs float64     `json:"entry_timeout_ms"`
	Spans          []TraceNode `json:"spans"`
	ActiveSpans    int         `json:"active_spans"`
	MaxRemainingMs float64     `json:"max_remaining_ms"`
	MinRemainingMs float64     `json:"min_remaining_ms"`
}

// Metrics is the full run report.
type Metrics struct {
	Mode             string  `json:"mode"`
	Seed             int64   `json:"seed"`
	Requests         int     `json:"requests"`
	RatePerSec       float64 `json:"rate_per_sec"`
	EntryTimeoutMs   float64 `json:"entry_timeout_ms"`
	SimulatedSeconds float64 `json:"simulated_seconds"`
	Services         int     `json:"services"`

	Outcomes map[string]int `json:"outcomes"`

	Spans         int `json:"spans"`
	SpanArrivals  int `json:"span_arrivals"`
	SpanStarts    int `json:"span_starts"`
	SpanDone      int `json:"span_done"`
	SpanFailed    int `json:"span_failed"`
	SpanCancelled int `json:"span_cancelled"`
	SpanDropped   int `json:"span_dropped"`
	SpanGated     int `json:"span_gated"`

	ProcTotal  float64 `json:"proc_total_ms"`
	WastedProc float64 `json:"wasted_proc_ms"`
	ProcUseful float64 `json:"proc_useful_ms"`

	WastedProcP99Ms float64 `json:"wasted_proc_p99_ms"`

	// Invariants, checked online and again offline from the record file.
	StartedAfterDeadline int `json:"started_after_deadline"`
	StartedAfterGiveUp   int `json:"started_after_give_up"`
	SentAfterDeadline    int `json:"sent_after_deadline"`

	CleanupRequired  int     `json:"cleanup_required"`
	CleanupRan       int     `json:"cleanup_ran"`
	CleanupCompleted int     `json:"cleanup_completed"`
	CleanupMissed    int     `json:"cleanup_missed"`
	CleanupOverCap   int     `json:"cleanup_over_cap"`
	CleanupMaxMs     float64 `json:"cleanup_max_ms"`
	CleanupTotalMs   float64 `json:"cleanup_total_ms"`

	RoomsAcquired   int     `json:"rooms_acquired"`
	RoomsHeldMs     float64 `json:"rooms_held_ms"`
	RoomsWastedMs   float64 `json:"rooms_wasted_ms"`
	RoomsTTL        int     `json:"rooms_ttl_expiry"`
	RoomsMaxHeld    int     `json:"rooms_max_held"`
	RoomMaxHoldMs   float64 `json:"room_max_hold_ms"`
	RoomWastedP99Ms float64 `json:"room_wasted_p99_ms"`
	RoomHeldP50Ms   float64 `json:"room_held_p50_ms"`
	RoomHeldP99Ms   float64 `json:"room_held_p99_ms"`
	OrphanBookings  int     `json:"orphan_bookings"`

	AsyncPublished     int     `json:"async_published"`
	AsyncDelivered     int     `json:"async_delivered"`
	AsyncDroppedStale  int     `json:"async_dropped_stale"`
	NotifyPublished    int     `json:"notify_published"`
	NotifyDelivered    int     `json:"notify_delivered"`
	NotifyDroppedStale int     `json:"notify_dropped_stale"`
	NotifyAfterGiveUp  int     `json:"notify_after_give_up"`
	NotifyMaxLateMs    float64 `json:"notify_max_late_ms"`

	Latency   Summary `json:"latency"`
	LatencyOK Summary `json:"latency_ok"`

	CancelBy   []CancelOrigin   `json:"cancel_by"`
	PerService []ServiceMetrics `json:"per_service"`

	Samples int            `json:"samples"`
	ByState map[string]int `json:"by_state"`
	Traces  []TraceSample  `json:"traces"`
}

func newMetrics(cfg Config, mode string) *Metrics {
	return &Metrics{
		Mode:           mode,
		Seed:           cfg.Seed,
		Requests:       cfg.Requests,
		RatePerSec:     cfg.RatePerSec,
		EntryTimeoutMs: ms(cfg.EntryTimeout),
		Outcomes:       map[string]int{},
	}
}

func sortCancel(v []CancelOrigin) {
	sort.Slice(v, func(i, j int) bool {
		if v[i].Count != v[j].Count {
			return v[i].Count > v[j].Count
		}
		if v[i].Kind != v[j].Kind {
			return v[i].Kind < v[j].Kind
		}
		return v[i].Host < v[j].Host
	})
}
