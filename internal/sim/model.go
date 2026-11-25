package sim

import (
	"math"
	"time"
)

// DistKind selects a latency distribution.
type DistKind string

const (
	DistConst     DistKind = "const"
	DistUniform   DistKind = "uniform"
	DistLognormal DistKind = "lognormal"
)

// Dist is a latency distribution in milliseconds.
type Dist struct {
	Kind   DistKind `json:"kind"`
	Min    float64  `json:"min_ms,omitempty"`
	Max    float64  `json:"max_ms,omitempty"`
	Median float64  `json:"median_ms,omitempty"`
	Sigma  float64  `json:"sigma,omitempty"`
}

func Const(ms float64) Dist       { return Dist{Kind: DistConst, Median: ms} }
func Uniform(lo, hi float64) Dist { return Dist{Kind: DistUniform, Min: lo, Max: hi} }
func LogNormal(median, sigma float64) Dist {
	return Dist{Kind: DistLognormal, Median: median, Sigma: sigma}
}

// MedianMs is the representative value used when deciding whether a call is
// worth sending.
func (d Dist) MedianMs() float64 {
	if d.Kind == DistUniform {
		return (d.Min + d.Max) / 2
	}
	return d.Median
}

// Scaled multiplies the location parameter, keeping the shape. The UI uses it
// to retune one service without changing its variance profile.
func (d Dist) Scaled(f float64) Dist {
	n := d
	switch d.Kind {
	case DistUniform:
		n.Min *= f
		n.Max *= f
	default:
		n.Median *= f
	}
	return n
}

// EdgeKind distinguishes a synchronous RPC from an asynchronous publish.
type EdgeKind string

const (
	EdgeSync  EdgeKind = "sync"
	EdgeAsync EdgeKind = "async"
)

// Edge is a call from a parent node to a child. Children sharing a Phase run
// concurrently; phases run in order, so a pipeline is a chain of phases.
type Edge struct {
	To    int      `json:"to"`
	Phase int      `json:"phase"`
	Net   Dist     `json:"net"`
	Kind  EdgeKind `json:"kind"`
}

// Node is one service in the call graph.
type Node struct {
	ID      int    `json:"id"`
	Name    string `json:"name"`
	Workers int    `json:"workers"`

	Lat     Dist    `json:"processing"`
	MinProc float64 `json:"min_proc_ms"` // minimum useful processing time for a downstream call

	// LocalBudgetMs is an optional, strictly stricter budget of this service's
	// own. It can only tighten what the caller gave it.
	LocalBudgetMs float64 `json:"local_budget_ms,omitempty"`

	// Cleanup means a cancelled handler runs a compensating action (release a
	// room, void a hold) after cancellation, with its own bounded budget.
	Cleanup      bool    `json:"cleanup"`
	CleanupMinMs float64 `json:"cleanup_min_ms,omitempty"`
	CleanupMaxMs float64 `json:"cleanup_max_ms,omitempty"`

	// LockRooms makes the handler take a room from the pool while it runs.
	LockRooms int `json:"lock_rooms,omitempty"`

	// Notifier marks the services on the notification branch, so the report
	// can separate notification traffic from all async traffic.
	Notifier bool `json:"notifier,omitempty"`

	// Commits marks a handler whose completion is a durable business commit
	// (charging a card, writing the order). Reaching it after the client gave
	// up is an orphan booking.
	Commits bool `json:"commits,omitempty"`

	BizFailPct  float64 `json:"biz_fail_pct,omitempty"`
	BizFailNote string  `json:"biz_fail_note,omitempty"`

	Children []Edge `json:"children"`
}

// Graph is the topology. A node's index is its id.
type Graph struct {
	Nodes  []*Node        `json:"nodes"`
	ByName map[string]int `json:"-"`
	Entry  int            `json:"entry"`
}

func (g *Graph) Add(n *Node) int {
	n.ID = len(g.Nodes)
	g.Nodes = append(g.Nodes, n)
	g.ByName[n.Name] = n.ID
	return n.ID
}

func (g *Graph) id(name string) int {
	id, ok := g.ByName[name]
	if !ok {
		panic("unknown service: " + name)
	}
	return id
}

func (g *Graph) Connect(parent, child string, phase int, kind EdgeKind, net Dist) {
	p := g.Nodes[g.id(parent)]
	p.Children = append(p.Children, Edge{To: g.id(child), Phase: phase, Net: net, Kind: kind})
}

// DefaultLockTTL is how long a room stays locked without an explicit release.
const DefaultLockTTL = 10 * time.Second

// DefaultGraph is the murder-mystery booking flow: 37 services, a five-deep
// synchronous pipeline, fan-out at several levels, a lock holder, and an
// asynchronous notification branch.
func DefaultGraph() *Graph {
	g := &Graph{ByName: map[string]int{}}

	svc := func(name string, workers int, p Dist, opts ...func(*Node)) {
		n := &Node{Name: name, Workers: workers, Lat: p, MinProc: math.Max(0.2, p.MedianMs()*0.25)}
		for _, o := range opts {
			o(n)
		}
		g.Add(n)
	}
	minProc := func(ms float64) func(*Node) { return func(n *Node) { n.MinProc = ms } }
	local := func(ms float64) func(*Node) { return func(n *Node) { n.LocalBudgetMs = ms } }
	cleanup := func(lo, hi float64) func(*Node) {
		return func(n *Node) { n.Cleanup, n.CleanupMinMs, n.CleanupMaxMs = true, lo, hi }
	}
	locks := func(rooms int) func(*Node) { return func(n *Node) { n.LockRooms = rooms } }
	fail := func(pct float64, note string) func(*Node) {
		return func(n *Node) { n.BizFailPct, n.BizFailNote = pct, note }
	}

	svc("gateway", 160, LogNormal(1.8, 0.5), cleanup(5, 20))
	svc("rate-limiter", 128, Const(0.6), minProc(0.2))
	svc("auth", 96, LogNormal(7, 0.6), minProc(3))
	svc("user-profile", 64, LogNormal(11, 0.7), minProc(4))
	svc("booking", 140, LogNormal(4, 0.5), cleanup(10, 40))
	svc("room-search", 64, LogNormal(22, 0.8), minProc(8), local(450))
	svc("search-index", 48, LogNormal(14, 0.9), minProc(4))
	svc("room-availability", 48, LogNormal(17, 0.9), minProc(6))
	svc("seat-map", 40, LogNormal(9, 0.7), minProc(3))
	svc("inventory-cache", 64, LogNormal(5, 0.6), minProc(2))
	svc("lock-service", 56, LogNormal(38, 0.9), minProc(12),
		cleanup(8, 30), locks(1), func(n *Node) { n.Commits = true },
		fail(4, "房间已被其他订单锁定"))
	svc("pricing", 56, LogNormal(26, 0.8), minProc(8))
	svc("price-engine", 44, LogNormal(36, 0.9), minProc(10))
	svc("promo-rules", 36, LogNormal(21, 0.8), minProc(6))
	svc("tax-service", 36, LogNormal(16, 0.8), minProc(5))
	svc("discount-cache", 48, LogNormal(4, 0.5), minProc(1))
	svc("coupon", 52, LogNormal(30, 0.9), minProc(10), cleanup(8, 30), local(700))
	svc("wallet", 44, LogNormal(27, 0.9), minProc(8), cleanup(8, 30))
	svc("ledger", 36, LogNormal(20, 0.8), minProc(6))
	svc("coupon-db", 48, LogNormal(10, 0.7), minProc(3))
	svc("fraud-check", 32, LogNormal(25, 1.0), minProc(8), fail(1.0, "风控拒绝"))
	svc("risk-score", 32, LogNormal(18, 1.1), minProc(6))
	svc("order-write", 48, LogNormal(23, 0.8), minProc(8), cleanup(8, 25),
		func(n *Node) { n.Commits = true })
	svc("order-db", 64, LogNormal(13, 0.7), minProc(4))
	svc("payment", 52, LogNormal(50, 0.9), minProc(15), cleanup(12, 45), local(900))
	svc("pay-gateway", 48, LogNormal(64, 1.0), minProc(20), cleanup(10, 40), local(700),
		func(n *Node) { n.Commits = true }, fail(3, "银行卡被拒"))
	svc("bank-sim", 72, LogNormal(90, 1.1), minProc(30))
	svc("currency-service", 32, LogNormal(10, 0.7), minProc(3))
	svc("payment-db", 56, LogNormal(16, 0.7), minProc(5))
	svc("audit-log", 40, LogNormal(4, 0.6), minProc(1))
	svc("notify-service", 40, LogNormal(3, 0.5), minProc(1), cleanup(5, 15),
		func(n *Node) { n.Notifier = true })
	svc("notifier-worker", 40, LogNormal(18, 0.9), minProc(5), cleanup(5, 20), local(500),
		func(n *Node) { n.Notifier = true })
	svc("sms-provider", 32, LogNormal(80, 1.0), minProc(20), func(n *Node) { n.Notifier = true })
	svc("push-provider", 32, LogNormal(35, 0.9), minProc(10), func(n *Node) { n.Notifier = true })
	svc("email-provider", 24, LogNormal(55, 1.0), minProc(15), func(n *Node) { n.Notifier = true })
	svc("metrics-collector", 64, Const(2.0), minProc(0.5))
	svc("geo-service", 32, LogNormal(12, 0.8), minProc(3))

	lan := LogNormal(1.8, 0.5) // intra-datacenter RPC
	wan := LogNormal(12, 0.8)  // third-party call
	mq := LogNormal(1500, 0.8) // message broker delivery (batched, heavy tail)

	// gateway: cheap guards first, then the booking pipeline.
	g.Connect("gateway", "rate-limiter", 0, EdgeSync, lan)
	g.Connect("gateway", "auth", 1, EdgeSync, lan)
	g.Connect("gateway", "booking", 2, EdgeSync, lan)
	g.Connect("gateway", "metrics-collector", 0, EdgeAsync, lan)
	g.Connect("auth", "user-profile", 0, EdgeSync, lan)

	// booking is the pipeline: search, then lock, then price+coupon, then
	// write, then pay, then notify.
	g.Connect("booking", "room-search", 0, EdgeSync, lan)
	g.Connect("booking", "lock-service", 1, EdgeSync, lan)
	g.Connect("booking", "pricing", 2, EdgeSync, lan)
	g.Connect("booking", "coupon", 2, EdgeSync, lan)
	g.Connect("booking", "order-write", 3, EdgeSync, lan)
	g.Connect("booking", "payment", 4, EdgeSync, lan)
	g.Connect("booking", "notify-service", 5, EdgeAsync, lan)
	g.Connect("booking", "audit-log", 5, EdgeAsync, lan)

	g.Connect("room-search", "search-index", 0, EdgeSync, lan)
	g.Connect("room-search", "room-availability", 1, EdgeSync, lan)
	g.Connect("room-availability", "seat-map", 0, EdgeSync, lan)
	g.Connect("room-availability", "inventory-cache", 1, EdgeSync, lan)

	g.Connect("lock-service", "inventory-cache", 0, EdgeSync, lan)

	g.Connect("pricing", "discount-cache", 0, EdgeSync, lan)
	g.Connect("pricing", "price-engine", 1, EdgeSync, lan)
	g.Connect("price-engine", "promo-rules", 0, EdgeSync, lan)
	g.Connect("price-engine", "tax-service", 0, EdgeSync, lan)
	g.Connect("tax-service", "geo-service", 0, EdgeSync, lan)

	g.Connect("coupon", "coupon-db", 0, EdgeSync, lan)
	g.Connect("coupon", "wallet", 1, EdgeSync, lan)
	g.Connect("wallet", "ledger", 0, EdgeSync, lan)
	g.Connect("wallet", "fraud-check", 1, EdgeSync, lan)
	g.Connect("fraud-check", "risk-score", 0, EdgeSync, lan)

	g.Connect("order-write", "inventory-cache", 0, EdgeSync, lan)
	g.Connect("order-write", "order-db", 1, EdgeSync, lan)

	g.Connect("payment", "payment-db", 0, EdgeSync, lan)
	g.Connect("payment", "pay-gateway", 1, EdgeSync, lan)
	g.Connect("pay-gateway", "currency-service", 0, EdgeSync, wan)
	g.Connect("pay-gateway", "bank-sim", 1, EdgeSync, wan)

	// The notification branch crosses a real asynchronous boundary.
	g.Connect("notify-service", "notifier-worker", 0, EdgeAsync, mq)
	g.Connect("notifier-worker", "sms-provider", 0, EdgeSync, wan)
	g.Connect("notifier-worker", "push-provider", 0, EdgeSync, wan)
	g.Connect("notifier-worker", "email-provider", 0, EdgeSync, wan)

	g.Entry = g.id("gateway")
	return g
}
