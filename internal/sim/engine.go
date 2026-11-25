package sim

import (
	"container/heap"
	"fmt"
	"math"
	"time"

	"scriptkill/sim/internal/dlctx"
)

// Config controls one run.
type Config struct {
	Seed         int64              `json:"seed"`
	Requests     int                `json:"requests"`
	RatePerSec   float64            `json:"rate_per_sec"`
	EntryTimeout time.Duration      `json:"entry_timeout"`
	Propagate    bool               `json:"propagate"`
	WorkerScale  float64            `json:"worker_scale"`
	ServiceScale map[string]float64 `json:"service_scale,omitempty"`
	MaxSim       time.Duration      `json:"max_sim"`
	LockTTL      time.Duration      `json:"lock_ttl"`
}

func DefaultConfig(propagate bool) Config {
	return Config{
		Seed:         20240517,
		Requests:     200000,
		RatePerSec:   1200,
		EntryTimeout: 3 * time.Second,
		Propagate:    propagate,
		WorkerScale:  1,
		MaxSim:       6 * time.Hour,
		LockTTL:      DefaultLockTTL,
	}
}

// CleanupHardCap bounds every compensating action. Cleanup runs detached from
// the request budget — it must run after the request is already dead — but it
// is never unbounded.
const CleanupHardCap = 250 * time.Millisecond

// ---------------------------------------------------------------------------
// Clock
// ---------------------------------------------------------------------------

type simClock struct {
	mono time.Duration
	wall time.Time
}

func (c simClock) Mono() time.Duration { return c.mono }
func (c simClock) Wall() time.Time     { return c.wall }

// ---------------------------------------------------------------------------
// Span lifecycle
// ---------------------------------------------------------------------------

type spanState uint8

const (
	stInflight spanState = iota // on the wire
	stQueued
	stRunning
	stWaiting
	stDone
	stFailed
	stCancelled
	stDropped
	stGated
)

func (s spanState) terminal() bool { return s >= stDone }

func (s spanState) String() string {
	switch s {
	case stInflight:
		return "inflight"
	case stQueued:
		return "queued"
	case stRunning:
		return "running"
	case stWaiting:
		return "waiting"
	case stDone:
		return "done"
	case stFailed:
		return "failed"
	case stCancelled:
		return "cancelled"
	case stDropped:
		return "dropped"
	case stGated:
		return "gated"
	}
	return "?"
}

// hop is one RPC or message hop in the call graph.
type hop struct {
	id      int
	parent  *hop
	rec     *reqRecord
	node    int
	kind    EdgeKind
	depth   int
	link    string
	awaited bool

	d        *dlctx.Deadline
	hasDL    bool
	deadline time.Duration

	sendAt, arriveAt, startAt, endAt time.Duration
	procStart, procEnd               time.Duration

	linkNet time.Duration // one-way latency of the link into this hop

	state  spanState
	origin dlctx.Origin
	reason string

	procDur  time.Duration
	bizFail  bool
	failNote string

	cancelReq bool
	notified  bool // parent has been told about this hop's terminal state
	holdsCPU  bool
	cpuNode   int

	children []*hop
	batches  [][]Edge
	batch    int
	pending  int

	cleanupNeeded bool
	cleanupRan    bool
	cleanupAt     time.Duration
	cleanupDur    time.Duration

	lease      bool // baseline only: a local housekeeping budget, not a propagated deadline
	room       int  // -1 = none
	keepsLock  bool // this handler's room survives its own completion
	thrownAway bool // the segment's result is discarded because the request died
}

func (h *hop) name(g *Graph) string { return g.Nodes[h.node].Name }

// reqRecord is one client request.
type reqRecord struct {
	id       uint32
	arrival  time.Duration
	giveUpAt time.Duration
	root     *hop
	outcome  string
	done     bool
	settled  bool
	endedAt  time.Duration
	sampled  bool
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

type evKind uint8

const (
	evRequest evKind = iota
	evArrive
	evAsyncDeliver
	evWorkDone
	evDeadline
	evClientGiveUp
	evCleanupDone
	evRoomTTL
	evCancel
)

type event struct {
	at     time.Duration
	seq    uint64
	kind   evKind
	hop    *hop
	origin dlctx.Origin
	rec    *reqRecord
	wire   dlctx.Wire
	msg    *mqMessage
	hold   *roomHold
}

type eventHeap []*event

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].at != h[j].at {
		return h[i].at < h[j].at
	}
	return h[i].seq < h[j].seq
}
func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(x any)   { *h = append(*h, x.(*event)) }
func (h *eventHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return e
}

type mqMessage struct {
	env        dlctx.Envelope
	hop        *hop
	sentAt     time.Duration
	deadline   time.Duration
	hasDL      bool
	tombstoned bool
	tombOrigin dlctx.Origin
}

type roomHold struct {
	room     int
	hop      *hop
	acquired time.Duration
	ttlAt    time.Duration
	released bool
}

// ---------------------------------------------------------------------------
// Simulation
// ---------------------------------------------------------------------------

type Sim struct {
	cfg  Config
	g    *Graph
	now  time.Duration
	wall time.Time

	events eventHeap
	seq    uint64

	metrics  *Metrics
	svc      []*ServiceMetrics
	waits    []*hist
	procs    []*hist
	latAll   *hist
	latOK    *hist
	wasteP   *hist
	wasteR   *hist
	roomHeld *hist

	queues []*dlctx.Queue[*hop]
	busy   []int

	rooms    map[int]*roomHold
	nextRoom int

	nextHop  int
	cancelBy map[dlctx.Origin]int

	samples []*reqRecord
	msgs    map[*reqRecord][]*mqMessage

	cleanupDue int
	cleanupRan int

	// invariant counters
	startedAfterDeadline int
	startedAfterGiveUp   int
	sentAfterDeadline    int

	rec *CheckWriter
}

func New(cfg Config, g *Graph) *Sim {
	if cfg.WorkerScale <= 0 {
		cfg.WorkerScale = 1
	}
	if cfg.LockTTL <= 0 {
		cfg.LockTTL = DefaultLockTTL
	}
	if cfg.MaxSim <= 0 {
		cfg.MaxSim = 6 * time.Hour
	}
	mode := "propagate"
	if !cfg.Propagate {
		mode = "no_propagation"
	}
	s := &Sim{
		cfg:      cfg,
		g:        g,
		wall:     time.Unix(1_700_000_000, 0),
		metrics:  newMetrics(cfg, mode),
		rooms:    map[int]*roomHold{},
		cancelBy: map[dlctx.Origin]int{},
		msgs:     map[*reqRecord][]*mqMessage{},
	}
	s.metrics.Services = len(g.Nodes)
	s.svc = make([]*ServiceMetrics, len(g.Nodes))
	s.waits = make([]*hist, len(g.Nodes))
	s.procs = make([]*hist, len(g.Nodes))
	for i, n := range g.Nodes {
		s.svc[i] = &ServiceMetrics{Name: n.Name}
		s.waits[i] = newHist(20000)
		s.procs[i] = newHist(20000)
	}
	s.latAll = newHist(400000)
	s.latOK = newHist(400000)
	s.wasteP = newHist(40000)
	s.wasteR = newHist(40000)
	s.roomHeld = newHist(40000)
	s.queues = make([]*dlctx.Queue[*hop], len(g.Nodes))
	for i := range s.queues {
		s.queues[i] = dlctx.NewQueue[*hop]()
	}
	s.busy = make([]int, len(g.Nodes))
	return s
}

func (s *Sim) SetCheckWriter(w *CheckWriter) { s.rec = w }

// groupPhases splits a node's children into concurrent phases.
func groupPhases(edges []Edge) [][]Edge {
	maxPhase := -1
	for _, e := range edges {
		if e.Phase > maxPhase {
			maxPhase = e.Phase
		}
	}
	out := make([][]Edge, maxPhase+1)
	for _, e := range edges {
		out[e.Phase] = append(out[e.Phase], e)
	}
	// Drop empty phases while preserving order.
	trimmed := out[:0]
	for _, p := range out {
		if len(p) > 0 {
			trimmed = append(trimmed, p)
		}
	}
	return trimmed
}

func (s *Sim) workers(node int) int {
	n := s.g.Nodes[node]
	w := int(math.Round(float64(n.Workers) * s.cfg.WorkerScale))
	if w < 1 {
		w = 1
	}
	return w
}

func (s *Sim) push(e *event) {
	s.seq++
	e.seq = s.seq
	heap.Push(&s.events, e)
}

func (s *Sim) sample(d Dist, recID uint32, hopID int, purpose uint64) time.Duration {
	v := d.Sample(s.cfg.Seed, uint64(recID), uint64(hopID), purpose)
	if v < 0 {
		v = 0
	}
	return time.Duration(v * float64(time.Millisecond))
}

// hostSkew is a fixed per-host wall-clock offset in ±9s. It is display-only:
// no deadline arithmetic in the engine or dlctx reads it.
func (s *Sim) hostSkew(node int) time.Duration {
	v := U(s.cfg.Seed, 0x5C0FF, uint64(node))
	return time.Duration((v*2 - 1) * float64(9*time.Second))
}

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

func (s *Sim) Run() *Metrics {
	if s.cfg.RatePerSec <= 0 {
		s.cfg.RatePerSec = 1
	}
	interval := time.Duration(float64(time.Second) / s.cfg.RatePerSec)
	for i := 0; i < s.cfg.Requests; i++ {
		s.push(&event{at: time.Duration(i) * interval, kind: evRequest,
			rec: &reqRecord{id: uint32(i)}})
	}
	for len(s.events) > 0 {
		e := heap.Pop(&s.events).(*event)
		if e.at > s.cfg.MaxSim {
			break
		}
		if e.at > s.now {
			s.wall = s.wall.Add(e.at - s.now)
			s.now = e.at
		}
		s.dispatch(e)
	}
	s.finish()
	return s.metrics
}

func (s *Sim) dispatch(e *event) {
	switch e.kind {
	case evRequest:
		s.startRequest(e.rec)
	case evArrive:
		s.arrive(e.hop, e.wire)
	case evAsyncDeliver:
		s.deliverAsync(e.msg)
	case evWorkDone:
		s.workDone(e.hop)
	case evDeadline:
		s.deadlineFired(e.hop)
	case evClientGiveUp:
		s.clientGiveUp(e.rec)
	case evCleanupDone:
		s.cleanupDone(e.hop)
	case evRoomTTL:
		s.roomTTL(e)
	}
}

// ---------------------------------------------------------------------------
// Request entry
// ---------------------------------------------------------------------------

func (s *Sim) startRequest(rec *reqRecord) {
	rec.arrival = s.now
	rec.giveUpAt = s.now + s.cfg.EntryTimeout
	rec.sampled = s.shouldSample(rec.id)
	if rec.sampled {
		s.samples = append(s.samples, rec)
	}

	entry := s.g.Nodes[s.g.Entry]
	root := dlctx.Root(fmt.Sprintf("%d", rec.id), "root",
		"client>"+entry.Name, "client", s.cfg.EntryTimeout,
		simClock{mono: s.now, wall: s.wall})

	h := &hop{
		id:     s.nextHop,
		rec:    rec,
		node:   s.g.Entry,
		kind:   EdgeSync,
		link:   "client>" + entry.Name,
		d:      root,
		hasDL:  true,
		sendAt: s.now,
		state:  stInflight,
		room:   -1,
	}
	s.nextHop++
	h.deadline = root.DeadlineMono()
	rec.root = h

	// The client stops waiting at its deadline no matter what the server does.
	s.push(&event{at: rec.giveUpAt, kind: evClientGiveUp, rec: rec})
	s.sendSync(h, 900*time.Microsecond)
}

func (s *Sim) shouldSample(id uint32) bool {
	if id < 10 {
		return true
	}
	return U(s.cfg.Seed, 0x5A3, uint64(id)) < 0.0006
}

// ---------------------------------------------------------------------------
// Sending
// ---------------------------------------------------------------------------

// issueChild builds and sends a child. This is the only place implementing
// rule 2: a call is not sent unless the remaining budget covers transit plus
// the callee's declared minimum useful processing time.
func (s *Sim) issueChild(parent *hop, edge Edge, slot int) *hop {
	n := s.g.Nodes[edge.To]
	net := s.sample(edge.Net, parent.rec.id, parent.id, 0x10+uint64(slot))
	minProc := time.Duration(n.MinProc * float64(time.Millisecond))

	var d *dlctx.Deadline
	if s.cfg.Propagate {
		d = parent.d
		if parent.state == stCancelled && parent.origin.Valid() {
			d = d.Cancel(parent.origin)
		}
	} else {
		// Baseline: the hop carries no deadline at all. The client's patience
		// and every ancestor's cancellation stop at this boundary, which is
		// exactly the production bug being measured: later services keep
		// working on an order nobody is waiting for.
		d = dlctx.Absent()
	}

	h := &hop{
		id:      s.nextHop,
		parent:  parent,
		rec:     parent.rec,
		node:    edge.To,
		kind:    edge.Kind,
		depth:   parent.depth + 1,
		link:    parent.link + ">" + n.Name,
		d:       d,
		hasDL:   d.Has(),
		sendAt:  s.now,
		linkNet: net,
		awaited: edge.Kind == EdgeSync,
		state:   stInflight,
		room:    -1,
	}
	s.nextHop++
	if h.hasDL {
		h.deadline = d.DeadlineMono()
	}

	// Rule 2.
	if h.hasDL {
		rem := d.Remaining(s.now)
		if rem-net < minProc {
			h.endAt = s.now
			h.reason = fmt.Sprintf("预算剩 %s，不足 网络 %s + 下游最低处理 %s",
				rem.Round(time.Microsecond), net.Round(time.Microsecond), minProc.Round(time.Microsecond))
			s.transition(h, stGated)
			return h
		}
	}

	if edge.Kind == EdgeAsync {
		// Rule 7: stamp the deadline when publishing. If it is already gone,
		// the broker is never contacted.
		env, ok := dlctx.Publish(d, s.now, s.wall.Add(s.hostSkew(edge.To)))
		if !ok {
			h.endAt = s.now
			h.reason = "已过期，未投递到消息队列"
			s.transition(h, stGated)
			return h
		}
		m := &mqMessage{env: env, hop: h, sentAt: s.now, hasDL: h.hasDL, deadline: h.deadline}
		s.msgs[parent.rec] = append(s.msgs[parent.rec], m)
		s.metrics.AsyncPublished++
		if n.Notifier {
			s.metrics.NotifyPublished++
		}
		s.push(&event{at: s.now + net, kind: evAsyncDeliver, msg: m})
		return h
	}

	s.sendSync(h, net)
	return h
}

func (s *Sim) sendSync(h *hop, net time.Duration) {
	if h.hasDL && s.now >= h.deadline {
		s.sentAfterDeadline++
		s.metrics.SentAfterDeadline++
	}
	s.push(&event{at: s.now + net, kind: evArrive, hop: h, wire: dlctx.ToWire(h.d, s.now)})
}

// arrive is the receiver: re-derive the budget, then enqueue.
func (s *Sim) arrive(h *hop, wire dlctx.Wire) {
	if h.cancelReq || h.state.terminal() {
		s.transition(h, stCancelled)
		h.endAt = s.now
		if h.reason == "" {
			h.reason = "在途时已被取消，到达即丢弃"
		}
		s.notifyParent(h)
		return
	}
	h.arriveAt = s.now
	s.svc[h.node].Arrivals++
	s.metrics.SpanArrivals++

	if s.cfg.Propagate {
		// Only durations are subtracted: transit on the shared monotonic
		// source. The receiver's (skewed) wall clock never enters the math.
		h.d = dlctx.FromWire(wire, s.now, h.name(s.g), s.wall.Add(s.hostSkew(h.node)))
	}
	// A service may enforce a budget stricter than the caller's, never looser.
	if n := s.g.Nodes[h.node]; s.cfg.Propagate && n.LocalBudgetMs > 0 {
		h.d = h.d.Tighten(n.Name, time.Duration(n.LocalBudgetMs*float64(time.Millisecond)),
			simClock{mono: s.now, wall: s.wall.Add(s.hostSkew(h.node))})
	}
	h.hasDL = h.d.Has()
	if h.hasDL {
		h.deadline = h.d.DeadlineMono()
	}

	if !s.cfg.Propagate {
		s.enqueueLocalLease(h)
		return
	}

	// Already dead on arrival: never queued, never started.
	if h.hasDL && s.now >= h.deadline {
		h.endAt = s.now
		h.reason = "到达时已过期，直接丢弃"
		s.transition(h, stDropped)
		s.svc[h.node].QueueExpired++
		s.notifyParent(h)
		return
	}

	s.queues[h.node].Push(h.d, int64(h.id), s.now, h)
	if q := s.queues[h.node]; q.Len() > s.svc[h.node].QueueDepthMax {
		s.svc[h.node].QueueDepthMax = q.Len()
	}
	if h.hasDL {
		s.push(&event{at: h.deadline, kind: evDeadline, hop: h})
	}
	s.pump(h.node)
}

// enqueueLocalLease is the baseline's receive path. The lease bounds how long
// this service will sit on the request; it knows nothing of the client. Its
// expiry does not cancel the caller: the caller of a baseline hop is not
// waiting on it.
func (s *Sim) enqueueLocalLease(h *hop) {
	// The service keeps its own housekeeping lease: it will not sit on a
	// request forever. This is how such systems really behave. The lease is
	// local: nothing an ancestor does can shorten it, and its expiry never
	// tells the caller anything.
	h.lease = true
	h.deadline = s.now + s.cfg.EntryTimeout
	leaseD := dlctx.Root(fmt.Sprintf("%d", h.rec.id), "lease", h.link, h.name(s.g),
		s.cfg.EntryTimeout, simClock{mono: s.now, wall: s.wall.Add(s.hostSkew(h.node))})
	s.transition(h, stQueued)
	s.queues[h.node].Push(leaseD, int64(h.id), s.now, h)
	if q := s.queues[h.node]; q.Len() > s.svc[h.node].QueueDepthMax {
		s.svc[h.node].QueueDepthMax = q.Len()
	}
	s.push(&event{at: h.deadline, kind: evDeadline, hop: h})
	s.pump(h.node)
}

// pump starts queued work while a worker is free, always taking the most
// urgent live entry and never starting an expired or cancelled one.
func (s *Sim) pump(node int) {
	q := s.queues[node]
	w := s.workers(node)
	for s.busy[node] < w {
		it, reason, ok := q.PopOne(s.now, func(it dlctx.Item[*hop]) bool {
			return it.Meta.state.terminal()
		})

		if !ok {
			switch reason {
			case dlctx.DropNone:
				return
			case dlctx.DropExpired:
				s.onQueueExpired(it.Meta)
			case dlctx.DropCancelled:
				// Already accounted when the cancel arrived.
			}
			continue
		}
		if !s.canStart(it.Meta) {
			s.onQueueExpired(it.Meta)
			continue
		}
		s.startHop(it.Meta)
	}
}

// canStart reports whether a dequeued hop has enough budget left to be worth
// starting. A service declares the minimum useful processing time it needs; if
// what remains is less, the entry is discarded at dequeue instead of burning a
// worker on work that cannot finish. Entries without a deadline always start.
func (s *Sim) canStart(h *hop) bool {
	if !h.hasDL || h.lease {
		return true
	}
	minProc := time.Duration(s.g.Nodes[h.node].MinProc * float64(time.Millisecond))
	return h.deadline-s.now >= minProc
}

func (s *Sim) onQueueExpired(h *hop) {
	if h.state.terminal() {
		return
	}
	h.endAt = s.now
	h.reason = "排队期间过期，出队即丢"
	s.transition(h, stDropped)
	s.svc[h.node].QueueExpired++
	s.notifyParent(h)
}

func (s *Sim) startHop(h *hop) {
	s.busy[h.node]++
	h.holdsCPU = true
	h.cpuNode = h.node
	s.transition(h, stRunning)
	h.startAt = s.now
	h.procStart = s.now
	s.svc[h.node].Starts++

	s.metrics.SpanStarts++
	if wait := s.now - h.arriveAt; wait > 0 {
		s.waits[h.node].Add(wait)
	}

	// The central invariant, checked exactly when work starts.
	if h.hasDL && s.now >= h.deadline {
		s.startedAfterDeadline++
		s.metrics.StartedAfterDeadline++
	}

	if s.now >= h.rec.giveUpAt {
		s.startedAfterGiveUp++
		s.metrics.StartedAfterGiveUp++
	}
	if s.rec != nil {
		s.rec.writeStart(startRec{
			req: h.rec.id, node: uint16(h.node), hop: uint32(h.id),
			startMs: uint32((s.now - h.rec.arrival) / time.Millisecond),
			dlMs:    uint32((h.deadline - h.rec.arrival) / time.Millisecond),
			hasDL:   h.hasDL,
		})
	}

	n := s.g.Nodes[h.node]
	dur := s.sample(n.Lat, h.rec.id, h.id, 0x21)
	h.procDur = dur
	if n.LockRooms > 0 {
		s.acquireRoom(h)
		h.keepsLock = true
	}

	finish := s.now + dur
	if n.BizFailPct > 0 && U(s.cfg.Seed, uint64(h.rec.id), uint64(h.id), 0x33) < n.BizFailPct/100 {
		h.bizFail = true
		h.failNote = n.BizFailNote
		finish = s.now + dur/2
	}
	// A running handler is interrupted exactly at its own deadline.
	if h.hasDL && h.deadline < finish {
		finish = h.deadline
	}
	s.push(&event{at: finish, kind: evWorkDone, hop: h})
}

// ---------------------------------------------------------------------------
// Handler progress
// ---------------------------------------------------------------------------

func (s *Sim) workDone(h *hop) {
	if h.state != stRunning {
		return
	}
	n := s.g.Nodes[h.node]

	if h.hasDL && s.now >= h.deadline {
		s.beginCancel(h, dlctx.Origin{Kind: dlctx.KindDeadline, Host: n.Name, Note: "本层处理预算耗尽"})
		return
	}

	s.releaseCPU(h)
	h.procEnd = s.now
	s.accountWork(h)

	if h.bizFail {
		h.endAt = s.now
		h.reason = h.failNote
		s.transition(h, stFailed)
		s.releaseRoom(h, "failed")
		s.notifyParent(h)
		s.pump(h.node)
		return
	}

	if len(n.Children) == 0 {
		s.complete(h)
		return
	}

	h.batches = groupPhases(n.Children)
	h.batch = 0
	s.transition(h, stWaiting)
	s.advance(h)
}

// advance issues the next phase of children and waits for its synchronous
// members. Async children are issued and never awaited.
func (s *Sim) advance(h *hop) {
	if h.state != stWaiting {
		return
	}
	for h.batch < len(h.batches) {
		batch := h.batches[h.batch]
		h.batch++
		pending := 0
		for i, edge := range batch {
			ch := s.issueChild(h, edge, h.batch*100+i)
			// A gated child is recorded on the tree too: "we decided not to
			// call this" is part of the request's story.
			h.children = append(h.children, ch)
			if ch.state == stGated {
				continue
			}
			if ch.awaited {
				pending++
			}
		}
		if pending == 0 {
			continue
		}
		h.pending = pending
		return
	}
	// Every phase is done; wait for outstanding async descendants only if
	// they were never issued? No: async fan-out is fire-and-forget.
	h.pending = 0
	s.complete(h)
}

func (s *Sim) complete(h *hop) {
	if h.state.terminal() {
		return
	}
	h.endAt = s.now
	if h.procEnd == 0 {
		h.procEnd = s.now
	}
	s.transition(h, stDone)
	if s.g.Nodes[h.node].Commits {
		s.noteCommit(h)
	}
	// A completed lock holder keeps the room: the business holds the room
	// until the order is paid. It is released by the payment success path or
	// by cleanup. Other handlers release on completion.
	if !h.keepsRoom() {
		s.releaseRoom(h, "done")
	}
	s.notifyParent(h)
	s.pump(h.node)
}

// ---------------------------------------------------------------------------
// Cancellation
// ---------------------------------------------------------------------------

// beginCancel starts a cancellation cascade. The decision point itself is
// cancelled at once; every descendant is cancelled by a signal that travels
// over its own link, so the cascade ripples down the same path the request
// took, one hop at a time.
func (s *Sim) beginCancel(h *hop, o dlctx.Origin) {
	if h == nil {
		return
	}
	s.applyCancel(h, o)
}

// requestCancel schedules a cancellation to arrive at h after its link latency.
func (s *Sim) requestCancel(h *hop, o dlctx.Origin) {
	if h == nil || h.state.terminal() {
		return
	}
	if h.linkNet <= 0 {
		s.applyCancel(h, o)
		return
	}
	s.push(&event{at: s.now + h.linkNet, kind: evCancel, hop: h, origin: o})
}

// applyCancel is what a hop does when the cancellation signal reaches it.
func (s *Sim) applyCancel(h *hop, o dlctx.Origin) {
	if h == nil || h.state.terminal() {
		return
	}
	switch h.state {
	case stInflight:
		h.cancelReq = true
		h.endAt = s.now
		h.origin = o
		s.transition(h, stCancelled)

	case stQueued:
		h.cancelReq = true
		h.endAt = s.now
		h.origin = o
		h.reason = "排队中被取消，直接丢弃"
		s.transition(h, stCancelled)
		s.svc[h.node].QueueCancelled++
		s.queues[h.node].Drop(int64(h.id))
		s.pump(h.node)

	case stRunning:
		h.endAt = s.now
		h.origin = o
		h.reason = "运行中被取消"
		h.thrownAway = !h.rec.done || h.rec.outcome != "success"
		s.transition(h, stCancelled)
		s.releaseCPU(h)
		h.procEnd = s.now
		s.accountWork(h)
		s.releaseRoom(h, "cancelled")
		s.cancelChildren(h, o)
		s.startCleanup(h)
		s.notifyParent(h)
		s.pump(h.node)

	case stWaiting:
		h.endAt = s.now
		h.origin = o
		h.reason = "等待子调用时被取消"
		s.transition(h, stCancelled)
		s.cancelChildren(h, o)
		s.startCleanup(h)
		s.notifyParent(h)
	}
}

func (s *Sim) cancelChildren(h *hop, o dlctx.Origin) {
	for _, c := range h.children {
		s.requestCancel(c, o)
	}
}

// startCleanup runs the compensating action for a service that needs one.
// Cleanup is detached from the cancelled request: it executes even though the
// deadline is gone, and it is bounded by CleanupHardCap.
func (s *Sim) startCleanup(h *hop) {
	n := s.g.Nodes[h.node]
	if !n.Cleanup || h.cleanupRan {
		return
	}
	h.cleanupRan = true
	h.cleanupNeeded = true
	s.cleanupDue++
	s.svc[h.node].CleanupDue++
	s.metrics.CleanupRequired++
	if s.rec != nil {
		s.rec.writeCancel(cancelRec{
			req: h.rec.id, node: uint16(h.node), hop: uint32(h.id),
			cancelMs:  uint32((s.now - h.rec.arrival) / time.Millisecond),
			needClean: true, kind: string(h.origin.Kind),
		})
	}

	dur := s.cleanupDuration(h)
	h.cleanupDur = dur
	h.cleanupAt = s.now
	s.push(&event{at: s.now + dur, kind: evCleanupDone, hop: h})
}

// cleanupDuration samples the compensating action's duration and clamps it to
// the hard cap. Cleanup is detached from the request budget but never unbounded.
func (s *Sim) cleanupDuration(h *hop) time.Duration {
	n := s.g.Nodes[h.node]
	lo, hi := n.CleanupMinMs, n.CleanupMaxMs
	if hi <= 0 {
		hi = lo
	}
	dur := s.sample(Dist{Kind: DistLognormal, Median: (lo + hi) / 2, Sigma: 0.3}, h.rec.id, h.id, 0x44)
	if lo > 0 && dur < time.Duration(lo*float64(time.Millisecond)) {
		dur = time.Duration(lo * float64(time.Millisecond))
	}
	if hi > 0 && dur > time.Duration(hi*float64(time.Millisecond)) {
		dur = time.Duration(hi * float64(time.Millisecond))
	}
	if dur > CleanupHardCap {
		dur = CleanupHardCap
	}
	return dur
}

func (s *Sim) cleanupDone(h *hop) {
	s.cleanupRan++
	s.metrics.CleanupRan++
	s.metrics.CleanupCompleted++
	s.svc[h.node].CleanupRan++
	d := float64(h.cleanupDur) / float64(time.Millisecond)
	s.metrics.CleanupTotalMs += d
	if d > s.metrics.CleanupMaxMs {
		s.metrics.CleanupMaxMs = d
	}
	if h.cleanupDur > CleanupHardCap {
		s.metrics.CleanupOverCap++
	}
	if s.rec != nil {
		s.rec.writeCleanup(cleanupRec{
			req: h.rec.id, node: uint16(h.node), hop: uint32(h.id),
			startMs: uint32((h.cleanupAt - h.rec.arrival) / time.Millisecond),
			endMs:   uint32((s.now - h.rec.arrival) / time.Millisecond),
			capMs:   uint32(CleanupHardCap / time.Millisecond),
		})
	}
	// Cleanup releases the room it was protecting.
	if h.room >= 0 {
		if hold, ok := s.rooms[h.room]; ok && hold.hop == h {
			delete(s.rooms, h.room)
			h.room = -1
			s.accountRoom(hold, s.now)
		}
	}
}

// deadlineFired is a hop's own budget ending while it still exists.
func (s *Sim) deadlineFired(h *hop) {
	if h.state.terminal() {
		return
	}
	n := s.g.Nodes[h.node]
	switch h.state {
	case stQueued:
		// The queue discards it at the next pop. Pumping is not enough: if
		// every worker is busy, nothing pops and the entry lingers past its
		// deadline. Remove it eagerly instead, so an expired request occupies
		// no queue slot and no future pop has to skip over it.
		if s.queues[h.node].Drop(int64(h.id)) {
			s.onQueueExpired(h)
		}
	case stRunning, stWaiting:
		if !s.cfg.Propagate {
			// The local lease expired. The service gives up on its own work,
			// but the caller never asked and is never told.
			s.beginCancel(h, dlctx.Origin{Kind: dlctx.KindDeadline, Host: n.Name, Note: "本地租约到期"})
			return
		}
		s.beginCancel(h, dlctx.Origin{Kind: dlctx.KindDeadline, Host: n.Name, Note: "本层预算耗尽"})
	case stInflight:
		// The receiver learns from the wire stamp when it arrives.
		h.cancelReq = true
	}
}

// clientGiveUp fires when the client's patience ends.
func (s *Sim) clientGiveUp(rec *reqRecord) {
	if rec.done {
		return
	}
	rec.done = true
	rec.outcome = "timeout"
	rec.endedAt = s.now
	s.metrics.Outcomes["timeout"]++
	s.latAll.Add(s.now - rec.arrival)

	if !s.cfg.Propagate {
		// The baseline: the caller gives up and every downstream service keeps
		// working, unaware. This is the bug the whole exercise is about.
		// Nobody tells the services, so nobody runs a compensating action.
		// The rooms are only reclaimed by the lease itself. That is the cost
		// being measured; settlement happens at TTL.
		return
	}
	s.beginCancel(rec.root, dlctx.Origin{Kind: dlctx.KindCaller, Host: "client", Note: "客户端入口超时"})
	// Cancellation ripples down. A lock holder that already finished its work
	// still owns a room, so settlement is driven by the request's death, not
	// by the leaf's state: every room reserved for this request is released by
	// the holder's bounded compensating action.
	s.settleRooms(rec, false)
}

// ---------------------------------------------------------------------------
// Async boundary
// ---------------------------------------------------------------------------

func (s *Sim) deliverAsync(m *mqMessage) {
	h := m.hop
	if h == nil {
		return
	}
	// The consumer re-derives the deadline. Cancellation crosses the boundary
	// as a tombstone (m.tombstoned) or, if it was lost, as an expired budget.
	d := dlctx.Consume(m.env, s.now, h.name(s.g), s.wall.Add(s.hostSkew(h.node)))
	expired := d.Cancelled() || d.Expired(s.now) || h.cancelReq || m.tombstoned
	if s.rec != nil {
		dl := uint32(0)
		if m.hasDL {
			dl = uint32((m.deadline - h.rec.arrival) / time.Millisecond)
		}
		s.rec.writeAsync(asyncRec{
			req: h.rec.id, node: uint16(h.node), hop: uint32(h.id),
			consumeMs: uint32((s.now - h.rec.arrival) / time.Millisecond),
			dlMs:      dl, hasDL: m.hasDL, dropped: expired,
			reason: staleReason(h, m),
		})
	}
	// A message for a request that has already been abandoned is stale even if
	// its own deadline has not elapsed: the order no longer exists.
	abandoned := h.rec.done && h.rec.outcome != "success"
	// A notifier consumer that reaches an abandoned order is about to message
	// somebody about a booking nobody wants. Count that intent, then decide
	// whether propagation lets us suppress it.
	if abandoned && s.g.Nodes[h.node].Notifier {
		s.metrics.NotifyAfterGiveUp++
		s.recordStaleNotify(h)
	}
	if abandoned {
		expired = true
	}
	if expired {
		h.endAt = s.now
		h.origin = d.Origin()
		if m.tombstoned {
			h.origin = m.tombOrigin
		}
		h.reason = "消息在队列中已过期或被取消，消费者直接丢弃"
		s.transition(h, stDropped)
		s.svc[h.node].QueueExpired++
		s.metrics.AsyncDroppedStale++
		if s.g.Nodes[h.node].Notifier {
			s.metrics.NotifyDroppedStale++
		}
		s.notifyParent(h)
		return
	}
	h.d = d
	h.hasDL = d.Has()
	if h.hasDL {
		h.deadline = d.DeadlineMono()
	}
	s.metrics.AsyncDelivered++
	if s.g.Nodes[h.node].Notifier {
		s.metrics.NotifyDelivered++
	}
	s.arrive(h, dlctx.ToWire(d, s.now))
}

// staleReason labels a dropped async message: either the request was already
// abandoned when it was consumed, or the message simply outlived its budget.
func staleReason(h *hop, m *mqMessage) string {
	if m.tombstoned {
		return "cancelled"
	}
	if h.rec.done && h.rec.outcome != "success" {
		return "stale_request"
	}
	return "expired"
}

func (s *Sim) recordStaleNotify(h *hop) {
	late := s.now - h.rec.giveUpAt
	if late <= 0 {
		return
	}
	if v := float64(late) / float64(time.Millisecond); v > s.metrics.NotifyMaxLateMs {
		s.metrics.NotifyMaxLateMs = v
	}
}

// tombstoneAsync marks queued messages for a cancelled subtree so that the
// consumer drops them even if the deadline somehow did not expire.
func (s *Sim) tombstoneAsync(h *hop, o dlctx.Origin) {
	for _, m := range s.msgs[h.rec] {
		if m.tombstoned || m.hop == nil {
			continue
		}
		if m.hop == h || isDescendant(m.hop, h) {
			m.tombstoned = true
			m.tombOrigin = o
		}
	}
}

func isDescendant(h, anc *hop) bool {
	for p := h.parent; p != nil; p = p.parent {
		if p == anc {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Parent notification
// ---------------------------------------------------------------------------

func (s *Sim) notifyParent(h *hop) {
	if h.notified {
		return
	}
	h.notified = true

	// Propagate cancellation across the async boundary for any message that
	// belongs to this subtree.
	if s.cfg.Propagate && h.state == stCancelled && h.origin.Valid() {
		s.tombstoneAsync(h, h.origin)
	}

	p := h.parent
	if p == nil {
		s.finishRequest(h)
		return
	}
	if !h.awaited || p.state != stWaiting {
		return
	}
	switch h.state {
	case stFailed, stCancelled, stDropped:
		// Fail fast: the parent can no longer succeed, so cancel every other
		// in-flight sibling right now.
		o := dlctx.Origin{Kind: dlctx.KindSibling, Host: p.name(s.g), Note: "兄弟调用失败"}
		p.pending = 0
		for _, c := range p.children {
			if c != h {
				s.requestCancel(c, o)
			}
		}
		s.failParent(p, "子调用失败: "+h.name(s.g))
	default:
		p.pending--
		if p.pending == 0 {
			s.pump(p.node)
			s.advance(p)
		}
	}
}

func (s *Sim) failParent(p *hop, note string) {
	if p.state.terminal() {
		return
	}
	p.endAt = s.now
	p.reason = note
	s.transition(p, stFailed)
	s.releaseRoom(p, "child_failed")
	s.startCleanup(p)
	s.notifyParent(p)
	s.pump(p.node)
}

func (s *Sim) finishRequest(h *hop) {
	rec := h.rec
	if rec.done {
		return
	}
	rec.done = true
	rec.endedAt = s.now
	s.latAll.Add(s.now - rec.arrival)
	if h.state == stDone {
		rec.outcome = "success"
		s.metrics.Outcomes["success"]++
		s.latOK.Add(s.now - rec.arrival)
		s.settleRooms(rec, true)
		return
	}
	rec.outcome = "failed"
	s.metrics.Outcomes["failed"]++
	s.settleRooms(rec, false)
}

// settleRooms releases every room held on behalf of a request. On success the
// rooms are freed at once. On failure or abandonment the holder's compensating
// action must run first, so the room stays unavailable for exactly as long as
// that cleanup takes — and that is the time we book as wasted.
// noteCommit records a commit that happened after the client gave up. This is
// the "still charged the customer's card for an order they cancelled" case.
func (s *Sim) noteCommit(h *hop) {
	rec := h.rec
	if rec.outcome == "timeout" || (rec.done && rec.endedAt < s.now && rec.outcome != "success") {
		s.metrics.OrphanBookings++
		s.svc[h.node].OrphanCommits++
	}
}

func (s *Sim) settleRooms(rec *reqRecord, committed bool) {
	if rec.settled {
		return
	}
	rec.settled = true
	for room, hold := range s.rooms {
		if hold.hop.rec != rec {
			continue
		}
		if committed {
			delete(s.rooms, room)
			hold.hop.room = -1
			s.accountRoom(hold, s.now)
			continue
		}
		n := s.g.Nodes[hold.hop.node]
		if n.Cleanup {
			// The compensating action releases the room when it finishes.
			dur := s.cleanupDuration(hold.hop)
			s.push(&event{at: s.now + dur, kind: evCleanupDone, hop: hold.hop})
			hold.hop.cleanupRan = true
			hold.hop.cleanupNeeded = true
			hold.hop.cleanupAt = s.now
			hold.hop.cleanupDur = dur
			s.cleanupDue++
			s.svc[hold.hop.node].CleanupDue++
			s.metrics.CleanupRequired++
			if s.rec != nil {
				s.rec.writeCancel(cancelRec{
					req: hold.hop.rec.id, node: uint16(hold.hop.node), hop: uint32(hold.hop.id),
					cancelMs:  uint32((s.now - hold.hop.rec.arrival) / time.Millisecond),
					needClean: true, kind: "caller",
				})
			}
			continue
		}
		delete(s.rooms, room)
		hold.hop.room = -1
		s.accountRoom(hold, s.now)
	}
}

// ---------------------------------------------------------------------------
// Rooms
// ---------------------------------------------------------------------------

// keepsRoom reports whether a finished handler keeps its room after it
// returns. The lock holder does: the room stays reserved until payment.
func (h *hop) keepsRoom() bool { return h.keepsLock && h.room >= 0 }

func (s *Sim) acquireRoom(h *hop) {
	// Every lock takes a distinct room. We model hold time, not contention:
	// the cost being measured is "a room nobody will pay for is unavailable".
	room := s.nextRoom
	s.nextRoom++
	hold := &roomHold{room: room, hop: h, acquired: s.now, ttlAt: s.now + s.cfg.LockTTL}
	s.rooms[room] = hold
	h.room = room
	s.metrics.RoomsAcquired++
	if len(s.rooms) > s.metrics.RoomsMaxHeld {
		s.metrics.RoomsMaxHeld = len(s.rooms)
	}
	s.push(&event{at: hold.ttlAt, kind: evRoomTTL, hold: hold})
}

func (s *Sim) releaseRoom(h *hop, reason string) {
	if h.room < 0 {
		return
	}
	hold, ok := s.rooms[h.room]
	if !ok || hold.hop != h {
		h.room = -1
		return
	}
	delete(s.rooms, h.room)
	s.accountRoom(hold, s.now)
	_ = reason // kept for call-site documentation
	h.room = -1
}

func (s *Sim) accountRoom(hold *roomHold, rel time.Duration) {
	if hold.released {
		return
	}
	hold.released = true
	held := rel - hold.acquired
	if held < 0 {
		held = 0
	}
	s.metrics.RoomsHeldMs += float64(held) / float64(time.Millisecond)
	s.roomHeld.Add(held)
	if float64(held)/float64(time.Millisecond) > s.metrics.RoomMaxHoldMs {
		s.metrics.RoomMaxHoldMs = float64(held) / float64(time.Millisecond)
	}
	// A room is wasted from the instant the request stopped being able to
	// succeed: the client's give-up time, or the moment the flow failed.
	end := hold.hop.rec.giveUpAt
	if hold.hop.rec.done && hold.hop.rec.outcome != "success" && hold.hop.rec.endedAt < end {
		end = hold.hop.rec.endedAt
	}
	from := hold.acquired
	if end > from {
		from = end
	}
	if rel > from {
		w := rel - from
		s.metrics.RoomsWastedMs += float64(w) / float64(time.Millisecond)
		s.wasteR.Add(w)
		s.svc[hold.hop.node].RoomWastedMs += float64(w) / float64(time.Millisecond)
	}
}

// roomTTL is the last-resort lease expiry. A lease should be settled by the
// booking flow or by cleanup; if neither happened within the TTL the room was
// orphaned and is only reclaimed by the lease itself.
func (s *Sim) roomTTL(q *event) {
	hold := q.hold
	cur, ok := s.rooms[hold.room]
	if !ok || cur != hold {
		return
	}
	delete(s.rooms, hold.room)
	hold.hop.room = -1
	s.accountRoom(hold, s.now)
	s.metrics.RoomsTTL++
}

// ---------------------------------------------------------------------------
// Work accounting
// ---------------------------------------------------------------------------

// accountWork books a finished or interrupted processing segment. Processing
// that happens after the client stopped caring is wasted no matter how it
// ended.
func (s *Sim) accountWork(h *hop) {
	if h.procEnd <= h.procStart {
		return
	}
	total := h.procEnd - h.procStart
	s.procs[h.node].Add(h.procDur)
	s.metrics.ProcTotal += float64(total) / float64(time.Millisecond)
	s.svc[h.node].ProcMs += float64(total) / float64(time.Millisecond)
	s.svc[h.node].Segments++

	// Work is wasted when its result is thrown away: the segment ran after the
	// client stopped caring, or it was interrupted after starting because the
	// request had already died. A segment that completes for a live request is
	// useful even if the request later fails for a business reason.
	afterGiveUp := time.Duration(0)
	if h.procEnd > h.rec.giveUpAt {
		from := h.procStart
		if h.rec.giveUpAt > from {
			from = h.rec.giveUpAt
		}
		afterGiveUp = h.procEnd - from
	}
	// A segment whose result was discarded because the request died is wasted
	// even if it happened to finish before the client gave up: the work was
	// real, and it produced nothing.
	wasted := afterGiveUp
	if h.thrownAway && total > wasted {
		wasted = total
	}
	if wasted > 0 {
		s.metrics.WastedProc += float64(wasted) / float64(time.Millisecond)
		s.svc[h.node].WastedMs += float64(wasted) / float64(time.Millisecond)
		s.wasteP.Add(wasted)
	}
}

func (s *Sim) releaseCPU(h *hop) {
	if h.holdsCPU {
		s.busy[h.cpuNode]--
		h.holdsCPU = false
	}
}

// ---------------------------------------------------------------------------
// Terminal accounting, in exactly one place
// ---------------------------------------------------------------------------

// transition moves a hop to a new state and performs all terminal accounting
// exactly once per hop. Callers must never assign hop.state directly once a hop
// is live.
func (s *Sim) transition(h *hop, st spanState) {
	if h.state == st {
		return
	}
	h.state = st
	switch st {
	case stDone:
		s.metrics.SpanDone++
		s.svc[h.node].Done++
	case stFailed:
		s.metrics.SpanFailed++
		s.svc[h.node].Failed++
	case stCancelled:
		s.metrics.SpanCancelled++
		s.svc[h.node].Cancelled++
		if h.origin.Valid() {
			s.cancelBy[h.origin]++
		}
		if s.rec != nil && !s.g.Nodes[h.node].Cleanup {
			s.rec.writeCancel(cancelRec{
				req: h.rec.id, node: uint16(h.node), hop: uint32(h.id),
				cancelMs: uint32((s.now - h.rec.arrival) / time.Millisecond),
				kind:     string(h.origin.Kind),
			})
		}
	case stDropped:
		s.metrics.SpanDropped++
	case stGated:
		s.metrics.SpanGated++
		s.svc[h.node].Gated++
	}
}

// ---------------------------------------------------------------------------
// Finish
// ---------------------------------------------------------------------------

func (s *Sim) finish() {
	// Sweep anything still queued so accounting is complete.
	for node := range s.queues {
		for _, it := range s.queues[node].Ordered() {
			h := it.Meta
			if h.state.terminal() {
				continue
			}
			if h.hasDL && s.now >= h.deadline {
				s.onQueueExpired(h)
			} else {
				// Unfinished due to the simulation horizon; count it as dropped
				// from the pipeline without inventing work.
				h.endAt = s.now
				h.reason = "仿真结束时仍在排队"
				s.transition(h, stDropped)
			}
		}
	}

	s.metrics.SimulatedSeconds = float64(s.now) / float64(time.Second)
	s.metrics.Spans = s.nextHop
	s.metrics.Latency = s.latAll.Summary()
	s.metrics.LatencyOK = s.latOK.Summary()
	s.metrics.WastedProcP99Ms = ms(s.wasteP.Percentile(0.99))
	s.metrics.RoomWastedP99Ms = ms(s.wasteR.Percentile(0.99))
	s.metrics.RoomHeldP50Ms = ms(s.roomHeld.Percentile(0.50))
	s.metrics.RoomHeldP99Ms = ms(s.roomHeld.Percentile(0.99))
	s.metrics.ProcUseful = s.metrics.ProcTotal - s.metrics.WastedProc
	s.metrics.StartedAfterDeadline = s.startedAfterDeadline
	s.metrics.StartedAfterGiveUp = s.startedAfterGiveUp
	s.metrics.SentAfterDeadline = s.sentAfterDeadline
	s.metrics.CleanupMissed = s.cleanupDue - s.cleanupRan

	for i, sm := range s.svc {
		sm.QueueWait = s.waits[i].Summary()
		sm.Service = s.procs[i].Summary()
		s.metrics.PerService = append(s.metrics.PerService, *sm)
	}
	for o, n := range s.cancelBy {
		s.metrics.CancelBy = append(s.metrics.CancelBy, CancelOrigin{
			Kind: string(o.Kind), Host: o.Host, Note: o.Note, Count: n,
		})
	}
	sortCancel(s.metrics.CancelBy)
	s.buildSamples()
}
