// Package dlctx propagates absolute deadlines and cancellation across
// synchronous RPC hops and asynchronous message-queue hops.
//
// # Why the wire does not carry a wall-clock deadline
//
// Hosts are routinely skewed by seconds. A naive implementation ships
// "deadline_at=10:00:03.500" and lets every receiver subtract its own wall
// clock; with a 7s skew the receiver grants 7s too much or too little.
//
// This package ships two integers sampled from ONE monotonic source (a
// cluster-wide monotonic duration clock — a TAI tick or a synchronized
// monotonic epoch; the simulator injects a shared virtual tick):
//
//	remaining = deadline_mono - now_mono_at_send
//	sent_mono = now_mono_at_send
//
// A receiver computes
//
//	remaining -= recv_mono - sent_mono
//
// Only durations are subtracted. No host compares its wall clock with a value
// produced by another host, so wall-clock skew cannot corrupt a deadline. A
// wall timestamp travels alongside for humans and per-host display only.
//
// Every hop may tighten the budget but never widen it (see Tighten). When a
// hop's local budget is tighter than the incoming one the tighter value wins,
// and the tightening is recorded in the lineage so the UI can explain it.
package dlctx

import (
	"container/heap"
	"sort"
	"sync"
	"time"
)

// Kind explains why a deadline carries a cancellation.
type Kind string

const (
	KindNone      Kind = ""
	KindDeadline  Kind = "deadline"  // the time budget ran out
	KindCaller    Kind = "caller"    // an ancestor was cancelled
	KindSibling   Kind = "sibling"   // a sibling call failed (fail fast)
	KindShutdown  Kind = "shutdown"  // the owning process stopped
	KindDiscarded Kind = "discarded" // discovered expired while queued
	KindTightened Kind = "tightened" // a local budget was stricter than incoming
)

// Origin is a cancellation or tightening decision point.
type Origin struct {
	Kind Kind   `json:"kind"`
	Host string `json:"host"`
	Note string `json:"note,omitempty"`
}

func (o Origin) Valid() bool { return o.Kind != "" }

// Clock exposes the two independent time sources. Mono must be skewless and
// monotonically non-decreasing across every host in the call graph. Wall may
// be arbitrarily skewed between hosts and is for display and local ordering
// only.
type Clock interface {
	Mono() time.Duration
	Wall() time.Time
}

// Deadline is one hop's immutable view of the budget. A nil pointer and the
// zero value both mean "no deadline"; use Has to distinguish that from an
// exhausted budget.
type Deadline struct {
	has       bool
	deadline  time.Duration // absolute, on the shared monotonic source
	localWall time.Time     // display only, on the local wall clock
	cancelled bool
	origin    Origin   // the cancellation decision, if any
	lineage   []Origin // tightening/cancellation decisions inherited from ancestors
	frame     string   // causal path, e.g. "gw>order>lock"
	trace     string
	span      string
	parent    string
	sender    string
}

// Absent returns an explicit "no deadline".
func Absent() *Deadline { return &Deadline{} }

func (d *Deadline) get() *Deadline {
	if d == nil {
		return &Deadline{}
	}
	return d
}

func (d *Deadline) Has() bool       { return d.get().has }
func (d *Deadline) Cancelled() bool { return d.get().cancelled }
func (d *Deadline) Origin() Origin  { return d.get().origin }

// DeadlineMono is the absolute deadline on the shared monotonic source.
func (d *Deadline) DeadlineMono() time.Duration { return d.get().deadline }

// queueKey is the ordering key: cancelled work sorts first so it is discarded
// before any live work is considered, and work without a deadline sorts last.
func (d *Deadline) queueKey() time.Duration { return d.get().sortKey() }

func (d *Deadline) sortKey() time.Duration {
	d = d.get()
	if d.cancelled {
		return -1 << 62 // cancelled entries are popped (and dropped) first
	}
	if !d.has {
		return 1 << 62 // entries without a deadline sort last
	}
	return d.deadline
}

// Remaining is the budget left at nowMono, clamped at zero.
func (d *Deadline) Remaining(nowMono time.Duration) time.Duration {
	d = d.get()
	if !d.has || d.cancelled {
		return 0
	}
	r := d.deadline - nowMono
	if r < 0 {
		return 0
	}
	return r
}

// Expired reports whether the budget is spent at nowMono. A cancelled
// deadline is expired by definition.
func (d *Deadline) Expired(nowMono time.Duration) bool {
	d = d.get()
	if d.cancelled {
		return true
	}
	return d.has && nowMono >= d.deadline
}

// Allows reports whether a downstream call that needs at least minProc of
// processing can still be useful, after accounting for the estimated one-way
// latency. When it returns false the caller must not send the request at all;
// it returns a deadline error locally instead. A missing deadline always
// allows the call.
func (d *Deadline) Allows(nowMono, minProc, estNet time.Duration) bool {
	d = d.get()
	if !d.has {
		return true
	}
	if d.cancelled {
		return false
	}
	return d.deadline-nowMono-estNet >= minProc
}

// Lineage returns the decisions in force, outermost first, ending with this
// hop's own cancellation origin.
func (d *Deadline) Lineage() []Origin {
	d = d.get()
	out := make([]Origin, 0, len(d.lineage)+1)
	out = append(out, d.lineage...)
	if d.cancelled && d.origin.Valid() {
		out = append(out, d.origin)
	}
	return out
}

// Root creates the entry deadline. timeout is the client's patience.
func Root(trace, span, frame, host string, timeout time.Duration, c Clock) *Deadline {
	if timeout < 0 {
		timeout = 0
	}
	now := c.Mono()
	return &Deadline{
		has:       true,
		deadline:  now + timeout,
		localWall: c.Wall().Add(timeout),
		frame:     frame,
		trace:     trace,
		span:      span,
		sender:    host,
	}
}

// Tighten applies a local budget measured from now. It never widens: if the
// local budget is looser than the incoming deadline the incoming value is
// returned unchanged, and a cancelled deadline is never resurrected.
func (d *Deadline) Tighten(host string, budget time.Duration, c Clock) *Deadline {
	d = d.get()
	if budget <= 0 || d.cancelled {
		return d
	}
	now := c.Mono()
	local := now + budget
	if d.has && d.deadline <= local {
		return d
	}
	n := *d
	n.has = true
	n.deadline = local
	n.localWall = c.Wall().Add(budget)
	n.lineage = append(append([]Origin{}, d.lineage...), Origin{
		Kind: KindTightened, Host: host, Note: "local budget is stricter than incoming",
	})
	return &n
}

// Cancel freezes the budget and records the decision point. The origin is what
// every downstream span reports as "cancelled by".
func (d *Deadline) Cancel(o Origin) *Deadline {
	d = d.get()
	if d.cancelled {
		return d // the first decision is the one that propagates
	}
	n := *d
	n.has = true
	n.cancelled = true
	n.origin = o
	n.localWall = time.Time{}
	if d.cancelled && d.origin.Valid() {
		n.lineage = append(append([]Origin{}, d.lineage...), d.origin)
	}
	return &n
}

// ---------------------------------------------------------------------------
// Wire format
// ---------------------------------------------------------------------------

// Wire is the on-the-wire representation. Remaining and SentMono are sampled
// from the shared monotonic source at the same instant. WallAbs is
// display-only and may be skewed; receivers must never use it for arithmetic.
type Wire struct {
	Has        bool          `json:"has"`
	Cancelled  bool          `json:"cancelled,omitempty"`
	Remaining  time.Duration `json:"remaining_ns"`
	SentMono   time.Duration `json:"sent_mono_ns"`
	WallAbs    int64         `json:"wall_abs_unix_nano,omitempty"`
	Origin     Origin        `json:"origin,omitempty"`
	Lineage    []Origin      `json:"lineage,omitempty"`
	Frame      string        `json:"frame,omitempty"`
	Trace      string        `json:"trace,omitempty"`
	Span       string        `json:"span,omitempty"`
	ParentSpan string        `json:"parent_span,omitempty"`
}

// ToWire samples remaining and the monotonic send stamp at the same instant.
func ToWire(d *Deadline, sentMono time.Duration) Wire {
	d = d.get()
	w := Wire{
		Has: d.has, Cancelled: d.cancelled, Origin: d.origin, Lineage: d.lineage,
		Frame: d.frame, Trace: d.trace, Span: d.span, ParentSpan: d.parent,
	}
	if d.has {
		w.Remaining = d.Remaining(sentMono)
		w.SentMono = sentMono
		if !d.cancelled {
			w.WallAbs = d.localWall.UnixNano()
		}
	}
	return w
}

// FromWire reconstructs the budget on the receiver. It subtracts only a
// duration (transit measured on the shared monotonic source) from a duration.
// Neither a wall clock nor a foreign timestamp participates.
func FromWire(w Wire, recvMono time.Duration, senderHost string, wallNow time.Time) *Deadline {
	d := &Deadline{
		has: w.Has, cancelled: w.Cancelled,
		origin: w.Origin, lineage: w.Lineage,
		frame: w.Frame, trace: w.Trace, span: w.Span, parent: w.ParentSpan,
		sender: senderHost,
	}
	if w.Has {
		transit := recvMono - w.SentMono
		if transit < 0 {
			// Impossible with a shared monotonic source. Clamp defensively so
			// a broken clock can never grant extra time.
			transit = 0
		}
		budget := w.Remaining - transit
		if budget < 0 {
			budget = 0
		}
		d.deadline = recvMono + budget
		if !d.cancelled {
			d.localWall = wallNow.Add(budget)
		}
	}
	return d
}

// NaiveRemaining is what a receiver would compute if it trusted wall clocks:
// its own wall time minus the sender's wall-clock deadline. It exists so tests
// and the simulator can quantify the error that skew introduces. Production
// code must never use it for scheduling.
func NaiveRemaining(w Wire, recvWall time.Time) time.Duration {
	if !w.Has || w.WallAbs == 0 {
		return 0
	}
	return time.Unix(0, w.WallAbs).Sub(recvWall)
}

// ---------------------------------------------------------------------------
// Asynchronous message envelope
// ---------------------------------------------------------------------------

// Envelope is what a producer hands to a broker. The budget is stamped at
// produce time and re-derived by the consumer, so an asynchronous boundary
// does not lose the deadline. SentAt is a wall clock, display-only.
type Envelope struct {
	Wire   Wire      `json:"deadline"`
	SentAt time.Time `json:"sent_at"`
}

// Publish stamps a message. If the budget is already gone the broker is not
// contacted at all.
func Publish(d *Deadline, monoNow time.Duration, wallNow time.Time) (Envelope, bool) {
	d = d.get()
	if d.Expired(monoNow) {
		return Envelope{}, false
	}
	return Envelope{Wire: ToWire(d, monoNow), SentAt: wallNow}, true
}

// Consume re-derives the deadline at the consumer. A message that expired
// while queued is returned already cancelled with KindDiscarded; the consumer
// must drop it without running the handler.
func Consume(env Envelope, recvMono time.Duration, consumerHost string, wallNow time.Time) *Deadline {
	d := FromWire(env.Wire, recvMono, "mq", wallNow)
	if d.Expired(recvMono) {
		return d.Cancel(Origin{Kind: KindDiscarded, Host: consumerHost, Note: "message expired while queued"})
	}
	return d
}

// ---------------------------------------------------------------------------
// Deadline-ordered server queue
// ---------------------------------------------------------------------------

// DropReason explains why an item left the queue without being processed.
type DropReason int

const (
	DropNone      DropReason = iota
	DropExpired              // the deadline passed while waiting
	DropCancelled            // an upstream abandoned the request
)

// Item is an element of an ordered queue.
type Item[T any] struct {
	D    *Deadline
	ID   int64
	At   time.Duration // enqueue time on the shared monotonic source
	seq  int64         // insertion order, for a stable FIFO tie-break
	Meta T
}

// Queue orders work by absolute deadline, FIFO within equal deadlines. In one
// queue every deadline derives from the same monotonic source, so ordering is
// exact. Expired or cancelled work is discarded at dequeue and never starts.
// Cancelled items are dropped lazily: they are marked by the predicate and
// skipped by the next pop, so they never occupy a worker.
//
// It is a binary heap so a deep backlog costs O(log n) per insert. Pop skips
// expired and cancelled entries by continuing to pop until it finds live work.
type Queue[T any] struct {
	mu      sync.Mutex
	items   itemHeap[T]
	dropped int
	seq     int64 // monotonic insertion counter, the FIFO tie-break

	// Discard, when set, is called for every item that leaves the queue
	// without being processed. It lets the owner account for the work that
	// was avoided.
	Discard func(it Item[T], reason DropReason)
}

func NewQueue[T any]() *Queue[T] { return &Queue[T]{} }

func (q *Queue[T]) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

func (q *Queue[T]) Dropped() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.dropped
}

type itemHeap[T any] []Item[T]

func (h itemHeap[T]) Len() int           { return len(h) }
func (h itemHeap[T]) Less(i, j int) bool { return less(h[i], h[j]) }
func (h itemHeap[T]) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *itemHeap[T]) Push(x any)        { *h = append(*h, x.(Item[T])) }
func (h *itemHeap[T]) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}

// Push inserts in deadline order.
func (q *Queue[T]) Push(d *Deadline, id int64, at time.Duration, meta T) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.seq++
	heap.Push(&q.items, Item[T]{D: d, ID: id, At: at, seq: q.seq, Meta: meta})
}

func less[T any](a, b Item[T]) bool {
	if ak, bk := a.D.queueKey(), b.D.queueKey(); ak != bk {
		return ak < bk
	}
	return a.seq < b.seq
}

// PopOne removes the most urgent item. A discarded item is returned together
// with its DropReason and ok=false. The caller keeps popping until ok is true
// or the queue is empty.
func (q *Queue[T]) PopOne(nowMono time.Duration, cancelled func(Item[T]) bool) (Item[T], DropReason, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return Item[T]{}, DropNone, false
	}
	it := heap.Pop(&q.items).(Item[T])
	if it.D.Expired(nowMono) {
		q.dropped++
		if q.Discard != nil {
			q.Discard(it, DropExpired)
		}
		return it, DropExpired, false
	}
	if cancelled != nil && cancelled(it) {
		q.dropped++
		if q.Discard != nil {
			q.Discard(it, DropCancelled)
		}
		return it, DropCancelled, false
	}
	return it, DropNone, true
}

// Drop removes a specific item by id, wherever it sits in the heap. It is used
// to reclaim a queued entry the moment it expires, instead of waiting for the
// head of the queue to be popped.
func (q *Queue[T]) Drop(id int64) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i := range q.items {
		if q.items[i].ID == id {
			heap.Remove(&q.items, i)
			q.dropped++
			return true
		}
	}
	return false
}

// Ordered returns the queue contents in service order. Used by tests.
func (q *Queue[T]) Ordered() []Item[T] {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]Item[T], len(q.items))
	copy(out, q.items)
	sort.SliceStable(out, func(i, j int) bool { return less(out[i], out[j]) })
	return out
}
