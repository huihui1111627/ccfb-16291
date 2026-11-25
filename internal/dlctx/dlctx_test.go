package dlctx

import (
	"testing"
	"time"
)

// fakeClock is a hand-cranked clock for tests.
type fakeClock struct {
	mono time.Duration
	wall time.Time
}

func (c *fakeClock) Mono() time.Duration { return c.mono }
func (c *fakeClock) Wall() time.Time     { return c.wall }
func (c *fakeClock) advance(d time.Duration) {
	c.mono += d
	c.wall = c.wall.Add(d)
}

func newFake() *fakeClock {
	return &fakeClock{mono: 1000 * time.Second, wall: time.Unix(1_700_000_000, 0)}
}

func TestWireRoundTripDeductsTransit(t *testing.T) {
	sender := newFake()
	d := Root("t", "s", "gw", "gw", 3*time.Second, sender)
	sender.advance(500 * time.Millisecond)

	wire := ToWire(d, sender.Mono()) // remaining 2.5s at send

	// Receiver's wall clock is skewed 7s into the future; must not matter.
	recv := newFake()
	recv.wall = recv.wall.Add(7 * time.Second)
	recv.mono = sender.Mono()
	recv.advance(120 * time.Millisecond) // 120ms network transit

	got := FromWire(wire, recv.Mono(), "gw", recv.Wall())

	// 2.5s remaining - 120ms transit = 2.38s.
	want := 2380 * time.Millisecond
	if diff := got.Remaining(recv.Mono()) - want; diff != 0 {
		t.Fatalf("remaining = %v, want %v", got.Remaining(recv.Mono()), want)
	}
	// And the naive wall-clock math would be wrong by the full skew, in
	// whichever direction the skew happens to point.
	naive := NaiveRemaining(wire, recv.Wall())
	if err := naive - want; err > -6*time.Second && err < 6*time.Second {
		t.Fatalf("naive skew error = %v, want |error| > 6s", err)
	}
	if !got.Expired(recv.Mono() + want) {
		t.Fatal("deadline should be expired exactly at its budget end")
	}
	if got.Expired(recv.Mono() + want - time.Nanosecond) {
		t.Fatal("deadline must not be expired 1ns early")
	}
}

func TestWireSurvivesNegativeTransitDefensively(t *testing.T) {
	c := newFake()
	d := Root("t", "s", "a", "a", time.Second, c)
	w := ToWire(d, c.Mono())
	// Receiver claims to have received 2s before the send: clamp, don't grant time.
	early := c.Mono() - 2*time.Second
	got := FromWire(w, early, "a", time.Now())
	if got.Remaining(early) > time.Second {
		t.Fatalf("negative transit granted extra time: %v", got.Remaining(early))
	}
}

func TestTightenNeverWidens(t *testing.T) {
	c := newFake()
	d := Root("t", "s", "a", "a", time.Second, c)

	wider := d.Tighten("b", 5*time.Second, c)
	if wider.Remaining(c.Mono()) != time.Second {
		t.Fatalf("tighten widened the budget: %v", wider.Remaining(c.Mono()))
	}
	if len(wider.Lineage()) != 0 {
		t.Fatalf("a no-op tighten must not add lineage, got %v", wider.Lineage())
	}

	narrower := d.Tighten("b", 300*time.Millisecond, c)
	if narrower.Remaining(c.Mono()) != 300*time.Millisecond {
		t.Fatalf("tighten did not narrow: %v", narrower.Remaining(c.Mono()))
	}
	lin := narrower.Lineage()
	if len(lin) != 1 || lin[0].Kind != KindTightened || lin[0].Host != "b" {
		t.Fatalf("lineage = %v", lin)
	}

	// A cancelled deadline must never be resurrected by a local budget.
	cancelled := d.Cancel(Origin{Kind: KindCaller, Host: "a"})
	if !cancelled.Tighten("b", time.Hour, c).Cancelled() {
		t.Fatal("tighten resurrected a cancelled deadline")
	}
}

func TestCancelPropagatesOrigin(t *testing.T) {
	c := newFake()
	d := Root("t", "s", "root", "root", time.Second, c)
	o := Origin{Kind: KindDeadline, Host: "gateway", Note: "client gone"}
	cd := d.Cancel(o)

	if !cd.Cancelled() || cd.Remaining(0) != 0 {
		t.Fatal("cancel must zero the budget")
	}
	lin := cd.Lineage()
	if len(lin) == 0 || lin[len(lin)-1] != o {
		t.Fatalf("lineage %v must end with %v", lin, o)
	}

	// Encoding and decoding preserves cancellation and its origin.
	w := ToWire(cd, c.Mono())
	got := FromWire(w, c.Mono(), "x", c.Wall())
	if !got.Cancelled() || got.Origin() != o {
		t.Fatalf("wire lost cancellation: %+v", got.Origin())
	}
}

func TestAllowsGate(t *testing.T) {
	c := newFake()
	d := Root("t", "s", "a", "a", 100*time.Millisecond, c)

	if !d.Allows(c.Mono(), 50*time.Millisecond, 10*time.Millisecond) {
		t.Fatal("100ms budget must allow 10ms net + 50ms work")
	}
	if d.Allows(c.Mono(), 95*time.Millisecond, 10*time.Millisecond) {
		t.Fatal("100ms budget must not allow 10ms net + 95ms work")
	}
	if Absent().Allows(0, time.Hour, time.Hour) == false {
		t.Fatal("absent deadline must always allow")
	}
}

func TestQueueOrdersByDeadlineAndDropsExpired(t *testing.T) {
	c := newFake()
	q := NewQueue[int]()
	mk := func(host string, budget time.Duration) *Deadline {
		return Root("t", "s", host, host, budget, c)
	}
	q.Push(mk("c", 300*time.Millisecond), 3, c.Mono(), 3)
	q.Push(mk("a", 100*time.Millisecond), 1, c.Mono(), 1)
	q.Push(mk("b", 200*time.Millisecond), 2, c.Mono(), 2)

	order := q.Ordered()
	if order[0].Meta != 1 || order[1].Meta != 2 || order[2].Meta != 3 {
		t.Fatalf("queue order = %+v", order)
	}

	// Advance past the first deadline: it must be dropped, not returned.
	c.advance(150 * time.Millisecond)
	it, reason, ok := q.PopOne(c.Mono(), nil)
	if ok || reason != DropExpired || it.Meta != 1 {
		t.Fatalf("expected expired drop of item 1, got ok=%v reason=%v meta=%v", ok, reason, it.Meta)
	}
	if q.Dropped() != 1 {
		t.Fatalf("dropped = %d", q.Dropped())
	}
	it, _, ok = q.PopOne(c.Mono(), nil)
	if !ok || it.Meta != 2 {
		t.Fatalf("expected item 2 next, got %v ok=%v", it.Meta, ok)
	}
}

func TestQueueCancelledLazyDrop(t *testing.T) {
	c := newFake()
	q := NewQueue[int]()
	d := Root("t", "s", "a", "a", time.Second, c)
	q.Push(d, 1, c.Mono(), 1)
	q.Push(Root("t", "s", "b", "b", 2*time.Second, c), 2, c.Mono(), 2)

	cancelled := map[int64]bool{1: true}
	it, reason, ok := q.PopOne(c.Mono(), func(i Item[int]) bool { return cancelled[i.ID] })
	if ok || reason != DropCancelled || it.ID != 1 {
		t.Fatalf("expected cancelled drop, got ok=%v reason=%v id=%v", ok, reason, it.ID)
	}
	it, reason, ok = q.PopOne(c.Mono(), func(i Item[int]) bool { return cancelled[i.ID] })
	if !ok || reason != DropNone || it.ID != 2 {
		t.Fatalf("expected live item 2, got ok=%v reason=%v id=%v", ok, reason, it.ID)
	}
}

func TestAsyncEnvelopeKeepsDeadline(t *testing.T) {
	c := newFake()
	d := Root("t", "s", "producer", "producer", 2*time.Second, c)
	env, ok := Publish(d, c.Mono(), c.Wall())
	if !ok {
		t.Fatal("publish dropped a live message")
	}

	// Consumer clock is skewed by -9s: must not shorten or extend the budget.
	consumer := newFake()
	consumer.wall = consumer.wall.Add(-9 * time.Second)
	consumer.mono = c.Mono()
	consumer.advance(2500 * time.Millisecond) // sat in the broker past the deadline

	got := Consume(env, consumer.Mono(), "notifier", consumer.Wall())
	if !got.Cancelled() || got.Origin().Kind != KindDiscarded {
		t.Fatalf("expired message not flagged discarded: cancelled=%v origin=%+v", got.Cancelled(), got.Origin())
	}

	// A message still inside its budget survives the boundary.
	d2 := Root("t", "s", "producer", "producer", 5*time.Second, c)
	env2, _ := Publish(d2, c.Mono(), c.Wall())
	c2 := newFake()
	c2.mono = c.Mono()
	c2.wall = c.Wall().Add(-9 * time.Second) // consumer wall is skewed
	c2.advance(100 * time.Millisecond)
	got2 := Consume(env2, c2.Mono(), "notifier", c2.Wall())
	if got2.Cancelled() {
		t.Fatal("live message lost its budget at the async boundary")
	}
	want := 5*time.Second - 100*time.Millisecond
	if got2.Remaining(c2.Mono()) != want {
		t.Fatalf("remaining = %v, want %v", got2.Remaining(c2.Mono()), want)
	}
}

func TestPublishRefusesExpired(t *testing.T) {
	c := newFake()
	d := Root("t", "s", "a", "a", time.Second, c)
	c.advance(time.Second)
	if _, ok := Publish(d, c.Mono(), c.Wall()); ok {
		t.Fatal("publish must not contact the broker for an expired message")
	}
}
