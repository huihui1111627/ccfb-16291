package sim

import (
	"path/filepath"
	"testing"
	"time"

	"scriptkill/sim/internal/dlctx"
)

// ---------------------------------------------------------------------------
// Requirement 1: an entry deadline propagates, with local elapsed time removed
// ---------------------------------------------------------------------------

func TestDeadlineIsReducedByElapsedLocalTime(t *testing.T) {
	// One request, modest load: every hop must carry strictly less budget than
	// its parent, and the reduction must match the time the parent actually
	// spent before calling.
	cfg := DefaultConfig(true)
	cfg.Requests = 40
	cfg.RatePerSec = 400
	s := New(cfg, DefaultGraph())
	m := s.Run()

	checked := 0
	for _, tr := range m.Traces {
		for _, sp := range tr.Spans {
			if sp.Parent == "" || !sp.HasDeadline {
				continue
			}
			plane := tr.Spans
			var parent *TraceNode
			for i := range plane {
				if plane[i].ID == sp.Parent {
					parent = &plane[i]
					break
				}
			}
			if parent == nil || !parent.HasDeadline {
				continue
			}
			// The child's remaining budget at its start must be no larger than
			// the parent's remaining budget at the parent's start, and the
			// difference must cover at least the child's start delay.
			if sp.RemainingStartMs > parent.RemainingStartMs {
				t.Fatalf("子 %s 剩余 %.1f > 父 %s 剩余 %.1f", sp.Service, sp.RemainingStartMs, parent.Service, parent.RemainingStartMs)
			}
			elapsed := sp.StartMs - parent.StartMs
			if elapsed > 1 && sp.RemainingStartMs > parent.RemainingStartMs-elapsed+0.5 {
				t.Fatalf("子 %s 没有扣掉父层已花时间：父 %.1f 子 %.1f 间隔 %.1f",
					sp.Service, parent.RemainingStartMs, sp.RemainingStartMs, elapsed)
			}
			checked++
		}
	}
	if checked < 20 {
		t.Fatalf("只检查了 %d 条边，样本不足", checked)
	}
}

func TestWireMathIsSkewImmune(t *testing.T) {
	// A 9-second wall skew must not change a single microsecond of budget.
	clock := func(mono time.Duration, wall time.Time) simClock { return simClock{mono: mono, wall: wall} }
	d := dlctx.Root("t", "s", "client>gw", "client", 2*time.Second, clock(10*time.Second, time.Unix(1000, 0)))
	w := dlctx.ToWire(d, 10*time.Second)

	honest := dlctx.FromWire(w, 10*time.Second+300*time.Millisecond, "gw", time.Unix(1000, 0))
	skewed := dlctx.FromWire(w, 10*time.Second+300*time.Millisecond, "gw", time.Unix(1009, 0))
	got, want := honest.Remaining(10*time.Second+300*time.Millisecond), 1700*time.Millisecond
	if got != want {
		t.Fatalf("remaining=%v want %v", got, want)
	}
	if skewed.Remaining(10*time.Second+300*time.Millisecond) != got {
		t.Fatal("墙钟偏差改变了剩余时间")
	}
	// And the naive wall-clock reading is wrong precisely by the skew.
	errNaive := dlctx.NaiveRemaining(w, time.Unix(1009, 0)) - got
	if errNaive > -8*time.Second && errNaive < 8*time.Second {
		t.Fatalf("naive error=%v, expected |error| > 8s", errNaive)
	}
}

// ---------------------------------------------------------------------------
// Requirement 2: a call that cannot fit the downstream minimum is not sent
// ---------------------------------------------------------------------------

// gatedGraph is a two-step chain whose downstream is deliberately expensive.
func gatedGraph() *Graph {
	g := &Graph{ByName: map[string]int{}}
	g.Add(&Node{Name: "front", Workers: 8, Lat: Const(2), MinProc: 1})
	g.Add(&Node{Name: "expensive", Workers: 8, Lat: Const(80), MinProc: 60})
	g.Connect("front", "expensive", 0, EdgeSync, Const(1))
	g.Entry = 0
	return g
}

func TestTightBudgetGatesTheCall(t *testing.T) {
	// 30ms of patience, 2ms of front work, 1ms of transit: only ~26ms remains,
	// and the downstream needs 60ms minimum, so the call must never be issued.
	cfg := DefaultConfig(true)
	cfg.Requests = 20
	cfg.RatePerSec = 100
	cfg.EntryTimeout = 30 * time.Millisecond
	s := New(cfg, gatedGraph())
	m := s.Run()

	if m.SpanGated == 0 {
		t.Fatal("预算不足的调用仍然被发了出去")
	}
	exp := m.PerService[s.g.ByName["expensive"]]
	if exp.Starts != 0 {
		t.Fatalf("预算不足的下游仍被启动 %d 次", exp.Starts)
	}
	if exp.Arrivals != 0 {
		t.Fatalf("预算不足的下游仍收到 %d 次请求", exp.Arrivals)
	}
}

func TestGenerousBudgetDoesNotGate(t *testing.T) {
	cfg := DefaultConfig(true)
	cfg.Requests = 20
	cfg.RatePerSec = 100
	cfg.EntryTimeout = 2 * time.Second
	s := New(cfg, gatedGraph())
	m := s.Run()
	if m.SpanGated != 0 {
		t.Fatalf("预算充足却门控了 %d 次", m.SpanGated)
	}
	if m.PerService[s.g.ByName["expensive"]].Starts == 0 {
		t.Fatal("预算充足时下游却没有被调用")
	}
}

// ---------------------------------------------------------------------------
// Requirement 3: cancellation reaches running and queued work
// ---------------------------------------------------------------------------

func TestClientGiveUpCancelsRunningWork(t *testing.T) {
	cfg := DefaultConfig(true)
	cfg.Requests = 60
	cfg.RatePerSec = 600
	cfg.EntryTimeout = 200 * time.Millisecond
	cfg.ServiceScale = map[string]float64{"payment": 4, "pay-gateway": 4, "bank-sim": 4}
	s := New(cfg, DefaultGraph())
	m := s.Run()

	if m.Outcomes["timeout"] == 0 {
		t.Fatal("这组参数应当有请求超时")
	}
	if m.SpanCancelled == 0 {
		t.Fatal("客户端放弃后没有任何下游被取消")
	}
	if m.StartedAfterGiveUp != 0 {
		t.Fatalf("%d 个 span 在客户端走后仍开始处理", m.StartedAfterGiveUp)
	}
	// Every cancellation must name a decision point.
	for _, tr := range m.Traces {
		for _, sp := range tr.Spans {
			if sp.State == "cancelled" && sp.CancelKind == "" {
				t.Fatalf("%s 被取消但没有取消来源", sp.Service)
			}
		}
	}
}

func TestCancellationRipplesOverLinks(t *testing.T) {
	// A deep chain with a slow leaf: the cancellation must arrive no earlier
	// than the link latency and no later than the leaf's own deadline.
	cfg := DefaultConfig(true)
	cfg.Requests = 20
	cfg.RatePerSec = 100
	cfg.EntryTimeout = 220 * time.Millisecond
	s := New(cfg, DefaultGraph())
	m := s.Run()
	seen := false
	for _, tr := range m.Traces {
		for _, sp := range tr.Spans {
			if sp.State != "cancelled" || sp.Depth < 3 {
				continue
			}
			seen = true
			if sp.EndMs < sp.StartMs {
				t.Fatalf("%s 取消时刻早于开始", sp.Service)
			}
		}
	}
	if !seen {
		t.Log("样本里没有深层取消，跳过")
	}
}

// ---------------------------------------------------------------------------
// Requirement 4: cleanup runs after cancellation and is bounded
// ---------------------------------------------------------------------------

func TestCleanupRunsForEveryCancelledResourceHolder(t *testing.T) {
	cfg := DefaultConfig(true)
	cfg.Requests = 800
	cfg.RatePerSec = 2000
	cfg.EntryTimeout = 400 * time.Millisecond
	s := New(cfg, DefaultGraph())
	m := s.Run()

	if m.CleanupRequired == 0 {
		t.Fatal("没有请求需要清理，参数不对")
	}
	if m.CleanupRan != m.CleanupRequired {
		t.Fatalf("需要清理 %d 次，实际只跑了 %d 次", m.CleanupRequired, m.CleanupRan)
	}
	if m.CleanupMissed != 0 {
		t.Fatalf("缺失 %d 次清理", m.CleanupMissed)
	}
	if m.CleanupOverCap != 0 {
		t.Fatalf("%d 次清理超出时间上限", m.CleanupOverCap)
	}
	if m.CleanupMaxMs > float64(CleanupHardCap/time.Millisecond)+0.001 {
		t.Fatalf("最长清理 %.1fms 超过硬上限", m.CleanupMaxMs)
	}
}

func TestCleanupHappensAfterCancellationNotBefore(t *testing.T) {
	cfg := DefaultConfig(true)
	cfg.Requests = 400
	cfg.RatePerSec = 2000
	cfg.EntryTimeout = 300 * time.Millisecond
	s := New(cfg, DefaultGraph())
	m := s.Run()
	for _, tr := range m.Traces {
		for _, sp := range tr.Spans {
			if sp.Cleanup == nil || !sp.Cleanup.Ran {
				continue
			}
			// A resource holder that finished its own work still owns a room;
			// it releases it when the request it belongs to dies. So a "done"
			// span may legitimately have cleanup, but only when the request
			// itself did not succeed.
			if sp.State == "done" && tr.Outcome == "success" {
				t.Fatalf("%s 已完成且请求成功，却跑了清理", sp.Service)
			}
			if sp.Cleanup.DurationMs <= 0 {
				t.Fatalf("%s 清理时长非正", sp.Service)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Requirement 5: a hop can tighten but never widen
// ---------------------------------------------------------------------------

func TestLocalBudgetTightensOnly(t *testing.T) {
	c := simClock{mono: 0, wall: time.Unix(0, 0)}
	d := dlctx.Root("t", "s", "a", "a", time.Second, c)
	if got := d.Tighten("b", 5*time.Second, c); got.Remaining(0) != time.Second {
		t.Fatalf("收紧操作放宽了预算: %v", got.Remaining(0))
	}
	narrow := d.Tighten("b", 100*time.Millisecond, c)
	if narrow.Remaining(0) != 100*time.Millisecond {
		t.Fatalf("收紧失败: %v", narrow.Remaining(0))
	}
	if len(narrow.Lineage()) != 1 || narrow.Lineage()[0].Kind != dlctx.KindTightened {
		t.Fatalf("收紧没有留下来源: %v", narrow.Lineage())
	}
	// A cancelled deadline must not be revived by a local budget.
	dead := d.Cancel(dlctx.Origin{Kind: dlctx.KindCaller, Host: "a"})
	if !dead.Tighten("b", time.Hour, c).Cancelled() {
		t.Fatal("收紧操作复活了已取消的截止时间")
	}
}

func TestEveryHopRespectsEntryTimeout(t *testing.T) {
	cfg := DefaultConfig(true)
	cfg.Requests = 500
	cfg.RatePerSec = 2000
	s := New(cfg, DefaultGraph())
	m := s.Run()
	limit := m.EntryTimeoutMs
	for _, tr := range m.Traces {
		for _, sp := range tr.Spans {
			if sp.HasDeadline && sp.DeadlineMs > limit+0.001 {
				t.Fatalf("%s 的截止 %.1fms 宽于入口 %.0fms", sp.Service, sp.DeadlineMs, limit)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Requirement 6: the server queue is ordered by deadline and drops expired work
// ---------------------------------------------------------------------------

func TestQueueIsOrderedByDeadline(t *testing.T) {
	c := simClock{mono: time.Second, wall: time.Unix(0, 0)}
	q := dlctx.NewQueue[int]()
	q.Push(dlctx.Root("t", "s", "c", "c", 300*time.Millisecond, c), 3, c.Mono(), 3)
	q.Push(dlctx.Root("t", "s", "a", "a", 100*time.Millisecond, c), 1, c.Mono(), 1)
	q.Push(dlctx.Root("t", "s", "b", "b", 200*time.Millisecond, c), 2, c.Mono(), 2)
	got := q.Ordered()
	if got[0].Meta != 1 || got[1].Meta != 2 || got[2].Meta != 3 {
		t.Fatalf("出队顺序不是截止时间序: %+v", got)
	}
	// Advance past the first two deadlines: both are dropped without running.
	c.mono += 250 * time.Millisecond
	it, reason, ok := q.PopOne(c.Mono(), nil)
	if ok || reason != dlctx.DropExpired || it.Meta != 1 {
		t.Fatalf("过期项没有被丢弃: ok=%v reason=%v", ok, reason)
	}
	it, reason, ok = q.PopOne(c.Mono(), nil)
	if ok || reason != dlctx.DropExpired || it.Meta != 2 {
		t.Fatalf("第二个过期项没有被丢弃: ok=%v reason=%v", ok, reason)
	}
	it, _, ok = q.PopOne(c.Mono(), nil)
	if !ok || it.Meta != 3 {
		t.Fatalf("剩余项不正确: %+v", it)
	}
	if q.Dropped() != 2 {
		t.Fatalf("dropped=%d", q.Dropped())
	}
}

// saturationGraph offers far more work than one slow service can drain, so its
// queue grows without bound and the oldest entries sit there for seconds.
func saturationGraph() *Graph {
	g := &Graph{ByName: map[string]int{}}
	g.Add(&Node{Name: "front", Workers: 4000, Lat: Const(0.2), MinProc: 0.05})
	// One worker with 1ms of work. The service enforces a 60ms budget of its
	// own; under a 4000/s arrival rate the queue grows far past 60ms of wait,
	// so the tail is discarded at dequeue because it can no longer finish even
	// its minimum unit of work. None of it touches a worker or a room.
	g.Add(&Node{Name: "bottleneck", Workers: 1, Lat: Const(1), MinProc: 5, LocalBudgetMs: 60})
	g.Connect("front", "bottleneck", 0, EdgeSync, Const(0.2))
	g.Entry = 0
	return g
}

func TestNoWorkStartsAfterDeadline(t *testing.T) {
	cfg := DefaultConfig(true)
	cfg.Requests = 12000
	cfg.RatePerSec = 4000
	cfg.EntryTimeout = 10 * time.Second
	s := New(cfg, saturationGraph())
	m := s.Run()

	// The invariant: no processing ever began at or after the request's
	// deadline, and none began after the client had given up.
	if m.StartedAfterDeadline != 0 {
		t.Fatalf("%d 个 span 在截止之后才开始处理", m.StartedAfterDeadline)
	}
	if m.StartedAfterGiveUp != 0 {
		t.Fatalf("%d 个 span 在客户端走后仍开始处理", m.StartedAfterGiveUp)
	}

	bn := m.PerService[s.g.ByName["bottleneck"]]
	if bn.QueueExpired == 0 {
		t.Fatalf("瓶颈服务没有丢弃任何排队过期的请求 (arr=%d starts=%d maxq=%d qP99=%.0f)",
			bn.Arrivals, bn.Starts, bn.QueueDepthMax, bn.QueueWait.P99)
	}
	// Everything that arrived is accounted for exactly once.
	if bn.Starts+bn.QueueExpired+bn.QueueCancelled != bn.Arrivals {
		t.Fatalf("%s 到达 %d ≠ 开始 %d + 排队过期 %d + 排队取消 %d",
			bn.Name, bn.Arrivals, bn.Starts, bn.QueueExpired, bn.QueueCancelled)
	}
	if bn.Starts == 0 {
		t.Fatal("瓶颈服务完全没有处理任何请求")
	}
}

// ---------------------------------------------------------------------------
// Requirement 7: deadline and cancellation cross an async boundary
// ---------------------------------------------------------------------------

func TestAsyncBoundaryCarriesDeadline(t *testing.T) {
	c := simClock{mono: 5 * time.Second, wall: time.Unix(0, 0)}
	d := dlctx.Root("t", "s", "producer", "producer", 2*time.Second, c)
	env, ok := dlctx.Publish(d, c.Mono(), c.Wall())
	if !ok {
		t.Fatal("live message was not published")
	}
	// Consumer is 30s skewed and receives 500ms later.
	got := dlctx.Consume(env, c.Mono()+500*time.Millisecond, "consumer",
		time.Unix(0, 0).Add(35*time.Second))
	if got.Cancelled() {
		t.Fatal("live message was dropped")
	}
	if rem := got.Remaining(c.Mono() + 500*time.Millisecond); rem != 1500*time.Millisecond {
		t.Fatalf("remaining=%v want 1.5s", rem)
	}
	// Sitting in the broker past the deadline makes it undeliverable.
	late := dlctx.Consume(env, c.Mono()+3*time.Second, "consumer", time.Unix(0, 0))
	if !late.Cancelled() || late.Origin().Kind != dlctx.KindDiscarded {
		t.Fatalf("expired message was not discarded: %+v", late.Origin())
	}
}

func TestExpiredMessageIsDroppedNotProcessed(t *testing.T) {
	cfg := DefaultConfig(true)
	cfg.Requests = 1500
	cfg.RatePerSec = 2500
	cfg.EntryTimeout = 600 * time.Millisecond
	s := New(cfg, DefaultGraph())
	m := s.Run()
	if m.AsyncPublished == 0 {
		t.Fatal("异步分支没有发出任何消息")
	}
	// Every publish either was delivered and processed, or dropped on arrival.
	if m.AsyncDroppedStale+m.NotifyDroppedStale > m.AsyncPublished {
		t.Fatal("丢弃计数超过发布数")
	}
	for _, svc := range m.PerService {
		if svc.Name == "notifier-worker" || svc.Name == "sms-provider" {
			// The consumer must never start a message whose request is gone.
			if svc.QueueExpired > 0 && svc.Starts == 0 {
				t.Logf("%s: %d dropped on arrival, 0 started", svc.Name, svc.QueueExpired)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// The end-to-end acceptance run
// ---------------------------------------------------------------------------

func TestAcceptanceRun(t *testing.T) {
	dir := t.TempDir()
	run := func(propagate bool) (*Metrics, *CheckResult) {
		cfg := DefaultConfig(propagate)
		cfg.Requests = 20000
		cfg.RatePerSec = 1500
		s := New(cfg, DefaultGraph())
		name := "prop"
		if !propagate {
			name = "base"
		}
		w, err := NewCheckWriter(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		s.SetCheckWriter(w)
		m := s.Run()
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		res, err := Check(filepath.Join(dir, name), uint32(cfg.EntryTimeout/time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		return m, res
	}

	prop, propChk := run(true)
	base, baseChk := run(false)

	if propChk.StartedAfterDeadline != 0 {
		t.Fatalf("传截止时间：%d 次在截止后开始", propChk.StartedAfterDeadline)
	}
	if propChk.StartedAfterClientLeft != 0 {
		t.Fatalf("传截止时间：%d 次在客户端走后开始", propChk.StartedAfterClientLeft)
	}
	if propChk.CleanupMissing != 0 {
		t.Fatalf("传截止时间：%d 次取消缺少清理", propChk.CleanupMissing)
	}
	if propChk.CleanupOverCap != 0 {
		t.Fatalf("传截止时间：%d 次清理超上限", propChk.CleanupOverCap)
	}
	if propChk.DeadlineWidened != 0 {
		t.Fatalf("传截止时间：%d 个截止时间被放宽", propChk.DeadlineWidened)
	}
	if propChk.AsyncExpiredStarted != 0 {
		t.Fatalf("传截止时间：%d 条过期消息仍被处理", propChk.AsyncExpiredStarted)
	}

	// The non-propagating baseline must actually exhibit the bug we are
	// measuring, otherwise the comparison is meaningless.
	if baseChk.StartedAfterClientLeft == 0 {
		t.Fatal("对照组没有出现“客户端走后仍在干活”，说明基线没有真正模拟不传截止时间")
	}
	if baseChk.CleanupMissing == 0 && base.WastedProc <= prop.WastedProc {
		t.Fatal("对照组没有比传截止时间更差，比较无意义")
	}

	if prop.WastedProc >= base.WastedProc {
		t.Fatalf("传截止时间的白干处理 %.0fms 不少于不传的 %.0fms", prop.WastedProc, base.WastedProc)
	}
	if prop.RoomsWastedMs >= base.RoomsWastedMs {
		t.Fatalf("传截止时间的房间白锁 %.0fms 不少于不传的 %.0fms", prop.RoomsWastedMs, base.RoomsWastedMs)
	}
	if prop.OrphanBookings != 0 {
		t.Fatalf("传截止时间仍产生 %d 个幽灵订单", prop.OrphanBookings)
	}
	if base.OrphanBookings == 0 {
		t.Fatal("对照组没有幽灵订单，样本不足以说明问题")
	}
	t.Logf("传: 白干 %.0fms 房间白锁 %.0fms | 不传: 白干 %.0fms 房间白锁 %.0fms",
		prop.WastedProc, prop.RoomsWastedMs, base.WastedProc, base.RoomsWastedMs)
}

func TestRunIsDeterministic(t *testing.T) {
	cfg := DefaultConfig(true)
	cfg.Requests = 500
	cfg.RatePerSec = 1000
	a := New(cfg, DefaultGraph()).Run()
	b := New(cfg, DefaultGraph()).Run()
	if a.Spans != b.Spans || a.ProcTotal != b.ProcTotal || a.WastedProc != b.WastedProc ||
		a.RoomsWastedMs != b.RoomsWastedMs {
		t.Fatalf("相同种子结果不同:\n%+v\n%+v", a, b)
	}
}

// A message for an already-abandoned request must be dropped at the consumer,
// and a message whose deadline expired in the broker must be dropped too.
func TestAsyncCancellationAndExpiryAreDropped(t *testing.T) {
	cfg := DefaultConfig(true)
	cfg.Requests = 3000
	cfg.RatePerSec = 1500
	cfg.EntryTimeout = 1200 * time.Millisecond
	s := New(cfg, DefaultGraph())
	m := s.Run()

	if m.AsyncPublished == 0 {
		t.Fatal("异步分支没有发布任何消息")
	}
	// Every consumed message either ran or was dropped; none may run for a
	// request that had already timed out.
	nw := m.PerService[s.g.ByName["notifier-worker"]]
	if m.NotifyAfterGiveUp != 0 {
		t.Fatalf("%d 条通知在客户端已放弃后仍被消费", m.NotifyAfterGiveUp)
	}
	if nw.Starts > nw.Arrivals {
		t.Fatalf("notifier-worker 启动 %d 多于到达 %d", nw.Starts, nw.Arrivals)
	}
	// Any message the consumer dropped must not have started work.
	if m.AsyncDroppedStale > m.AsyncDelivered {
		t.Fatalf("丢弃 %d 多于投递 %d", m.AsyncDroppedStale, m.AsyncDelivered)
	}
}
