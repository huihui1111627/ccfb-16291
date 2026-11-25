package sim

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
)

// The offline checker replays a compact event log written by the simulator and
// proves, independently of the in-process counters, that:
//
//  1. no service started processing a request at or after that request's
//     deadline (and, in the propagating run, after the client had given up);
//  2. every cancellation that required a compensating action got one, it
//     finished, and it stayed inside its cap;
//  3. no message consumer began work on a message whose request was already
//     abandoned or whose deadline had passed.
//
// All *_ms values are integer milliseconds since the request's arrival.
//
// Log format (CSV; the first field is the record type):
//
//	S, req, node, hop, start_ms, deadline_ms, has_dl
//	C, req, node, hop, cancel_ms, need_cleanup, cancel_kind
//	K, req, node, hop, start_ms, end_ms, cap_ms     (cleanup completed)
//	A, req, node, hop, consume_ms, deadline_ms, has_dl, dropped, reason
type CheckWriter struct {
	startW, cancelW, doneW, asyncW *csv.Writer
	closers                        []io.Closer
	nStart, nCancel, nDone, nAsync int
}

func NewCheckWriter(prefix string) (*CheckWriter, error) {
	w := &CheckWriter{}
	open := func(suffix string) (*csv.Writer, error) {
		f, err := os.Create(prefix + suffix)
		if err != nil {
			return nil, err
		}
		w.closers = append(w.closers, f)
		return csv.NewWriter(bufio.NewWriterSize(f, 1<<20)), nil
	}
	var err error
	if w.startW, err = open(".starts.csv"); err != nil {
		return nil, err
	}
	if w.cancelW, err = open(".cancels.csv"); err != nil {
		return nil, err
	}
	if w.doneW, err = open(".cleanups.csv"); err != nil {
		return nil, err
	}
	if w.asyncW, err = open(".async.csv"); err != nil {
		return nil, err
	}
	return w, nil
}

func u(i int) string       { return strconv.Itoa(i) }
func u16(v uint16) string  { return strconv.FormatUint(uint64(v), 10) }
func u32i(v uint32) string { return strconv.FormatUint(uint64(v), 10) }

func boolStr(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

func u32(v uint32) string { return strconv.FormatUint(uint64(v), 10) }

func (w *CheckWriter) writeStart(r startRec) {
	if w == nil {
		return
	}
	w.nStart++
	_ = w.startW.Write([]string{"S", u32(r.req), u16(r.node), u32i(r.hop),
		u32(r.startMs), u32(r.dlMs), boolStr(r.hasDL)})
	if w.nStart%8192 == 0 {
		w.startW.Flush()
	}
}

func (w *CheckWriter) writeCancel(r cancelRec) {
	if w == nil {
		return
	}
	w.nCancel++
	_ = w.cancelW.Write([]string{"C", u32(r.req), u16(r.node), u32i(r.hop),
		u32(r.cancelMs), boolStr(r.needClean), string(r.kind)})
	if w.nCancel%8192 == 0 {
		w.cancelW.Flush()
	}
}

func (w *CheckWriter) writeCleanup(r cleanupRec) {
	if w == nil {
		return
	}
	w.nDone++
	_ = w.doneW.Write([]string{"K", u32(r.req), u16(r.node), u32i(r.hop),
		u32(r.startMs), u32(r.endMs), u32(r.capMs)})
	if w.nDone%8192 == 0 {
		w.doneW.Flush()
	}
}

func (w *CheckWriter) writeAsync(r asyncRec) {
	if w == nil {
		return
	}
	w.nAsync++
	_ = w.asyncW.Write([]string{"A", u32(r.req), u16(r.node), u32i(r.hop),
		u32(r.consumeMs), u32(r.dlMs), boolStr(r.hasDL), boolStr(r.dropped), r.reason})
	if w.nAsync%8192 == 0 {
		w.asyncW.Flush()
	}
}

func (w *CheckWriter) Close() error {
	if w == nil {
		return nil
	}
	for _, cw := range []*csv.Writer{w.startW, w.cancelW, w.doneW, w.asyncW} {
		cw.Flush()
		if err := cw.Error(); err != nil {
			return err
		}
	}
	for _, c := range w.closers {
		if err := c.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Record types written by the engine.
type startRec struct {
	req     uint32
	node    uint16
	hop     uint32
	startMs uint32
	dlMs    uint32
	hasDL   bool
}

type cancelRec struct {
	req       uint32
	node      uint16
	hop       uint32
	cancelMs  uint32
	needClean bool
	kind      string
}

type cleanupRec struct {
	req     uint32
	node    uint16
	hop     uint32
	startMs uint32
	endMs   uint32
	capMs   uint32
}

type asyncRec struct {
	req       uint32
	node      uint16
	hop       uint32
	consumeMs uint32
	dlMs      uint32
	hasDL     bool
	dropped   bool
	reason    string
}

// CheckResult is what a replay proves.
type CheckResult struct {
	Starts                 int `json:"starts"`
	StartedAfterDeadline   int `json:"started_after_deadline"`
	StartedAfterClientLeft int `json:"started_after_client_left"`
	DeadlineWidened        int `json:"deadline_widened"`

	Cancels        int    `json:"cancels"`
	CleanupNeeded  int    `json:"cleanup_needed"`
	CleanupRan     int    `json:"cleanup_ran"`
	CleanupMissing int    `json:"cleanup_missing"`
	CleanupOverCap int    `json:"cleanup_over_cap"`
	MaxCleanupMs   uint32 `json:"max_cleanup_ms"`

	AsyncConsumed       int `json:"async_consumed"`
	AsyncDropped        int `json:"async_dropped"`
	AsyncDroppedStale   int `json:"async_dropped_stale"`
	AsyncExpiredStarted int `json:"async_expired_started"`

	Problems []string `json:"problems,omitempty"`
}

func addProblem(dst *[]string, s string) {
	if len(*dst) < 20 {
		*dst = append(*dst, s)
	}
}

type hopKey struct {
	req  uint32
	node uint16
	hop  uint32
}

// Check replays the four logs written under prefix.
func Check(prefix string, entryTimeoutMs uint32) (*CheckResult, error) {
	res := &CheckResult{}
	need := map[hopKey]bool{}
	done := map[hopKey]bool{}

	read := func(suffix string, fn func([]string)) error {
		f, err := os.Open(prefix + suffix)
		if err != nil {
			return err
		}
		defer f.Close()
		r := csv.NewReader(bufio.NewReaderSize(f, 1<<20))
		r.FieldsPerRecord = -1
		r.ReuseRecord = true
		for {
			rec, err := r.Read()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			fn(rec)
		}
	}
	num := func(s string) uint32 { v, _ := strconv.ParseUint(s, 10, 32); return uint32(v) }

	if err := read(".starts.csv", func(rec []string) {
		if len(rec) < 7 || rec[0] != "S" {
			return
		}
		res.Starts++
		start, dl, has := num(rec[4]), num(rec[5]), rec[6] == "1"
		if has && start > dl {
			res.StartedAfterDeadline++
			addProblem(&res.Problems, fmt.Sprintf("请求 %s hop %s 越过截止后才开始：start=%dms dl=%dms", rec[1], rec[3], start, dl))
		}
		if has && dl > entryTimeoutMs {
			res.DeadlineWidened++
			addProblem(&res.Problems, fmt.Sprintf("请求 %s hop %s 截止 %dms 超出入口超时 %dms", rec[1], rec[3], dl, entryTimeoutMs))
		}
		if start > entryTimeoutMs {
			res.StartedAfterClientLeft++
			addProblem(&res.Problems, fmt.Sprintf("请求 %s hop %s 在客户端离开 %dms 后才开始", rec[1], rec[3], start-entryTimeoutMs))
		}
	}); err != nil {
		return nil, err
	}

	if err := read(".cancels.csv", func(rec []string) {
		if len(rec) < 7 || rec[0] != "C" {
			return
		}
		res.Cancels++
		if rec[5] == "1" {
			res.CleanupNeeded++
			need[hopKey{num(rec[1]), uint16(num(rec[2])), num(rec[3])}] = true
		}
	}); err != nil {
		return nil, err
	}

	if err := read(".cleanups.csv", func(rec []string) {
		if len(rec) < 7 || rec[0] != "K" {
			return
		}
		k := hopKey{num(rec[1]), uint16(num(rec[2])), num(rec[3])}
		done[k] = true
		start, end, capMs := num(rec[4]), num(rec[5]), num(rec[6])
		dur := end - start
		if dur > res.MaxCleanupMs {
			res.MaxCleanupMs = dur
		}
		if capMs > 0 && dur > capMs {
			res.CleanupOverCap++
			addProblem(&res.Problems, fmt.Sprintf("请求 %s 节点 %s 清理 %dms 超过上限 %dms", rec[1], rec[2], dur, capMs))
		}
	}); err != nil {
		return nil, err
	}

	if err := read(".async.csv", func(rec []string) {
		if len(rec) < 9 || rec[0] != "A" {
			return
		}
		res.AsyncConsumed++
		if rec[7] == "1" {
			res.AsyncDropped++
			if rec[8] == "stale_request" {
				res.AsyncDroppedStale++
			}
			return
		}
		if rec[6] == "1" && num(rec[4]) > num(rec[5]) {
			res.AsyncExpiredStarted++
			addProblem(&res.Problems, fmt.Sprintf("请求 %s 节点 %s 消费了过期消息仍开始处理", rec[1], rec[2]))
		}
	}); err != nil {
		return nil, err
	}

	for k := range need {
		if !done[k] {
			res.CleanupMissing++
			if res.CleanupMissing <= 5 {
				addProblem(&res.Problems, fmt.Sprintf("请求 %d hop %d 需要清理但没有清理记录", k.req, k.hop))
			}
		}
	}
	res.CleanupRan = len(done)
	sort.Strings(res.Problems)
	return res, nil
}
