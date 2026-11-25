package sim

import (
	"fmt"
	"time"
)

// buildSamples converts sampled request trees into the JSON shape the UI
// consumes. All times are relative to the request arrival.
func (s *Sim) buildSamples() {
	for _, rec := range s.samples {
		ts := &TraceSample{
			ID:             rec.id,
			Outcome:        rec.outcome,
			ArrivalMs:      0,
			GiveUpMs:       ms(rec.giveUpAt - rec.arrival),
			EntryTimeoutMs: s.metrics.EntryTimeoutMs,
		}
		if rec.endedAt > rec.arrival {
			ts.EndToEndMs = ms(rec.endedAt - rec.arrival)
		}
		first := true
		walk(rec.root, func(h *hop) {
			n := s.node(h)
			start, end := h.startAt, h.endAt
			switch h.state {
			case stQueued, stInflight:
				start, end = h.arriveAt, h.arriveAt
			case stGated:
				start, end = h.sendAt, h.sendAt
			case stDropped:
				start, end = h.arriveAt, h.arriveAt
			}
			if h.state == stInflight && h.arriveAt == 0 {
				start, end = h.sendAt, h.sendAt
			}
			if start < rec.arrival {
				start = rec.arrival
			}
			if end < start {
				end = start
			}
			tn := TraceNode{
				ID:          fmt.Sprintf("h%d", h.id),
				Service:     n.Name,
				Kind:        string(h.kind),
				State:       h.state.String(),
				Depth:       h.depth,
				Link:        h.link,
				StartMs:     ms(start - rec.arrival),
				EndMs:       ms(end - rec.arrival),
				QueueMs:     ms(h.startAt - h.arriveAt),
				ProcMs:      ms(h.procEnd - h.procStart),
				HasDeadline: h.hasDL,
				Gated:       h.state == stGated,
				Note:        h.reason,
			}
			if h.parent != nil {
				tn.Parent = fmt.Sprintf("h%d", h.parent.id)
			}
			if h.hasDL {
				tn.DeadlineMs = ms(h.deadline - rec.arrival)
				ref := h.startAt
				switch h.state {
				case stQueued:
					ref = h.arriveAt
				case stGated:
					ref = h.sendAt
				}
				if ref == 0 {
					ref = h.sendAt
				}
				rem := h.deadline - ref
				if rem < 0 {
					rem = 0
				}
				tn.RemainingStartMs = ms(rem)
				if h.state == stGated {
					// The call was never sent; report the budget the parent had
					// when it decided not to make it.
					tn.RemainingStartMs = ms(h.deadline - h.sendAt)
				}
				remEnd := h.deadline - end
				if remEnd < 0 {
					remEnd = 0
				}
				tn.RemainingEndMs = ms(remEnd)
				if first {
					ts.MaxRemainingMs = tn.RemainingStartMs
					first = false
				}
				if tn.RemainingStartMs > ts.MaxRemainingMs {
					ts.MaxRemainingMs = tn.RemainingStartMs
				}
				if ts.MinRemainingMs == 0 || tn.RemainingEndMs < ts.MinRemainingMs {
					ts.MinRemainingMs = tn.RemainingEndMs
				}
			}
			if h.origin.Valid() {
				tn.CancelKind = string(h.origin.Kind)
				tn.CancelHost = h.origin.Host
			}
			if h.d != nil {
				for _, o := range h.d.Lineage() {
					tn.Lineage = append(tn.Lineage, string(o.Kind)+"@"+o.Host)
				}
			}
			if h.cleanupRan {
				needed := h.state == stCancelled
				cj := &CleanupJSON{
					Needed:     needed,
					Ran:        true,
					StartMs:    ms(h.endAt - rec.arrival),
					DurationMs: ms(h.cleanupDur),
					Capped:     h.cleanupDur >= CleanupHardCap,
				}
				tn.Cleanup = cj
			}
			if h.room >= 0 {
				room := h.room
				tn.LockRoom = &room
			}
			ts.Spans = append(ts.Spans, tn)
		})
		ts.ActiveSpans = len(ts.Spans)
		s.metrics.Samples++
		s.metrics.Traces = append(s.metrics.Traces, *ts)
	}
}

func (s *Sim) node(h *hop) *Node { return s.g.Nodes[h.node] }

func walk(h *hop, fn func(*hop)) {
	if h == nil {
		return
	}
	fn(h)
	for _, c := range h.children {
		walk(c, fn)
	}
}

// line is a small helper used by the CLI summary.
func line(format string, args ...any) string { return fmt.Sprintf(format, args...) }

var _ = time.Millisecond
